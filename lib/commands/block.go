package commands

import (
	"math"
	"time"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// Blocking list and sorted-set commands (design doc §7 P2, §4.2): BLPOP,
// BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH, BZPOPMIN, BZPOPMAX, BZMPOP.
//
// The wait never occupies a shard goroutine: after a fast-path scan finds
// every key empty, the command registers a shard.Waiter on each key (in
// the same shard task that found the key empty, so no wake can be missed)
// and parks the connection goroutine on the waiter's channel. Pushes into
// a watched key signal the first waiter (Shard.WakeWaiter, called from
// the mutation closures in list.go/zset.go); the woken command
// deregisters and retries the fast path. Redis semantics probed against
// 7.2.7 and encoded here:
//   - Keys are scanned in argument order; a missing key is skipped, a
//     wrong-type key errors immediately, and the first non-empty key is
//     popped without consulting later keys (even wrong-type ones).
//   - Timeout parse: NaN/unparseable → "timeout is not a float or out of
//     range", negative → "timeout is negative", > MaxInt64/1000 s →
//     "timeout is out of range"; 0 (and -0) blocks forever.
//   - BLMOVE blocks only when the source is empty: a wrong-type
//     destination is not checked while blocking (only at move time).
//   - Inside MULTI/EXEC a blocking command takes a single non-blocking
//     fast-path attempt and replies null when every key is empty.

var (
	errTimeoutNotFloat = resp.Err("ERR timeout is not a float or out of range")
	errTimeoutNegative = resp.Err("ERR timeout is negative")
	errTimeoutRange    = resp.Err("ERR timeout is out of range")
	errNumKeys         = resp.Err("ERR numkeys should be greater than 0")
	errCountPositive   = resp.Err("ERR count should be greater than 0")
)

// parseBlockTimeout mirrors Redis getTimeoutFromObjectOrReply in seconds
// (BLPOP-style): 0 blocks forever (returned as the zero Duration); a
// positive timeout that rounds below one nanosecond rounds UP to the
// shortest possible wait (Redis rounds up to 1 ms — never down to
// forever), and one that overflows a Duration clamps to the maximum
// (centuries — indistinguishable from forever).
func parseBlockTimeout(b []byte) (time.Duration, resp.Value, bool) {
	f, ok := parseFloat(b)
	if !ok {
		return 0, errTimeoutNotFloat, false
	}
	if f < 0 { // note: -0 is not < 0, and blocks forever like 0
		return 0, errTimeoutNegative, false
	}
	if f > float64(math.MaxInt64)/1000 { // f*1000 must fit a mstime_t
		return 0, errTimeoutRange, false
	}
	if f == 0 {
		return 0, resp.Value{}, true
	}
	d := f * float64(time.Second)
	switch {
	case d >= float64(math.MaxInt64):
		return time.Duration(math.MaxInt64), resp.Value{}, true
	case d < 1:
		return time.Duration(1), resp.Value{}, true
	}
	return time.Duration(d), resp.Value{}, true
}

// tryKey is the command-specific check-and-pop for one key, run inside
// the key's shard goroutine: it returns (reply, true) on a hit or a
// WRONGTYPE error, or the zero Value with false when the key is
// empty/missing (the caller then registers the waiter in the same task).
type tryKey func(s *shard.Shard, key string) (resp.Value, bool)

// blockTry walks keys in argument order, one shard task per key — key
// order is observable (BLPOP k1 k2 must pop k1 first even when k2 is also
// non-empty), so no doMulti fan-out. A key found empty registers w in the
// same task (check-and-register is atomic per shard). On a hit after
// earlier registrations, w is deregistered from those keys before
// returning. A nil w (EXEC fast path) skips registration.
func (e *Engine) blockTry(cs *ConnState, keys [][]byte, w *shard.Waiter, try tryKey) (resp.Value, bool) {
	var registered []string
	var reply resp.Value
	done := false
	for _, k := range keys {
		key := string(k)
		e.do(cs, k, func(s *shard.Shard) {
			if done {
				return
			}
			r, hit := try(s, key)
			if hit {
				reply, done = r, true
				return
			}
			if w != nil {
				s.AddWaiter(cs.DB, key, w)
				registered = append(registered, key)
			}
		})
		if done {
			break
		}
	}
	if done {
		for _, key := range registered {
			kb := []byte(key)
			e.do(cs, kb, func(s *shard.Shard) { s.RemoveWaiter(cs.DB, key, w) })
		}
	}
	return reply, done
}

// blockDeregister removes w from every watched key (RemoveWaiter drops
// all occurrences, so duplicate key arguments need no special casing).
func (e *Engine) blockDeregister(cs *ConnState, keys [][]byte, w *shard.Waiter) {
	for _, k := range keys {
		key := string(k)
		e.do(cs, k, func(s *shard.Shard) { s.RemoveWaiter(cs.DB, key, w) })
	}
}

// block runs the blocking loop shared by all eight commands: attempt is
// one fast-path pass (see blockTry), returning (reply, true) when the
// command resolved — data or error — or (zero, false) when every key is
// empty and the waiter is registered on all of them. Inside EXEC the
// attempt runs exactly once with no waiter (blocking commands in MULTI
// behave as non-blocking, replying null when empty).
func (e *Engine) block(cs *ConnState, keys [][]byte, timeout time.Duration,
	attempt func(w *shard.Waiter) (resp.Value, bool),
) resp.Value {
	if cs.inExec {
		reply, done := attempt(nil)
		if !done {
			return resp.Null()
		}
		return reply
	}
	w := &shard.Waiter{Ch: make(chan struct{}, 1), ID: cs.ID}
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout) // absolute: re-parks never extend it
		defer timer.Stop()
		timeoutCh = timer.C
	}
	blocked := false
	defer func() {
		if blocked {
			e.blockedClients.Add(-1)
		}
	}()
	for {
		reply, done := attempt(w)
		if done {
			return reply
		}
		if !blocked {
			e.blockedClients.Add(1)
			blocked = true
		}
		select {
		case <-w.Ch:
			// Another connection may have won the pop race: deregister,
			// retry the fast path, and re-register if still empty.
			// Re-registration goes to the BACK of each FIFO (Redis keeps
			// a blocked client's original position; this is looser).
			e.blockDeregister(cs, keys, w)
		case <-timeoutCh:
			e.blockDeregister(cs, keys, w)
			return resp.Null()
		case <-e.Shards.Closing():
			// Shutdown: reply null. The shards are being stopped, so no
			// deregistration — submitting shard tasks now could hang.
			return resp.Null()
		}
	}
}

// --- BLPOP / BRPOP -----------------------------------------------------------

func cmdBLPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return bpopCmd(e, cs, args, true)
}

func cmdBRPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return bpopCmd(e, cs, args, false)
}

func bpopCmd(e *Engine, cs *ConnState, args [][]byte, head bool) resp.Value {
	timeout, errV, ok := parseBlockTimeout(args[len(args)-1])
	if !ok {
		return errV
	}
	keys := args[1 : len(args)-1]
	return e.block(cs, keys, timeout, func(w *shard.Waiter) (resp.Value, bool) {
		return e.blockTry(cs, keys, w, func(s *shard.Shard, key string) (resp.Value, bool) {
			ent, wt := getColl(s, cs.DB, key, shard.TypeList)
			switch {
			case wt:
				return errWrongType, true
			case ent == nil:
				return resp.Value{}, false
			}
			l := ent.Obj.(*types.List)
			v, _ := popOne(l, head)
			s.Touch(ent)
			if l.Len() == 0 {
				s.Delete(cs.DB, key)
			} else {
				// Serve the next queued waiter while elements remain.
				s.WakeWaiter(cs.DB, key)
			}
			return resp.Arr(resp.BlobString([]byte(key)), resp.BlobString(v)), true
		})
	})
}

// --- BLMOVE / BRPOPLPUSH -------------------------------------------------------

func cmdBLMove(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	var srcHead, dstHead bool
	switch lowerASCII(args[3]) {
	case "left":
		srcHead = true
	case "right":
	default:
		return errSyntax
	}
	switch lowerASCII(args[4]) {
	case "left":
		dstHead = true
	case "right":
	default:
		return errSyntax
	}
	timeout, errV, ok := parseBlockTimeout(args[5])
	if !ok {
		return errV
	}
	return blmoveCmd(e, cs, args[1], args[2], srcHead, dstHead, timeout)
}

func cmdBRPopLPush(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	timeout, errV, ok := parseBlockTimeout(args[3])
	if !ok {
		return errV
	}
	return blmoveCmd(e, cs, args[1], args[2], false, true, timeout)
}

// blmoveCmd is the blocking form of lmoveCmd. Only the source gates
// blocking (Redis semantics: a missing source parks even when the
// destination holds a wrong type — the destination is checked only when a
// move is attempted).
func blmoveCmd(e *Engine, cs *ConnState, srcB, dstB []byte, srcHead, dstHead bool, timeout time.Duration) resp.Value {
	keys := [][]byte{srcB}
	return e.block(cs, keys, timeout, func(w *shard.Waiter) (resp.Value, bool) {
		candidate := false
		reply, done := e.blockTry(cs, keys, w, func(s *shard.Shard, key string) (resp.Value, bool) {
			ent, wt := getColl(s, cs.DB, key, shard.TypeList)
			switch {
			case wt:
				return errWrongType, true
			case ent == nil:
				return resp.Value{}, false
			}
			candidate = true
			return resp.Value{}, true
		})
		if !done {
			return resp.Value{}, false
		}
		if !candidate {
			return reply, true // WRONGTYPE source
		}
		// The source was non-empty a moment ago; run the move outside the
		// shard task (the destination may live on another shard — calling
		// doMulti from inside would deadlock). Losing the race to another
		// popper yields null from lmoveCmd: re-block instead of replying.
		out := lmoveCmd(e, cs, srcB, dstB, srcHead, dstHead)
		if out.Kind == resp.KindNull {
			return resp.Value{}, false
		}
		return out, true
	})
}

// --- BLMPOP ---------------------------------------------------------------------

// parseMpopTail parses the shared BLMPOP/BZMPOP argument tail after the
// timeout: numkeys, the key slice, the direction, and the optional COUNT.
// idx points at numkeys; dirWords are the accepted direction keywords.
func parseMpopTail(args [][]byte, idx int, dirs [2]string) (keys [][]byte, fromMin bool, count int64, errV resp.Value, failed bool) {
	nk, ok := parseIntStrict(args[idx])
	if !ok || nk <= 0 {
		return nil, false, 0, errNumKeys, true
	}
	idx++
	if len(args) < idx+int(nk)+1 {
		return nil, false, 0, errSyntax, true
	}
	keys = args[idx : idx+int(nk)]
	idx += int(nk)
	switch lowerASCII(args[idx]) {
	case dirs[0]:
		fromMin = true
	case dirs[1]:
	default:
		return nil, false, 0, errSyntax, true
	}
	idx++
	count = 1
	if idx < len(args) {
		if len(args) != idx+2 || lowerASCII(args[idx]) != "count" {
			return nil, false, 0, errSyntax, true
		}
		v, ok := parseIntStrict(args[idx+1])
		if !ok || v <= 0 {
			return nil, false, 0, errCountPositive, true
		}
		count = v
	}
	return keys, fromMin, count, resp.Value{}, false
}

func cmdBLMPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	timeout, errV, ok := parseBlockTimeout(args[1])
	if !ok {
		return errV
	}
	keys, head, count, errV2, failed := parseMpopTail(args, 2, [2]string{"left", "right"})
	if failed {
		return errV2
	}
	return e.block(cs, keys, timeout, func(w *shard.Waiter) (resp.Value, bool) {
		return e.blockTry(cs, keys, w, func(s *shard.Shard, key string) (resp.Value, bool) {
			ent, wt := getColl(s, cs.DB, key, shard.TypeList)
			switch {
			case wt:
				return errWrongType, true
			case ent == nil:
				return resp.Value{}, false
			}
			l := ent.Obj.(*types.List)
			n := min(int(count), l.Len())
			out := make([]resp.Value, 0, n)
			for range n {
				v, _ := popOne(l, head)
				out = append(out, resp.BlobString(v))
			}
			s.Touch(ent)
			if l.Len() == 0 {
				s.Delete(cs.DB, key)
			} else {
				s.WakeWaiter(cs.DB, key) // serve the next waiter while elements remain
			}
			return resp.Arr(resp.BlobString([]byte(key)), resp.Arr(out...)), true
		})
	})
}

// --- BZPOPMIN / BZPOPMAX ---------------------------------------------------------

// zScoreValue renders a popped score the way ZPOPMIN does (zset.go):
// a RESP3 double, a RESP2 bulk string via resp.FormatDouble.
func zScoreValue(cs *ConnState, score float64) resp.Value {
	if cs.Proto == 3 {
		return resp.Double(score)
	}
	return resp.BlobStr(resp.FormatDouble(score))
}

func cmdBZPopMin(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return bzpopCmd(e, cs, args, true)
}

func cmdBZPopMax(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return bzpopCmd(e, cs, args, false)
}

func bzpopCmd(e *Engine, cs *ConnState, args [][]byte, fromMin bool) resp.Value {
	timeout, errV, ok := parseBlockTimeout(args[len(args)-1])
	if !ok {
		return errV
	}
	keys := args[1 : len(args)-1]
	return e.block(cs, keys, timeout, func(w *shard.Waiter) (resp.Value, bool) {
		return e.blockTry(cs, keys, w, func(s *shard.Shard, key string) (resp.Value, bool) {
			ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
			switch {
			case wt:
				return errWrongType, true
			case ent == nil:
				return resp.Value{}, false
			}
			z := ent.Obj.(*types.ZSet)
			i := 0
			if !fromMin {
				i = z.Len() - 1
			}
			el, _ := z.At(i)
			z.Remove(el.Member)
			s.Touch(ent)
			if z.Len() == 0 {
				s.Delete(cs.DB, key)
			} else {
				s.WakeWaiter(cs.DB, key) // serve the next waiter while elements remain
			}
			return resp.Arr(resp.BlobString([]byte(key)), resp.BlobStr(el.Member),
				zScoreValue(cs, el.Score)), true
		})
	})
}

// --- BZMPOP ---------------------------------------------------------------------

func cmdBZMPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	timeout, errV, ok := parseBlockTimeout(args[1])
	if !ok {
		return errV
	}
	keys, fromMin, count, errV2, failed := parseMpopTail(args, 2, [2]string{"min", "max"})
	if failed {
		return errV2
	}
	return e.block(cs, keys, timeout, func(w *shard.Waiter) (resp.Value, bool) {
		return e.blockTry(cs, keys, w, func(s *shard.Shard, key string) (resp.Value, bool) {
			ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
			switch {
			case wt:
				return errWrongType, true
			case ent == nil:
				return resp.Value{}, false
			}
			z := ent.Obj.(*types.ZSet)
			n := min(int(count), z.Len())
			out := make([]resp.Value, 0, n)
			for range n {
				i := 0
				if !fromMin {
					i = z.Len() - 1
				}
				el, _ := z.At(i)
				out = append(out, resp.Arr(resp.BlobStr(el.Member), zScoreValue(cs, el.Score)))
				z.Remove(el.Member)
			}
			s.Touch(ent)
			if z.Len() == 0 {
				s.Delete(cs.DB, key)
			} else {
				s.WakeWaiter(cs.DB, key) // serve the next waiter while elements remain
			}
			return resp.Arr(resp.BlobString([]byte(key)), resp.Arr(out...)), true
		})
	})
}
