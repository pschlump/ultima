package commands

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// --- SET ------------------------------------------------------------------

type setOptions struct {
	nx, xx, get, keepttl bool
	expireAtMs           int64 // absolute expiry in ms; 0 = none
	hasExpire            bool
	expUnit              string // "ex", "px", "exat", "pxat"
}

// parseSetOptions parses SET's option tail exactly as Redis 7.2 does:
// NX/XX are mutually exclusive, KEEPTTL excludes the four expire options,
// an expire unit may repeat (last wins) but units never mix, and values
// must be positive int64s. The second return is the error reply, if any.
func parseSetOptions(args [][]byte) (setOptions, resp.Value, bool) {
	var o setOptions
	expUnit := ""
	fail := func(v resp.Value) (setOptions, resp.Value, bool) { return o, v, true }
	for i := 3; i < len(args); i++ {
		switch opt := lowerASCII(args[i]); opt {
		case "nx":
			if o.xx {
				return fail(errSyntax)
			}
			o.nx = true
		case "xx":
			if o.nx {
				return fail(errSyntax)
			}
			o.xx = true
		case "get":
			o.get = true
		case "keepttl":
			if expUnit != "" {
				return fail(errSyntax)
			}
			o.keepttl = true
		case "ex", "px", "exat", "pxat":
			if o.keepttl || (expUnit != "" && expUnit != opt) {
				return fail(errSyntax)
			}
			if i+1 >= len(args) {
				return fail(errSyntax)
			}
			v, ok := parseIntStrict(args[i+1])
			if !ok {
				return fail(errNotInt)
			}
			if v <= 0 {
				return fail(errInvalidExpire("set"))
			}
			expUnit = opt
			o.hasExpire = true
			o.expUnit = opt
			o.expireAtMs = v // raw value; resolveExpire converts it
			i++
		default:
			return fail(errSyntax)
		}
	}
	return o, resp.Value{}, false
}

func errInvalidExpire(cmd string) resp.Value {
	return resp.Err(fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
}

// resolveExpire converts the raw option value to absolute ms, catching
// unit-conversion overflow the way Redis does. Returns false on error.
func (o *setOptions) resolveExpire(now int64) (resp.Value, bool) {
	if !o.hasExpire {
		return resp.Value{}, false
	}
	v := o.expireAtMs
	switch o.expUnit {
	case "ex":
		if v > (math.MaxInt64-now)/1000 {
			return errInvalidExpire("set"), true
		}
		o.expireAtMs = now + v*1000
	case "px":
		o.expireAtMs = now + v
	case "exat":
		if v > math.MaxInt64/1000 {
			return errInvalidExpire("set"), true
		}
		o.expireAtMs = v * 1000
	case "pxat":
		o.expireAtMs = v
	}
	return resp.Value{}, false
}

func cmdSet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	o, errV, failed := parseSetOptions(args)
	if failed {
		return errV
	}
	if errV, failed := o.resolveExpire(e.Shards.NowMs()); failed {
		return errV
	}
	key, val := string(args[1]), args[2]
	var reply resp.Value
	var applied bool
	e.do(cs, args[1], func(s *shard.Shard) {
		old, found := s.Lookup(cs.DB, key)
		blocked := (o.nx && found) || (o.xx && !found)
		applied = !blocked
		if !blocked {
			switch {
			case o.hasExpire && o.expireAtMs <= e.Shards.NowMs():
				// expiry already in the past: store-then-expire == delete
				if found {
					s.Delete(cs.DB, key)
				}
			default:
				ent := &shard.Entry{Type: shard.TypeString, Str: dupBytes(val)}
				if o.keepttl && found {
					ent.ExpireAtMs = old.ExpireAtMs
					ent.ExpGen = old.ExpGen // old heap entries stay valid
				} else if o.hasExpire {
					ent.ExpireAtMs = o.expireAtMs
				}
				s.Store(cs.DB, key, ent)
				if o.hasExpire {
					s.PushExpire(cs.DB, key, ent)
				}
			}
		}
		switch {
		case o.get && found:
			reply = resp.BlobString(old.Str)
		case o.get:
			reply = resp.Null()
		case blocked:
			reply = resp.Null()
		default:
			reply = replyOK
		}
	})
	if applied {
		e.notifyKeyspace(cs.DB, key, "set")
		if o.hasExpire {
			e.notifyKeyspace(cs.DB, key, "expire")
		}
	}
	return reply
}

// --- GET and family ---------------------------------------------------------

func cmdGet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		if ent, ok := s.Lookup(cs.DB, key); ok {
			if ent.Type != shard.TypeString {
				reply = errWrongType
				return
			}
			reply = resp.BlobString(ent.Str)
		} else {
			reply = resp.Null()
		}
	})
	return reply
}

func cmdGetSet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	var stored bool
	e.do(cs, args[1], func(s *shard.Shard) {
		old, found := s.Lookup(cs.DB, key)
		if found && old.Type != shard.TypeString {
			reply = errWrongType
			return
		}
		s.Store(cs.DB, key, &shard.Entry{Type: shard.TypeString, Str: dupBytes(args[2])})
		stored = true
		if found {
			reply = resp.BlobString(old.Str)
		} else {
			reply = resp.Null()
		}
	})
	if stored {
		e.notifyKeyspace(cs.DB, key, "set")
	}
	return reply
}

func cmdGetDel(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	var deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		if ent, ok := s.Lookup(cs.DB, key); ok {
			if ent.Type != shard.TypeString {
				reply = errWrongType
				return
			}
			s.Delete(cs.DB, key)
			deleted = true
			reply = resp.BlobString(ent.Str)
		} else {
			reply = resp.Null()
		}
	})
	if deleted {
		e.notifyKeyspace(cs.DB, key, "del")
	}
	return reply
}

func cmdGetEx(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	// Phase 1: syntax — unknown options, missing values, and non-integer
	// values error even when the key is missing; the >0 check is deferred
	// to when a live key is found (Redis getexCommand ordering).
	persist := false
	expKind := ""
	var expRaw []byte // value parse is deferred until a live key is found
	for i := 2; i < len(args); i++ {
		switch opt := lowerASCII(args[i]); opt {
		case "persist":
			if expKind != "" {
				return errSyntax
			}
			persist = true
		case "ex", "px", "exat", "pxat":
			if persist || expKind != "" {
				return errSyntax
			}
			if i+1 >= len(args) {
				return errSyntax
			}
			expKind, expRaw = opt, args[i+1]
			i++
		default:
			return errSyntax
		}
	}
	key := string(args[1])
	var reply resp.Value
	var persisted, expireSet, deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, found := s.Lookup(cs.DB, key)
		if !found {
			reply = resp.Null()
			return
		}
		if ent.Type != shard.TypeString {
			reply = errWrongType
			return
		}
		reply = resp.BlobString(ent.Str)
		now := e.Shards.NowMs()
		switch {
		case persist:
			persisted = ent.ExpireAtMs != 0
			ent.ExpireAtMs = 0
			s.Touch(ent)
		case expKind != "":
			expVal, ok := parseIntStrict(expRaw)
			if !ok {
				reply = errNotInt
				return
			}
			if expVal <= 0 {
				reply = errInvalidExpire("getex")
				return
			}
			var at int64
			switch expKind {
			case "ex":
				if expVal > (math.MaxInt64-now)/1000 {
					reply = errInvalidExpire("getex")
					return
				}
				at = now + expVal*1000
			case "px":
				at = now + expVal
			case "exat":
				if expVal > math.MaxInt64/1000 {
					reply = errInvalidExpire("getex")
					return
				}
				at = expVal * 1000
			case "pxat":
				at = expVal
			}
			if at <= now {
				// Return the value, drop the key. Delete the live entry
				// directly (see expireCommon): a pre-set past ExpireAtMs
				// would surface as a spurious "expired", Redis emits "del".
				s.Delete(cs.DB, key)
				deleted = true
			} else {
				ent.ExpireAtMs = at
				s.Touch(ent)
				s.PushExpire(cs.DB, key, ent)
				expireSet = true
			}
		}
	})
	switch {
	case persisted:
		e.notifyKeyspace(cs.DB, key, "persist")
	case deleted:
		e.notifyKeyspace(cs.DB, key, "del")
	case expireSet:
		e.notifyKeyspace(cs.DB, key, "expire")
	}
	return reply
}

// --- counters ----------------------------------------------------------------

func cmdIncr(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return incrBy(e, cs, args[1], 1)
}

func cmdDecr(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return incrBy(e, cs, args[1], -1)
}

func cmdIncrBy(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	v, ok := parseIntStrict(args[2])
	if !ok {
		return errNotInt
	}
	return incrBy(e, cs, args[1], v)
}

func cmdDecrBy(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	v, ok := parseIntStrict(args[2])
	if !ok {
		return errNotInt
	}
	if v == math.MinInt64 {
		return errDecrOvf // -(-2^63) cannot be represented
	}
	return incrBy(e, cs, args[1], -v)
}

func incrBy(e *Engine, cs *ConnState, keyB []byte, delta int64) resp.Value {
	key := string(keyB)
	var reply resp.Value
	var done bool
	e.do(cs, keyB, func(s *shard.Shard) {
		var cur int64
		ent, found := s.Lookup(cs.DB, key)
		if found {
			if ent.Type != shard.TypeString {
				reply = errWrongType
				return
			}
			v, ok := parseIntStrict(ent.Str)
			if !ok {
				reply = errNotInt
				return
			}
			cur = v
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			reply = errIncrOvf
			return
		}
		cur += delta
		if found {
			ent.Str = []byte(fmt.Sprintf("%d", cur))
			s.Touch(ent)
		} else {
			s.Store(cs.DB, key, &shard.Entry{
				Type: shard.TypeString,
				Str:  []byte(fmt.Sprintf("%d", cur)),
			})
		}
		reply = resp.Int(cur)
		done = true
	})
	if done {
		// Redis routes DECR/DECRBY through incrDecrCommand too: the event
		// is always "incrby".
		e.notifyKeyspace(cs.DB, key, "incrby")
	}
	return reply
}

// cmdIncrByFloat is INCRBYFLOAT: the float twin of incrBy. The stored value
// and the reply use the Redis ld2string rendering (formatHumanFloat); the
// reply is a bulk string, not a RESP3 double, matching Redis. Redis does
// not reject a NaN/Inf delta up front (unlike HINCRBYFLOAT): an infinite
// increment on a finite value trips the result check.
func cmdIncrByFloat(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	delta, ok := parseFloat(args[2])
	if !ok {
		return errBadFloat
	}
	key := string(args[1])
	var reply resp.Value
	var done bool
	e.do(cs, args[1], func(s *shard.Shard) {
		var cur float64
		ent, found := s.Lookup(cs.DB, key)
		if found {
			if ent.Type != shard.TypeString {
				reply = errWrongType
				return
			}
			v, ok := parseFloat(ent.Str)
			if !ok {
				reply = errBadFloat
				return
			}
			cur = v
		}
		cur += delta
		if math.IsNaN(cur) || math.IsInf(cur, 0) {
			reply = resp.Err("ERR increment would produce NaN or Infinity")
			return
		}
		out := formatHumanFloat(cur)
		if found {
			ent.Str = []byte(out)
			s.Touch(ent)
		} else {
			s.Store(cs.DB, key, &shard.Entry{
				Type: shard.TypeString,
				Str:  []byte(out),
			})
		}
		reply = resp.BlobStr(out)
		done = true
	})
	if done {
		e.notifyKeyspace(cs.DB, key, "incrbyfloat")
	}
	return reply
}

// --- append / strlen ---------------------------------------------------------

func cmdAppend(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	var done bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, found := s.Lookup(cs.DB, key)
		if found {
			if ent.Type != shard.TypeString {
				reply = errWrongType
				return
			}
			ent.Str = append(ent.Str, args[2]...)
			s.Touch(ent)
			reply = resp.Int(int64(len(ent.Str)))
			done = true
			return
		}
		s.Store(cs.DB, key, &shard.Entry{
			Type: shard.TypeString,
			Str:  dupBytes(args[2]),
		})
		reply = resp.Int(int64(len(args[2])))
		done = true
	})
	if done {
		e.notifyKeyspace(cs.DB, key, "append")
	}
	return reply
}

func cmdStrLen(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		if ent, found := s.Lookup(cs.DB, key); found {
			if ent.Type != shard.TypeString {
				reply = errWrongType
				return
			}
			reply = resp.Int(int64(len(ent.Str)))
		} else {
			reply = resp.Int(0)
		}
	})
	return reply
}

// --- multi-key (§4.2 fan-out) -------------------------------------------------

func cmdMGet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	keys := args[1:]
	vals := make([]resp.Value, len(keys))
	for i := range vals {
		vals[i] = resp.Null()
	}
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if ent, ok := s.Lookup(cs.DB, string(keys[i])); ok && ent.Type == shard.TypeString {
				vals[i] = resp.BlobString(ent.Str)
			}
		}
	})
	return resp.Arr(vals...)
}

func cmdMSet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args)%2 == 0 { // name + odd tail: unbalanced pairs
		return errArity("mset")
	}
	keys := pairKeys(args[1:])
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			s.Store(cs.DB, string(keys[i]), &shard.Entry{
				Type: shard.TypeString,
				Str:  dupBytes(args[2+2*i]),
			})
		}
	})
	// One "set" per key in argument order; doMulti fans out concurrently,
	// so emission happens here on the connection goroutine.
	for _, k := range keys {
		e.notifyKeyspace(cs.DB, string(k), "set")
	}
	return replyOK
}

func cmdMSetNX(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args)%2 == 0 {
		return errArity("msetnx")
	}
	keys := pairKeys(args[1:])
	// Fast path per §4.2: existence check phase, then write phase;
	// per-shard atomicity only (a racing writer between phases wins).
	var exists atomic.Bool // fn runs concurrently on several shard goroutines
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if _, ok := s.Lookup(cs.DB, string(keys[i])); ok {
				exists.Store(true)
			}
		}
	})
	if exists.Load() {
		return resp.Int(0)
	}
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			s.Store(cs.DB, string(keys[i]), &shard.Entry{
				Type: shard.TypeString,
				Str:  dupBytes(args[2+2*i]),
			})
		}
	})
	for _, k := range keys {
		e.notifyKeyspace(cs.DB, string(k), "set")
	}
	return resp.Int(1)
}

// dupBytes copies b, never returning nil (an empty value is a distinct
// reply from null).
func dupBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// pairKeys extracts the key of each key/value pair starting at args[0].
func pairKeys(args [][]byte) [][]byte {
	keys := make([][]byte, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		keys = append(keys, args[i])
	}
	return keys
}
