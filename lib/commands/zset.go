package commands

import (
	"math"
	"math/rand/v2"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// Sorted-set commands (design doc §7 P1). Zsets are *types.ZSet in
// Entry.Obj: pluto skip_list_ts ordered by (score, member) with span-based
// rank plus a member→score map (§5.3 #1). Emptied zsets are deleted.

var (
	errGTLTNX = resp.Err("ERR GT, LT, and/or NX options at the same time are not compatible")
	errXXNX   = resp.Err("ERR XX and NX options at the same time are not compatible")
	errNaN    = resp.Err("ERR resulting score is not a number (NaN)")
)

// --- ZADD ----------------------------------------------------------------------

func cmdZAdd(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	var nx, xx, gt, lt, ch, incr bool
	i := 2
	for ; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "nx":
			if xx {
				return errXXNX
			}
			if gt || lt {
				return errGTLTNX
			}
			nx = true
		case "xx":
			if nx {
				return errXXNX
			}
			xx = true
		case "gt":
			if nx || lt {
				return errGTLTNX
			}
			gt = true
		case "lt":
			if nx || gt {
				return errGTLTNX
			}
			lt = true
		case "ch":
			ch = true
		case "incr":
			incr = true
		default:
			goto pairs
		}
	}
pairs:
	rest := args[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return errSyntax
	}
	if incr && len(rest) != 2 {
		return resp.Err("ERR INCR option supports a single increment-element pair")
	}
	// All scores parse before the key is touched (Redis ordering).
	type sv struct {
		score  float64
		member string
	}
	pairs := make([]sv, 0, len(rest)/2)
	for j := 0; j+1 < len(rest); j += 2 {
		sc, ok := parseFloat(rest[j])
		if !ok {
			return errBadFloat
		}
		pairs = append(pairs, sv{sc, string(rest[j+1])})
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		var z *types.ZSet
		if ent == nil {
			z = types.NewZSet()
		} else {
			z = ent.Obj.(*types.ZSet)
		}
		var added, changed int64
		processed := false
		var incrScore float64
		for _, p := range pairs {
			cur, exists := z.Score(p.member)
			switch {
			case xx && !exists:
				continue
			case nx && exists:
				continue
			}
			score := p.score
			if incr && exists {
				score = cur + p.score
				if math.IsNaN(score) {
					reply = errNaN
					return
				}
			}
			// GT/LT compare against the resulting score (after INCR).
			if exists && (gt && score <= cur || lt && score >= cur) {
				continue
			}
			if z.Add(p.member, score) {
				added++
				changed++
			} else if cur != score {
				changed++
			}
			processed = true
			incrScore = score
		}
		if ent != nil && (added > 0 || changed > 0) {
			s.Touch(cs.DB, key, ent)
		}
		if added > 0 || changed > 0 {
			// Publish before waking (see pushCmd in list.go): a woken
			// BZPOPMIN/BZMPOP pops and publishes on its own goroutine.
			// zaddGenericCommand: INCR mode notifies "zincr", plain "zadd".
			ev := "zadd"
			if incr {
				ev = "zincr"
			}
			e.notifyKeyspace(cs.DB, key, ev)
		}
		if added > 0 || (incr && processed) {
			// The key holds members now; a parked BZPOPMIN/BZMPOP waiter
			// (registered while the key was missing) can proceed.
			s.WakeWaiter(cs.DB, key)
		}
		if incr {
			if !processed {
				reply = resp.Null()
				return
			}
			if ent == nil {
				storeColl(s, cs.DB, key, shard.TypeZSet, z)
			}
			reply = resp.Double(incrScore)
			return
		}
		if added > 0 && ent == nil {
			storeColl(s, cs.DB, key, shard.TypeZSet, z)
		}
		if ch {
			reply = resp.Int(changed)
		} else {
			reply = resp.Int(added)
		}
	})
	return reply
}

func cmdZIncrBy(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	delta, ok := parseFloat(args[2])
	if !ok {
		return errBadFloat
	}
	key, member := string(args[1]), string(args[3])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		var z *types.ZSet
		var cur float64
		if ent != nil {
			z = ent.Obj.(*types.ZSet)
			if c, ok := z.Score(member); ok {
				cur = c
			}
		} else {
			z = types.NewZSet()
		}
		score := cur + delta
		if math.IsNaN(score) {
			reply = errNaN
			return
		}
		z.Add(member, score)
		if ent == nil {
			storeColl(s, cs.DB, key, shard.TypeZSet, z)
		} else {
			s.Touch(cs.DB, key, ent)
		}
		e.notifyKeyspace(cs.DB, key, "zincr") // before the wake: see pushCmd
		s.WakeWaiter(cs.DB, key)              // serve parked BZPOPMIN/BZMPOP waiters
		reply = resp.Double(score)
	})
	return reply
}

func cmdZScore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key, member := string(args[1]), string(args[2])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Null()
		default:
			if sc, ok := ent.Obj.(*types.ZSet).Score(member); ok {
				reply = resp.Double(sc)
			} else {
				reply = resp.Null()
			}
		}
	})
	return reply
}

func cmdZMScore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	out := make([]resp.Value, len(args)-2)
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		for i, m := range args[2:] {
			ok := false
			var sc float64
			if ent != nil {
				sc, ok = ent.Obj.(*types.ZSet).Score(string(m))
			}
			if ok {
				out[i] = resp.Double(sc)
			} else {
				out[i] = resp.Null()
			}
		}
	})
	if reply.Kind == resp.KindError {
		return reply
	}
	return resp.Arr(out...)
}

func cmdZCard(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			reply = resp.Int(int64(ent.Obj.(*types.ZSet).Len()))
		}
	})
	return reply
}

func cmdZRem(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	var n int64
	var deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			z := ent.Obj.(*types.ZSet)
			for _, m := range args[2:] {
				if z.Remove(string(m)) {
					n++
				}
			}
			if n > 0 {
				s.Touch(cs.DB, key, ent)
			}
			if z.Len() == 0 {
				s.Delete(cs.DB, key)
				deleted = true
			}
			reply = resp.Int(n)
		}
	})
	if n > 0 {
		e.notifyKeyspace(cs.DB, key, "zrem")
		if deleted {
			e.notifyKeyspace(cs.DB, key, "del")
		}
	}
	return reply
}

// --- rank -----------------------------------------------------------------------

func zRankCmd(e *Engine, cs *ConnState, args [][]byte, rev bool) resp.Value {
	withScore := false
	if len(args) == 4 {
		if lowerASCII(args[3]) != "withscore" {
			return errSyntax
		}
		withScore = true
	}
	key, member := string(args[1]), string(args[2])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Null()
		default:
			z := ent.Obj.(*types.ZSet)
			r, ok := z.Rank(member)
			if !ok {
				reply = resp.Null()
				return
			}
			sc, _ := z.Score(member)
			if rev {
				r = z.Len() - 1 - r
			}
			if withScore {
				reply = resp.Arr(resp.Int(int64(r)), resp.Double(sc))
			} else {
				reply = resp.Int(int64(r))
			}
		}
	})
	return reply
}

func cmdZRank(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zRankCmd(e, cs, args, false)
}

func cmdZRevRank(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zRankCmd(e, cs, args, true)
}

// --- ranges ----------------------------------------------------------------------

// zrangeMode selects how start/stop are interpreted.
type zrangeMode int

const (
	byIndex zrangeMode = iota
	byScore
	byLex
)

// zrangeReq is a parsed start/stop pair; bounds are validated before the
// key is looked up (Redis parses ranges first, so bad bounds error even
// on missing keys).
type zrangeReq struct {
	mode        zrangeMode
	start, stop int64      // byIndex
	min, max    scoreBound // byScore
	lmin, lmax  lexBound   // byLex
}

// parseZRangeBounds validates startB/stopB for the mode.
func parseZRangeBounds(mode zrangeMode, startB, stopB []byte) (zrangeReq, resp.Value, bool) {
	var req zrangeReq
	req.mode = mode
	switch mode {
	case byIndex:
		start, ok1 := parseIntStrict(startB)
		stop, ok2 := parseIntStrict(stopB)
		if !ok1 || !ok2 {
			return req, errNotInt, true
		}
		req.start, req.stop = start, stop
	case byScore:
		minB, ok1 := parseScoreBound(startB)
		maxB, ok2 := parseScoreBound(stopB)
		if !ok1 || !ok2 {
			return req, errBadFloatRange, true
		}
		req.min, req.max = minB, maxB
	default: // byLex
		minB, ok1 := parseLexBound(startB)
		maxB, ok2 := parseLexBound(stopB)
		if !ok1 || !ok2 {
			return req, errBadLexRange, true
		}
		req.lmin, req.lmax = minB, maxB
	}
	return req, resp.Value{}, false
}

// window computes the ascending rank window [lo, hi) against z.
func (r zrangeReq) window(z *types.ZSet) (int, int) {
	n := z.Len()
	switch r.mode {
	case byIndex:
		start, stop := r.start, r.stop
		if start < 0 {
			start += int64(n)
			if start < 0 {
				start = 0
			}
		}
		if stop < 0 {
			stop += int64(n)
		}
		if stop >= int64(n) {
			stop = int64(n) - 1
		}
		if start > stop || start >= int64(n) || stop < 0 {
			return 0, 0
		}
		return int(start), int(stop) + 1
	case byScore:
		return z.ScoreRangeLoc(r.min.val, r.max.val, r.min.excl, r.max.excl)
	default:
		lk, lv := lexKind(r.lmin)
		hk, hv := lexKind(r.lmax)
		return z.LexRangeLoc(lk, lv, hk, hv)
	}
}

func lexKind(b lexBound) (byte, string) {
	switch b.kind {
	case '-':
		return '<', ""
	case '+':
		return '>', ""
	default:
		return b.kind, b.val
	}
}

// zrangeEmit materializes the window, honoring REV, LIMIT and WITHSCORES.
func zrangeEmit(z *types.ZSet, lo, hi int, rev bool, offset, count int64, limitSet, withScores bool, proto int) resp.Value {
	if lo > hi {
		lo, hi = 0, 0
	}
	var idxs []int
	if rev {
		for i := hi - 1; i >= lo; i-- {
			idxs = append(idxs, i)
		}
	} else {
		for i := lo; i < hi; i++ {
			idxs = append(idxs, i)
		}
	}
	if limitSet {
		if offset < 0 {
			idxs = nil
		} else if offset >= int64(len(idxs)) {
			idxs = nil
		} else {
			idxs = idxs[offset:]
			if count >= 0 && count < int64(len(idxs)) {
				idxs = idxs[:count]
			}
		}
	}
	if !withScores {
		out := make([]resp.Value, 0, len(idxs))
		for _, i := range idxs {
			el, _ := z.At(i)
			out = append(out, resp.BlobStr(el.Member))
		}
		return resp.Arr(out...)
	}
	if proto == 3 {
		out := make([]resp.Value, 0, len(idxs))
		for _, i := range idxs {
			el, _ := z.At(i)
			out = append(out, resp.Arr(resp.BlobStr(el.Member), resp.Double(el.Score)))
		}
		return resp.Arr(out...)
	}
	out := make([]resp.Value, 0, len(idxs)*2)
	for _, i := range idxs {
		el, _ := z.At(i)
		out = append(out, resp.BlobStr(el.Member), resp.BlobStr(resp.FormatDouble(el.Score)))
	}
	return resp.Arr(out...)
}

// cmdZRange handles ZRANGE key start stop [BYSCORE|BYLEX] [REV]
// [LIMIT offset count] [WITHSCORES].
func cmdZRange(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	var hasByScore, hasByLex, rev, withScores, limitSet bool
	var offset, count int64
	i := 4
	for ; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "byscore":
			if hasByLex {
				return errSyntax
			}
			hasByScore = true
		case "bylex":
			if hasByScore {
				return errSyntax
			}
			hasByLex = true
		case "rev":
			rev = true
		case "withscores":
			withScores = true
		case "limit":
			if i+2 >= len(args) {
				return errSyntax
			}
			o, ok1 := parseIntStrict(args[i+1])
			c, ok2 := parseIntStrict(args[i+2])
			if !ok1 || !ok2 {
				return errNotInt
			}
			offset, count, limitSet = o, c, true
			i += 2
		default:
			return errSyntax
		}
	}
	if limitSet && !hasByScore && !hasByLex {
		return resp.Err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
	}
	if withScores && hasByLex {
		return resp.Err("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
	}
	mode := byIndex
	if hasByScore {
		mode = byScore
	} else if hasByLex {
		mode = byLex
	}
	startB, stopB := args[2], args[3]
	if rev && mode != byIndex {
		startB, stopB = stopB, startB // REV: arguments are max min
	}
	req, errV, failed := parseZRangeBounds(mode, startB, stopB)
	if failed {
		return errV
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			reply = resp.Arr()
			return
		}
		z := ent.Obj.(*types.ZSet)
		lo, hi := req.window(z)
		reply = zrangeEmit(z, lo, hi, rev, offset, count, limitSet, withScores, cs.Proto)
	})
	return reply
}

// zrangeByCmd backs ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX:
// key min max [WITHSCORES] [LIMIT offset count] (max min when rev).
func zrangeByCmd(e *Engine, cs *ConnState, args [][]byte, mode zrangeMode, rev bool) resp.Value {
	var withScores, limitSet bool
	var offset, count int64
	i := 4
	for ; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "withscores":
			if mode == byLex {
				return resp.Err("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
			}
			withScores = true
		case "limit":
			if i+2 >= len(args) {
				return errSyntax
			}
			o, ok1 := parseIntStrict(args[i+1])
			c, ok2 := parseIntStrict(args[i+2])
			if !ok1 || !ok2 {
				return errNotInt
			}
			offset, count, limitSet = o, c, true
			i += 2
		default:
			return errSyntax
		}
	}
	minB, maxB := args[2], args[3]
	if rev {
		minB, maxB = maxB, minB
	}
	req, errV, failed := parseZRangeBounds(mode, minB, maxB)
	if failed {
		return errV
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			reply = resp.Arr()
			return
		}
		z := ent.Obj.(*types.ZSet)
		lo, hi := req.window(z)
		reply = zrangeEmit(z, lo, hi, rev, offset, count, limitSet, withScores, cs.Proto)
	})
	return reply
}

func cmdZRangeByScore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zrangeByCmd(e, cs, args, byScore, false)
}

func cmdZRevRangeByScore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zrangeByCmd(e, cs, args, byScore, true)
}

func cmdZRangeByLex(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zrangeByCmd(e, cs, args, byLex, false)
}

func cmdZRevRange(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	// ZREVRANGE key start stop [WITHSCORES]: the pre-6.2 form; build a
	// synthetic ZRANGE-equivalent call.
	withScores := false
	for i := 4; i < len(args); i++ {
		if lowerASCII(args[i]) != "withscores" {
			return errSyntax
		}
		withScores = true
	}
	start, ok1 := parseIntStrict(args[2])
	stop, ok2 := parseIntStrict(args[3])
	if !ok1 || !ok2 {
		return errNotInt
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			reply = resp.Arr()
			return
		}
		z := ent.Obj.(*types.ZSet)
		// ZREVRANGE indexes count from the tail: reversed index j is
		// forward index n-1-j. Normalize in reversed space (Redis
		// zrevrangeCommand), then map to a forward window.
		n := int64(z.Len())
		s0, s1 := start, stop
		if s0 < 0 {
			s0 += n
		}
		if s1 < 0 {
			s1 += n
		}
		if s0 < 0 {
			s0 = 0
		}
		if s0 > s1 || s0 >= n || s1 < 0 {
			reply = resp.Arr()
			return
		}
		if s1 >= n {
			s1 = n - 1
		}
		lo, hi := int(n-1-s1), int(n-s0)
		reply = zrangeEmit(z, lo, hi, true, 0, 0, false, withScores, cs.Proto)
	})
	return reply
}

// --- removals by range ------------------------------------------------------------

func cmdZRemRangeByRank(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	start, ok1 := parseIntStrict(args[2])
	stop, ok2 := parseIntStrict(args[3])
	if !ok1 || !ok2 {
		return errNotInt
	}
	key := string(args[1])
	var reply resp.Value
	var n int
	var deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			z := ent.Obj.(*types.ZSet)
			req := zrangeReq{mode: byIndex, start: start, stop: stop}
			lo, hi := req.window(z)
			n = z.RemoveRankRange(lo, hi-1)
			if n > 0 {
				s.Touch(cs.DB, key, ent)
			}
			if z.Len() == 0 {
				s.Delete(cs.DB, key)
				deleted = true
			}
			reply = resp.Int(int64(n))
		}
	})
	if n > 0 {
		e.notifyKeyspace(cs.DB, key, "zremrangebyrank")
		if deleted {
			e.notifyKeyspace(cs.DB, key, "del")
		}
	}
	return reply
}

func cmdZRemRangeByScore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	minB, ok1 := parseScoreBound(args[2])
	maxB, ok2 := parseScoreBound(args[3])
	if !ok1 || !ok2 {
		return errBadFloatRange
	}
	return zRemRange(e, cs, args[1], "zremrangebyscore", func(z *types.ZSet) (int, int) {
		return z.ScoreRangeLoc(minB.val, maxB.val, minB.excl, maxB.excl)
	})
}

func cmdZRemRangeByLex(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	minB, ok1 := parseLexBound(args[2])
	maxB, ok2 := parseLexBound(args[3])
	if !ok1 || !ok2 {
		return errBadLexRange
	}
	lk, lv := lexKind(minB)
	hk, hv := lexKind(maxB)
	return zRemRange(e, cs, args[1], "zremrangebylex", func(z *types.ZSet) (int, int) {
		return z.LexRangeLoc(lk, lv, hk, hv)
	})
}

// zRemRange deletes the window [lo, hi) and reports the count.
func zRemRange(e *Engine, cs *ConnState, keyB []byte, event string, loc func(*types.ZSet) (int, int)) resp.Value {
	key := string(keyB)
	var reply resp.Value
	var n int
	var deleted bool
	e.do(cs, keyB, func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			z := ent.Obj.(*types.ZSet)
			lo, hi := loc(z)
			if lo > hi {
				lo, hi = 0, 0
			}
			n = z.RemoveRankRange(lo, hi-1)
			if n > 0 {
				s.Touch(cs.DB, key, ent)
			}
			if z.Len() == 0 {
				s.Delete(cs.DB, key)
				deleted = true
			}
			reply = resp.Int(int64(n))
		}
	})
	if n > 0 {
		e.notifyKeyspace(cs.DB, key, event)
		if deleted {
			e.notifyKeyspace(cs.DB, key, "del")
		}
	}
	return reply
}

// --- counts ------------------------------------------------------------------------

func cmdZCount(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	minB, ok1 := parseScoreBound(args[2])
	maxB, ok2 := parseScoreBound(args[3])
	if !ok1 || !ok2 {
		return errBadFloatRange
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			lo, hi := ent.Obj.(*types.ZSet).ScoreRangeLoc(minB.val, maxB.val, minB.excl, maxB.excl)
			reply = resp.Int(int64(max(hi-lo, 0)))
		}
	})
	return reply
}

func cmdZLexCount(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	minB, ok1 := parseLexBound(args[2])
	maxB, ok2 := parseLexBound(args[3])
	if !ok1 || !ok2 {
		return errBadLexRange
	}
	lk, lv := lexKind(minB)
	hk, hv := lexKind(maxB)
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			lo, hi := ent.Obj.(*types.ZSet).LexRangeLoc(lk, lv, hk, hv)
			reply = resp.Int(int64(max(hi-lo, 0)))
		}
	})
	return reply
}

// --- pops ---------------------------------------------------------------------------

func cmdZPopMin(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zPopCmd(e, cs, args, true)
}

func cmdZPopMax(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zPopCmd(e, cs, args, false)
}

func zPopCmd(e *Engine, cs *ConnState, args [][]byte, fromMin bool) resp.Value {
	if len(args) > 3 {
		return errArity(lowerASCII(args[0]))
	}
	hasCount := len(args) == 3
	var count int64 = 1
	if hasCount {
		v, ok := parseIntStrict(args[2])
		if !ok || v < 0 {
			return errNotPositive
		}
		count = v
	}
	key := string(args[1])
	var reply resp.Value
	var didPop, deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil || count == 0 {
			reply = resp.Arr()
			return
		}
		z := ent.Obj.(*types.ZSet)
		n := min(int(count), z.Len())
		didPop = true
		popped := make([]types.ZElem, 0, n)
		for range n {
			i := 0
			if !fromMin {
				i = z.Len() - 1
			}
			el, _ := z.At(i)
			popped = append(popped, el)
			z.Remove(el.Member)
		}
		s.Touch(cs.DB, key, ent)
		if z.Len() == 0 {
			s.Delete(cs.DB, key)
			deleted = true
		}
		if !hasCount {
			el := popped[0]
			if cs.Proto == 3 {
				reply = resp.Arr(resp.BlobStr(el.Member), resp.Double(el.Score))
			} else {
				reply = resp.Arr(resp.BlobStr(el.Member), resp.BlobStr(resp.FormatDouble(el.Score)))
			}
			return
		}
		if cs.Proto == 3 {
			out := make([]resp.Value, 0, len(popped))
			for _, el := range popped {
				out = append(out, resp.Arr(resp.BlobStr(el.Member), resp.Double(el.Score)))
			}
			reply = resp.Arr(out...)
			return
		}
		out := make([]resp.Value, 0, len(popped)*2)
		for _, el := range popped {
			out = append(out, resp.BlobStr(el.Member), resp.BlobStr(resp.FormatDouble(el.Score)))
		}
		reply = resp.Arr(out...)
	})
	if didPop {
		ev := "zpopmax"
		if fromMin {
			ev = "zpopmin"
		}
		e.notifyKeyspace(cs.DB, key, ev)
		if deleted {
			e.notifyKeyspace(cs.DB, key, "del")
		}
	}
	return reply
}

func cmdZRandMember(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	withScores := false
	hasCount := false
	var count int64
	if len(args) >= 3 {
		v, ok := parseIntStrict(args[2])
		if !ok {
			return errNotInt
		}
		count, hasCount = v, true
	}
	if len(args) >= 4 {
		if lowerASCII(args[3]) != "withscores" {
			return errSyntax
		}
		withScores = true
	}
	if len(args) > 4 {
		return errSyntax
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			if hasCount {
				reply = resp.Arr()
			} else {
				reply = resp.Null()
			}
			return
		}
		z := ent.Obj.(*types.ZSet)
		n := z.Len()
		pick := func(i int) types.ZElem {
			el, _ := z.At(i)
			return el
		}
		var chosen []types.ZElem
		if !hasCount {
			chosen = []types.ZElem{pick(rand.IntN(n))}
		} else if count >= 0 {
			k := min(int(count), n)
			if int(count) >= n {
				// count >= size: every member in skiplist order,
				// unshuffled (Redis 7.2 fast path).
				for i := 0; i < n; i++ {
					chosen = append(chosen, pick(i))
				}
			} else {
				for _, i := range rand.Perm(n)[:k] {
					chosen = append(chosen, pick(i))
				}
			}
		} else {
			for i := int64(0); i < -count; i++ {
				chosen = append(chosen, pick(rand.IntN(n)))
			}
		}
		emit := func(el types.ZElem, out *[]resp.Value) {
			if !withScores {
				*out = append(*out, resp.BlobStr(el.Member))
			} else if cs.Proto == 3 {
				*out = append(*out, resp.Arr(resp.BlobStr(el.Member), resp.Double(el.Score)))
			} else {
				*out = append(*out, resp.BlobStr(el.Member), resp.BlobStr(resp.FormatDouble(el.Score)))
			}
		}
		if !hasCount {
			var out []resp.Value
			emit(chosen[0], &out)
			reply = out[0]
			return
		}
		out := make([]resp.Value, 0, len(chosen))
		for _, el := range chosen {
			emit(el, &out)
		}
		reply = resp.Arr(out...)
	})
	return reply
}

// --- multi-key algebra ---------------------------------------------------------------

// zSnapshot is a read-only view of one key for ZUNION/ZINTER/ZDIFF:
// members with scores (sets count as score 1).
type zSnapshot struct {
	members map[string]float64
	order   []string // skiplist order of a zset, member order of a set
	present bool
}

// snapshotZSets reads zset/set contents for the given keys; any other
// live type is WRONGTYPE.
func snapshotZSets(e *Engine, cs *ConnState, keys [][]byte) ([]zSnapshot, resp.Value, bool) {
	snaps := make([]zSnapshot, len(keys))
	wt := make([]bool, len(keys)) // slot-indexed: the fan-out runs concurrently
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			ent, ok := s.Lookup(cs.DB, string(keys[i]))
			if !ok {
				continue
			}
			switch ent.Type {
			case shard.TypeZSet:
				z := ent.Obj.(*types.ZSet)
				sn := zSnapshot{present: true, members: make(map[string]float64, z.Len())}
				for j := 0; j < z.Len(); j++ {
					el, _ := z.At(j)
					sn.members[el.Member] = el.Score
					sn.order = append(sn.order, el.Member)
				}
				snaps[i] = sn
			case shard.TypeSet:
				st := ent.Obj.(*types.Set)
				members := st.Members()
				sn := zSnapshot{present: true, members: make(map[string]float64, len(members))}
				for _, m := range members {
					sn.members[m] = 1
					sn.order = append(sn.order, m)
				}
				snaps[i] = sn
			default:
				wt[i] = true
			}
		}
	})
	for _, w := range wt {
		if w {
			return nil, errWrongType, true
		}
	}
	return snaps, resp.Value{}, false
}

const (
	aggrSum = iota
	aggrMin
	aggrMax
)

// zAggregate unions/intersects the snapshots. For intersect only members
// present in every input survive. A NaN aggregate folds to 0 (Redis 7.2
// zunioninter behavior for inf + -inf).
func zAggregate(snaps []zSnapshot, intersect bool, weights []float64, aggregate int) map[string]float64 {
	acc := map[string]float64{}
	seen := map[string]int{}
	for i, sn := range snaps {
		w := 1.0
		if weights != nil {
			w = weights[i]
		}
		for m, sc := range sn.members {
			v := sc * w
			if c, ok := acc[m]; !ok {
				acc[m] = v
				seen[m] = 1
			} else {
				seen[m]++
				switch aggregate {
				case aggrMin:
					if v < c {
						acc[m] = v
					}
				case aggrMax:
					if v > c {
						acc[m] = v
					}
				default:
					acc[m] = c + v
				}
			}
		}
	}
	if intersect {
		for m := range acc {
			if seen[m] != len(snaps) {
				delete(acc, m)
			}
		}
	}
	for m, sc := range acc {
		if math.IsNaN(sc) {
			acc[m] = 0
		}
	}
	return acc
}

// zAggregateReply renders a computed zset: members sorted by (score,
// member) — the order Redis's zunioninter result skiplist yields.
func zAggregateReply(acc map[string]float64, withScores bool, proto int) resp.Value {
	z := types.NewZSet()
	for m, sc := range acc {
		z.Add(m, sc)
	}
	n := z.Len()
	if !withScores {
		out := make([]resp.Value, 0, n)
		for i := 0; i < n; i++ {
			el, _ := z.At(i)
			out = append(out, resp.BlobStr(el.Member))
		}
		return resp.Arr(out...)
	}
	if proto == 3 {
		out := make([]resp.Value, 0, n)
		for i := 0; i < n; i++ {
			el, _ := z.At(i)
			out = append(out, resp.Arr(resp.BlobStr(el.Member), resp.Double(el.Score)))
		}
		return resp.Arr(out...)
	}
	out := make([]resp.Value, 0, n*2)
	for i := 0; i < n; i++ {
		el, _ := z.At(i)
		out = append(out, resp.BlobStr(el.Member), resp.BlobStr(resp.FormatDouble(el.Score)))
	}
	return resp.Arr(out...)
}

// zNumkeysArgs parses and validates the numkeys prefix of ZDIFF/ZINTER/
// ZUNION: (keys, rest, error).
func zNumkeysArgs(args [][]byte, cmd string) ([][]byte, [][]byte, resp.Value, bool) {
	numkeys, ok := parseIntStrict(args[1])
	if !ok {
		return nil, nil, errNotInt, true
	}
	if numkeys < 1 {
		return nil, nil, errArity(cmd), true
	}
	if int(numkeys) > len(args)-2 {
		return nil, nil, errSyntax, true
	}
	return args[2 : 2+numkeys], args[2+numkeys:], resp.Value{}, false
}

func cmdZDiff(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	keys, rest, errV, failed := zNumkeysArgs(args, "zdiff")
	if failed {
		return errV
	}
	withScores := false
	if len(rest) == 1 && lowerASCII(rest[0]) == "withscores" {
		withScores = true
	} else if len(rest) != 0 {
		return errSyntax
	}
	snaps, errV, failed := snapshotZSets(e, cs, keys)
	if failed {
		return errV
	}
	// Members of the first key absent from all the rest, in the first
	// key's order.
	others := map[string]bool{}
	for _, sn := range snaps[1:] {
		for m := range sn.members {
			others[m] = true
		}
	}
	acc := map[string]float64{}
	if len(snaps) > 0 && snaps[0].present {
		for _, m := range snaps[0].order {
			if !others[m] {
				acc[m] = snaps[0].members[m]
			}
		}
	}
	return zAggregateReply(acc, withScores, cs.Proto)
}

func cmdZInter(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zUnionInterCmd(e, cs, args, "zinter", true)
}

func cmdZUnion(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zUnionInterCmd(e, cs, args, "zunion", false)
}

func zUnionInterCmd(e *Engine, cs *ConnState, args [][]byte, cmd string, intersect bool) resp.Value {
	keys, rest, errV, failed := zNumkeysArgs(args, cmd)
	if failed {
		return errV
	}
	withScores := false
	if len(rest) == 1 && lowerASCII(rest[0]) == "withscores" {
		withScores = true
	} else if len(rest) != 0 {
		return errSyntax
	}
	snaps, errV, failed := snapshotZSets(e, cs, keys)
	if failed {
		return errV
	}
	return zAggregateReply(zAggregate(snaps, intersect, nil, aggrSum), withScores, cs.Proto)
}

// zStoreCmd backs ZINTERSTORE/ZUNIONSTORE:
// dest numkeys key... [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX].
func zStoreCmd(e *Engine, cs *ConnState, args [][]byte, cmd string, intersect bool) resp.Value {
	numkeys, ok := parseIntStrict(args[2])
	if !ok {
		return errNotInt
	}
	if numkeys < 1 {
		return errArity(cmd)
	}
	if int(numkeys) > len(args)-3 {
		return errSyntax
	}
	keys := args[3 : 3+numkeys]
	var weights []float64
	aggregate := aggrSum
	i := 3 + int(numkeys)
	for i < len(args) {
		switch lowerASCII(args[i]) {
		case "weights":
			if i+int(numkeys) >= len(args) {
				return errSyntax
			}
			weights = make([]float64, numkeys)
			for j := range weights {
				w, ok := parseFloat(args[i+1+j])
				if !ok {
					return resp.Err("ERR weight value is not a float")
				}
				weights[j] = w
			}
			i += 1 + int(numkeys)
		case "aggregate":
			if i+1 >= len(args) {
				return errSyntax
			}
			switch lowerASCII(args[i+1]) {
			case "sum":
				aggregate = aggrSum
			case "min":
				aggregate = aggrMin
			case "max":
				aggregate = aggrMax
			default:
				return errSyntax
			}
			i += 2
		default:
			return errSyntax
		}
	}
	snaps, errV, failed := snapshotZSets(e, cs, keys)
	if failed {
		return errV
	}
	acc := zAggregate(snaps, intersect, weights, aggregate)
	dest := string(args[1])
	var reply resp.Value
	var deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		if len(acc) == 0 {
			deleted = s.Delete(cs.DB, dest)
			reply = resp.Int(0)
			return
		}
		z := types.NewZSet()
		for m, sc := range acc {
			z.Add(m, sc)
		}
		storeColl(s, cs.DB, dest, shard.TypeZSet, z)
		reply = resp.Int(int64(len(acc)))
	})
	// zstoreGenericCommand: the store event name equals the command name;
	// an empty result that removed an existing dest emits "del" instead.
	if len(acc) > 0 {
		e.notifyKeyspace(cs.DB, dest, cmd)
	} else if deleted {
		e.notifyKeyspace(cs.DB, dest, "del")
	}
	return reply
}

func cmdZInterStore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zStoreCmd(e, cs, args, "zinterstore", true)
}

func cmdZUnionStore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return zStoreCmd(e, cs, args, "zunionstore", false)
}

func cmdZScan(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	cursor, ok := parseCursor(args[2])
	if !ok {
		return resp.Err("ERR invalid cursor")
	}
	match, count, errV, failed := scanOpts(args, 3)
	if failed {
		return errV
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeZSet)
		if wt {
			reply = errWrongType
			return
		}
		var items []types.ZElem
		if ent != nil {
			z := ent.Obj.(*types.ZSet)
			items = make([]types.ZElem, 0, z.Len())
			for i := 0; i < z.Len(); i++ {
				el, _ := z.At(i)
				items = append(items, el)
			}
		}
		kept, next := scanWindow(items, cursor, count, func(el types.ZElem) bool {
			return match == nil || GlobMatch(match, []byte(el.Member))
		})
		out := make([]resp.Value, 0, len(kept)*2)
		for _, el := range kept {
			out = append(out, resp.BlobStr(el.Member), resp.BlobStr(resp.FormatDouble(el.Score)))
		}
		reply = resp.Arr(resp.BlobStr(next), resp.Arr(out...))
	})
	return reply
}
