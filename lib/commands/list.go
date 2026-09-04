package commands

import (
	"bytes"
	"sort"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// List commands (design doc §7 P1). Lists are *types.List (pluto
// quicklist_ts, §5.3 #10) in Entry.Obj; a list emptied by pops, LREM or
// LTRIM is deleted as a key, exactly like Redis.

func cmdLPush(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return pushCmd(e, cs, args, true, false)
}

func cmdRPush(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return pushCmd(e, cs, args, false, false)
}

func cmdLPushX(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return pushCmd(e, cs, args, true, true)
}

func cmdRPushX(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return pushCmd(e, cs, args, false, true)
}

func pushCmd(e *Engine, cs *ConnState, args [][]byte, head, onlyIfExists bool) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil && onlyIfExists {
			reply = resp.Int(0)
			return
		}
		var l *types.List
		if ent == nil {
			l = types.NewList()
			storeColl(s, cs.DB, key, shard.TypeList, l)
		} else {
			l = ent.Obj.(*types.List)
		}
		for _, v := range args[2:] {
			if head {
				l.PushHead(dupBytes(v))
			} else {
				l.PushTail(dupBytes(v))
			}
		}
		if ent != nil {
			s.Touch(ent)
		}
		s.WakeWaiter(cs.DB, key) // serve parked BLPOP/BLMOVE/BLMPOP waiters
		reply = resp.Int(int64(l.Len()))
	})
	return reply
}

func cmdLPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return popCmd(e, cs, args, true)
}

func cmdRPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return popCmd(e, cs, args, false)
}

func popCmd(e *Engine, cs *ConnState, args [][]byte, head bool) resp.Value {
	if len(args) > 3 {
		return errArity(lowerASCII(args[0]))
	}
	hasCount := len(args) == 3
	var count int64
	if hasCount {
		v, ok := parseIntStrict(args[2])
		if !ok || v < 0 {
			return errNotPositive
		}
		count = v
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			if hasCount {
				reply = resp.Null()
			} else {
				reply = resp.Null()
			}
		default:
			l := ent.Obj.(*types.List)
			if !hasCount {
				v, _ := popOne(l, head)
				s.Touch(ent)
				if l.Len() == 0 {
					s.Delete(cs.DB, key)
				}
				reply = resp.BlobString(v)
				return
			}
			if count == 0 {
				reply = resp.Arr()
				return
			}
			n := min(int(count), l.Len())
			out := make([]resp.Value, 0, n)
			for range n {
				v, _ := popOne(l, head)
				out = append(out, resp.BlobString(v))
			}
			s.Touch(ent)
			if l.Len() == 0 {
				s.Delete(cs.DB, key)
			}
			reply = resp.Arr(out...)
		}
	})
	return reply
}

func popOne(l *types.List, head bool) ([]byte, bool) {
	if head {
		return l.PopHead()
	}
	return l.PopTail()
}

func cmdLLen(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			reply = resp.Int(int64(ent.Obj.(*types.List).Len()))
		}
	})
	return reply
}

func cmdLIndex(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Null() // missing key wins over a bad index
		default:
			idx, ok := parseIntStrict(args[2])
			if !ok {
				reply = errNotInt
				return
			}
			if v, ok := ent.Obj.(*types.List).At(int(idx)); ok {
				reply = resp.BlobString(v)
			} else {
				reply = resp.Null()
			}
		}
	})
	return reply
}

func cmdLRange(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	start, ok1 := parseIntStrict(args[2])
	stop, ok2 := parseIntStrict(args[3])
	if !ok1 || !ok2 {
		return errNotInt
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		if wt {
			reply = errWrongType
			return
		}
		out := []resp.Value{}
		if ent != nil {
			for _, v := range ent.Obj.(*types.List).Range(int(start), int(stop)) {
				out = append(out, resp.BlobString(v))
			}
		}
		reply = resp.Arr(out...)
	})
	return reply
}

func cmdLSet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Err("ERR no such key") // missing key wins over a bad index
		default:
			idx, ok := parseIntStrict(args[2])
			if !ok {
				reply = errNotInt
				return
			}
			if !ent.Obj.(*types.List).Set(int(idx), dupBytes(args[3])) {
				reply = resp.Err("ERR index out of range")
			} else {
				s.Touch(ent)
				reply = replyOK
			}
		}
	})
	return reply
}

func cmdLInsert(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	var before bool
	switch lowerASCII(args[2]) {
	case "before":
		before = true
	case "after":
	default:
		return errSyntax
	}
	key := string(args[1])
	pivot := args[3]
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			l := ent.Obj.(*types.List)
			idx := -1
			for i, v := range l.All() {
				if bytes.Equal(v, pivot) {
					idx = i
					break
				}
			}
			if idx < 0 {
				reply = resp.Int(-1)
				return
			}
			if before {
				l.InsertBefore(idx, dupBytes(args[4]))
			} else {
				l.InsertAfter(idx, dupBytes(args[4]))
			}
			s.Touch(ent)
			reply = resp.Int(int64(l.Len()))
		}
	})
	return reply
}

func cmdLRem(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	count, ok := parseIntStrict(args[2])
	if !ok {
		return errNotInt
	}
	key := string(args[1])
	val := args[3]
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			l := ent.Obj.(*types.List)
			var idxs []int
			if count >= 0 {
				limit := int(count)
				for i, v := range l.All() {
					if bytes.Equal(v, val) {
						idxs = append(idxs, i)
						if limit > 0 && len(idxs) == limit {
							break
						}
					}
				}
			} else {
				limit := int(-count)
				l.Lock()
				n := l.NlLen()
				for i := n - 1; i >= 0; i-- {
					v, _ := l.NlAt(i)
					if bytes.Equal(v, val) {
						idxs = append(idxs, i)
						if len(idxs) == limit {
							break
						}
					}
				}
				l.Unlock()
			}
			// Delete by descending index so earlier positions stay valid.
			sort.Sort(sort.Reverse(sort.IntSlice(idxs)))
			for _, i := range idxs {
				l.Delete(i)
			}
			if len(idxs) > 0 {
				s.Touch(ent)
			}
			if l.Len() == 0 {
				s.Delete(cs.DB, key)
			}
			reply = resp.Int(int64(len(idxs)))
		}
	})
	return reply
}

func cmdLTrim(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	start, ok1 := parseIntStrict(args[2])
	stop, ok2 := parseIntStrict(args[3])
	if !ok1 || !ok2 {
		return errNotInt
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = replyOK
		default:
			l := ent.Obj.(*types.List)
			l.Trim(int(start), int(stop))
			s.Touch(ent)
			if l.Len() == 0 {
				s.Delete(cs.DB, key)
			}
			reply = replyOK
		}
	})
	return reply
}

func cmdRPopLPush(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return lmoveCmd(e, cs, args[1], args[2], false, true)
}

func cmdLMove(e *Engine, cs *ConnState, args [][]byte) resp.Value {
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
	return lmoveCmd(e, cs, args[1], args[2], srcHead, dstHead)
}

// lmoveCmd implements RPOPLPUSH/LMOVE: pop from one end of src, push to
// one end of dst. Same-shard (incl. same-key) runs as one atomic task;
// cross-shard validates both keys first, then applies the pop and the
// push in a second fan-out (§4.2 fast-path: per-shard atomicity).
func lmoveCmd(e *Engine, cs *ConnState, srcB, dstB []byte, srcHead, dstHead bool) resp.Value {
	src, dst := string(srcB), string(dstB)
	keys := [][]byte{srcB, dstB}
	if src == dst {
		var reply resp.Value
		e.do(cs, srcB, func(s *shard.Shard) {
			ent, wt := getColl(s, cs.DB, src, shard.TypeList)
			switch {
			case wt:
				reply = errWrongType
			case ent == nil:
				reply = resp.Null()
			default:
				l := ent.Obj.(*types.List)
				v, _ := popOne(l, srcHead)
				if dstHead {
					l.PushHead(v)
				} else {
					l.PushTail(v)
				}
				s.Touch(ent)
				s.WakeWaiter(cs.DB, src) // dst == src: parked waiters can proceed
				reply = resp.BlobString(v)
			}
		})
		return reply
	}
	// Phase 1: validate and peek. Redis ordering: src WRONGTYPE, then a
	// missing src answers null without consulting dst, then dst
	// WRONGTYPE. When the fan-out is concurrent, src (i==0) and dst
	// (i==1) are in different shard groups, so the closure writes below
	// touch disjoint variables and need no synchronization.
	var val []byte
	var srcWT, dstWT bool
	missing := false
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if i == 0 {
				ent, wt := getColl(s, cs.DB, src, shard.TypeList)
				switch {
				case wt:
					srcWT = true
				case ent == nil:
					missing = true
				default:
					l := ent.Obj.(*types.List)
					if srcHead {
						val, _ = l.PeekHead()
					} else {
						val, _ = l.PeekTail()
					}
				}
			} else {
				_, wt := getColl(s, cs.DB, dst, shard.TypeList)
				if wt {
					dstWT = true
				}
			}
		}
	})
	if srcWT {
		return errWrongType
	}
	if missing {
		return resp.Null()
	}
	if dstWT {
		return errWrongType
	}
	// Phase 2: apply.
	e.doMulti(cs, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if i == 0 {
				ent, ok := s.Lookup(cs.DB, src)
				if !ok || ent.Type != shard.TypeList {
					continue
				}
				l := ent.Obj.(*types.List)
				popOne(l, srcHead)
				s.Touch(ent)
				if l.Len() == 0 {
					s.Delete(cs.DB, src)
				} else {
					// Serve the next parked waiter while elements remain.
					s.WakeWaiter(cs.DB, src)
				}
			} else {
				ent, _ := getColl(s, cs.DB, dst, shard.TypeList)
				var l *types.List
				if ent == nil {
					l = types.NewList()
					storeColl(s, cs.DB, dst, shard.TypeList, l)
				} else {
					l = ent.Obj.(*types.List)
				}
				if dstHead {
					l.PushHead(dupBytes(val))
				} else {
					l.PushTail(dupBytes(val))
				}
				if ent != nil {
					s.Touch(ent)
				}
				s.WakeWaiter(cs.DB, dst) // a BLMOVE/BLPOP on dst can proceed
			}
		}
	})
	return resp.BlobString(val)
}

func cmdLPos(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	// LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
	rank := int64(1)
	count := int64(-1) // -1: single-result mode
	maxlen := int64(0)
	for i := 3; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "rank":
			if i+1 >= len(args) {
				return errSyntax
			}
			v, ok := parseIntStrict(args[i+1])
			if !ok {
				return errNotInt
			}
			if v == 0 {
				return resp.Err("ERR RANK can't be zero: use 1 to start from the first match, " +
					"2 from the second ... or use negative to start from the end of the list")
			}
			rank = v
			i++
		case "count":
			if i+1 >= len(args) {
				return errSyntax
			}
			v, ok := parseIntStrict(args[i+1])
			if !ok || v < 0 {
				return resp.Err("ERR COUNT can't be negative")
			}
			count = v
			i++
		case "maxlen":
			if i+1 >= len(args) {
				return errSyntax
			}
			v, ok := parseIntStrict(args[i+1])
			if !ok || v < 0 {
				return resp.Err("ERR MAXLEN can't be negative")
			}
			maxlen = v
			i++
		default:
			return errSyntax
		}
	}
	key := string(args[1])
	elem := args[2]
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeList)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			if count >= 0 {
				reply = resp.Arr()
			} else {
				reply = resp.Null()
			}
		default:
			l := ent.Obj.(*types.List)
			var snap [][]byte
			for _, v := range l.All() {
				snap = append(snap, v)
			}
			n := len(snap)
			var matches []int64
			skip := rank
			if rank < 0 {
				skip = -rank
			}
			examined := int64(0)
			done := func() bool {
				if maxlen > 0 && examined >= maxlen {
					return true
				}
				if count > 0 && int64(len(matches)) >= count {
					return true // COUNT 0 means unlimited
				}
				return count < 0 && len(matches) > 0
			}
			step := func(i int) {
				examined++
				if !bytes.Equal(snap[i], elem) {
					return
				}
				if skip > 1 {
					skip--
					return
				}
				matches = append(matches, int64(i))
			}
			if rank > 0 {
				for i := 0; i < n && !done(); i++ {
					step(i)
				}
			} else {
				for i := n - 1; i >= 0 && !done(); i-- {
					step(i)
				}
			}
			if count >= 0 {
				out := make([]resp.Value, 0, len(matches))
				for _, m := range matches {
					out = append(out, resp.Int(m))
				}
				reply = resp.Arr(out...)
			} else if len(matches) > 0 {
				reply = resp.Int(matches[0])
			} else {
				reply = resp.Null()
			}
		}
	})
	return reply
}
