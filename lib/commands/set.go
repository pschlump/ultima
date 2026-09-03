package commands

import (
	"math/rand/v2"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// Set commands (design doc §7 P1). Sets are *types.Set in Entry.Obj
// (intset-equivalent small encoding); an emptied set is deleted as a key.

func cmdSAdd(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		if wt {
			reply = errWrongType
			return
		}
		var st *types.Set
		if ent == nil {
			st = types.NewSet()
			storeColl(s, cs.DB, key, shard.TypeSet, st)
		} else {
			st = ent.Obj.(*types.Set)
		}
		var n int64
		for _, m := range args[2:] {
			if st.Add(string(m)) {
				n++
			}
		}
		reply = resp.Int(n)
	})
	return reply
}

func cmdSRem(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			st := ent.Obj.(*types.Set)
			var n int64
			for _, m := range args[2:] {
				if st.Remove(string(m)) {
					n++
				}
			}
			if st.Len() == 0 {
				s.Delete(cs.DB, key)
			}
			reply = resp.Int(n)
		}
	})
	return reply
}

func cmdSMembers(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		if wt {
			reply = errWrongType
			return
		}
		out := []resp.Value{}
		if ent != nil {
			for _, m := range ent.Obj.(*types.Set).Members() {
				out = append(out, resp.BlobStr(m))
			}
		}
		reply = resp.Set(out...)
	})
	return reply
}

func cmdSIsMember(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key, member := string(args[1]), string(args[2])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			reply = resp.Int(boolInt(ent.Obj.(*types.Set).Contains(member)))
		}
	})
	return reply
}

func cmdSMIsMember(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	out := make([]resp.Value, len(args)-2)
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		if wt {
			reply = errWrongType
			return
		}
		for i, m := range args[2:] {
			ok := false
			if ent != nil {
				ok = ent.Obj.(*types.Set).Contains(string(m))
			}
			out[i] = resp.Int(boolInt(ok))
		}
	})
	if reply.Kind == resp.KindError {
		return reply
	}
	return resp.Arr(out...)
}

func cmdSCard(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			reply = resp.Int(int64(ent.Obj.(*types.Set).Len()))
		}
	})
	return reply
}

func cmdSPop(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args) > 3 {
		return errArity("spop")
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
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			if hasCount {
				reply = resp.Set()
			} else {
				reply = resp.Null()
			}
			return
		}
		st := ent.Obj.(*types.Set)
		if !hasCount {
			m := st.Members()[rand.IntN(st.Len())]
			st.Remove(m)
			if st.Len() == 0 {
				s.Delete(cs.DB, key)
			}
			reply = resp.BlobStr(m)
			return
		}
		out := []resp.Value{}
		if count > 0 {
			members := st.Members()
			if int(count) >= len(members) {
				out = make([]resp.Value, 0, len(members))
				for _, m := range members {
					out = append(out, resp.BlobStr(m))
				}
				s.Delete(cs.DB, key)
			} else {
				out = make([]resp.Value, 0, count)
				for _, i := range rand.Perm(len(members))[:count] {
					m := members[i]
					st.Remove(m)
					out = append(out, resp.BlobStr(m))
				}
			}
		}
		reply = resp.Set(out...)
	})
	return reply
}

func cmdSRandMember(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args) > 3 {
		return errSyntax
	}
	hasCount := len(args) == 3
	var count int64
	if hasCount {
		v, ok := parseIntStrict(args[2])
		if !ok {
			return errNotInt
		}
		count = v
	}
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
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
		members := ent.Obj.(*types.Set).Members()
		n := len(members)
		if !hasCount {
			reply = resp.BlobStr(members[rand.IntN(n)])
			return
		}
		var out []resp.Value
		if count >= 0 {
			// count >= size returns the whole set in encoding order,
			// unshuffled (Redis 7.2 fast path).
			k := min(int(count), n)
			out = make([]resp.Value, 0, k)
			if int(count) >= n {
				for _, m := range members {
					out = append(out, resp.BlobStr(m))
				}
			} else {
				for _, i := range rand.Perm(n)[:k] {
					out = append(out, resp.BlobStr(members[i]))
				}
			}
		} else {
			out = make([]resp.Value, 0, -count)
			for i := int64(0); i < -count; i++ {
				out = append(out, resp.BlobStr(members[rand.IntN(n)]))
			}
		}
		reply = resp.Arr(out...)
	})
	return reply
}

func cmdSMove(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	src, dst, member := string(args[1]), string(args[2]), string(args[3])
	if src == dst {
		var reply resp.Value
		e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
			ent, wt := getColl(s, cs.DB, src, shard.TypeSet)
			switch {
			case wt:
				reply = errWrongType
			case ent == nil:
				reply = resp.Int(0)
			default:
				reply = resp.Int(boolInt(ent.Obj.(*types.Set).Contains(member)))
			}
		})
		return reply
	}
	// Two-phase cross-shard move (§4.2 fast path): validate both keys,
	// then remove from src and add to dst in a second fan-out. Redis
	// ordering: src WRONGTYPE first, then a missing src (or absent
	// member) answers 0 without consulting dst, then dst WRONGTYPE.
	keys := [][]byte{args[1], args[2]}
	var srcWT, dstWT bool
	found := false
	e.Shards.DoMulti(cs.DB, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if i == 0 {
				ent, wt := getColl(s, cs.DB, src, shard.TypeSet)
				switch {
				case wt:
					srcWT = true
				case ent != nil:
					found = ent.Obj.(*types.Set).Contains(member)
				}
			} else {
				_, wt := getColl(s, cs.DB, dst, shard.TypeSet)
				if wt {
					dstWT = true
				}
			}
		}
	})
	if srcWT {
		return errWrongType
	}
	if !found {
		return resp.Int(0)
	}
	if dstWT {
		return errWrongType
	}
	e.Shards.DoMulti(cs.DB, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if i == 0 {
				ent, ok := s.Lookup(cs.DB, src)
				if !ok || ent.Type != shard.TypeSet {
					continue
				}
				st := ent.Obj.(*types.Set)
				st.Remove(member)
				if st.Len() == 0 {
					s.Delete(cs.DB, src)
				}
			} else {
				ent, _ := getColl(s, cs.DB, dst, shard.TypeSet)
				var st *types.Set
				if ent == nil {
					st = types.NewSet()
					storeColl(s, cs.DB, dst, shard.TypeSet, st)
				} else {
					st = ent.Obj.(*types.Set)
				}
				st.Add(member)
			}
		}
	})
	return resp.Int(1)
}

// --- set algebra (multi-key, §4.2 fan-out) ------------------------------------

// setSnapshot is a read-only view of one key's set content for algebra.
type setSnapshot struct {
	members []string
	present bool // key existed
}

// snapshotSets reads every key's set members via DoMulti, or returns a
// WRONGTYPE error value when any live key is not a set.
func snapshotSets(e *Engine, cs *ConnState, keys [][]byte) ([]setSnapshot, resp.Value, bool) {
	snaps := make([]setSnapshot, len(keys))
	var errV resp.Value
	e.Shards.DoMulti(cs.DB, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			ent, wt := getColl(s, cs.DB, string(keys[i]), shard.TypeSet)
			if wt {
				errV = errWrongType
				continue
			}
			if ent != nil {
				snaps[i] = setSnapshot{
					members: ent.Obj.(*types.Set).Members(),
					present: true,
				}
			}
		}
	})
	if errV.Kind == resp.KindError {
		return nil, errV, true
	}
	return snaps, resp.Value{}, false
}

// intersect builds the intersection; a missing key empties the result.
func intersect(snaps []setSnapshot) []string {
	for _, sn := range snaps {
		if !sn.present {
			return nil
		}
	}
	if len(snaps) == 0 {
		return nil
	}
	// Iterate the smallest input, as Redis does.
	base := 0
	for i := range snaps {
		if len(snaps[i].members) < len(snaps[base].members) {
			base = i
		}
	}
	others := make([]map[string]bool, 0, len(snaps)-1)
	for i, sn := range snaps {
		if i == base {
			continue
		}
		m := make(map[string]bool, len(sn.members))
		for _, mem := range sn.members {
			m[mem] = true
		}
		others = append(others, m)
	}
	result := types.NewSet()
	for _, mem := range snaps[base].members {
		inAll := true
		for _, o := range others {
			if !o[mem] {
				inAll = false
				break
			}
		}
		if inAll {
			result.Add(mem)
		}
	}
	return result.Members()
}

func unionAll(snaps []setSnapshot) []string {
	result := types.NewSet()
	for _, sn := range snaps {
		for _, mem := range sn.members {
			result.Add(mem)
		}
	}
	return result.Members()
}

func diffAll(snaps []setSnapshot) []string {
	if len(snaps) == 0 || !snaps[0].present {
		return nil
	}
	rest := map[string]bool{}
	for _, sn := range snaps[1:] {
		for _, mem := range sn.members {
			rest[mem] = true
		}
	}
	result := types.NewSet()
	for _, mem := range snaps[0].members {
		if !rest[mem] {
			result.Add(mem)
		}
	}
	return result.Members()
}

func setReply(members []string) resp.Value {
	out := make([]resp.Value, 0, len(members))
	for _, m := range members {
		out = append(out, resp.BlobStr(m))
	}
	return resp.Set(out...)
}

func cmdSInter(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	snaps, errV, failed := snapshotSets(e, cs, args[1:])
	if failed {
		return errV
	}
	return setReply(intersect(snaps))
}

func cmdSUnion(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	snaps, errV, failed := snapshotSets(e, cs, args[1:])
	if failed {
		return errV
	}
	return setReply(unionAll(snaps))
}

func cmdSDiff(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	snaps, errV, failed := snapshotSets(e, cs, args[1:])
	if failed {
		return errV
	}
	return setReply(diffAll(snaps))
}

// storeSetResult writes the algebra result to dest: an empty result
// deletes dest (Redis semantics), a non-empty one replaces it wholesale
// (dropping any prior value and TTL).
func storeSetResult(e *Engine, cs *ConnState, dest []byte, members []string) resp.Value {
	var n int64
	e.Shards.Do(cs.DB, dest, func(s *shard.Shard) {
		if len(members) == 0 {
			s.Delete(cs.DB, string(dest))
			n = 0
			return
		}
		st := types.NewSet()
		for _, m := range members {
			st.Add(m)
		}
		storeColl(s, cs.DB, string(dest), shard.TypeSet, st)
		n = int64(len(members))
	})
	return resp.Int(n)
}

func cmdSInterStore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	snaps, errV, failed := snapshotSets(e, cs, args[2:])
	if failed {
		return errV
	}
	return storeSetResult(e, cs, args[1], intersect(snaps))
}

func cmdSUnionStore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	snaps, errV, failed := snapshotSets(e, cs, args[2:])
	if failed {
		return errV
	}
	return storeSetResult(e, cs, args[1], unionAll(snaps))
}

func cmdSDiffStore(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	snaps, errV, failed := snapshotSets(e, cs, args[2:])
	if failed {
		return errV
	}
	return storeSetResult(e, cs, args[1], diffAll(snaps))
}

func cmdSInterCard(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	numkeys, ok := parseIntStrict(args[1])
	if !ok || numkeys <= 0 {
		return resp.Err("ERR numkeys should be greater than 0")
	}
	if int(numkeys) > len(args)-2 {
		return resp.Err("ERR Number of keys can't be greater than number of args")
	}
	keys := args[2 : 2+numkeys]
	limit := int64(0)
	rest := args[2+numkeys:]
	if len(rest) > 0 {
		if len(rest) != 2 || lowerASCII(rest[0]) != "limit" {
			return errSyntax
		}
		v, ok := parseIntStrict(rest[1])
		if !ok || v < 0 {
			return resp.Err("ERR LIMIT can't be negative")
		}
		limit = v
	}
	snaps, errV, failed := snapshotSets(e, cs, keys)
	if failed {
		return errV
	}
	inter := intersect(snaps)
	n := int64(len(inter))
	if limit > 0 && n > limit {
		n = limit
	}
	return resp.Int(n)
}

func cmdSScan(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	cursor, ok := parseCursor(args[2])
	if !ok {
		return resp.Err("ERR invalid cursor")
	}
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeSet)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			reply = resp.Arr(resp.BlobStr("0"), resp.Arr())
			return
		}
		match, count, errV, failed := scanOpts(args, 3)
		if failed {
			reply = errV
			return
		}
		items := ent.Obj.(*types.Set).Members()
		kept, next := scanWindow(items, cursor, count, func(m string) bool {
			return match == nil || GlobMatch(match, []byte(m))
		})
		out := make([]resp.Value, 0, len(kept))
		for _, m := range kept {
			out = append(out, resp.BlobStr(m))
		}
		reply = resp.Arr(resp.BlobStr(next), resp.Arr(out...))
	})
	return reply
}
