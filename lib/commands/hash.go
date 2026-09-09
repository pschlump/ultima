package commands

import (
	"math"
	"math/rand/v2"
	"strconv"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// Hash commands (design doc §7 P1). Hashes are types.Hash values in
// Entry.Obj; all mutation happens inside the owning shard goroutine, and
// a hash whose last field is removed ceases to exist as a key.

func cmdHSet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args)%2 != 0 {
		return errArity("hset")
	}
	key := string(args[1])
	var reply resp.Value
	var done bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		var h *types.Hash
		if ent == nil {
			h = types.NewHash()
			storeColl(s, cs.DB, key, shard.TypeHash, h)
		} else {
			h = ent.Obj.(*types.Hash)
		}
		var added int64
		for i := 2; i+1 < len(args); i += 2 {
			if h.Set(string(args[i]), string(args[i+1])) {
				added++
			}
		}
		if ent != nil {
			s.Touch(cs.DB, key, ent) // field set/overwrite mutates the live hash
		}
		done = true // Redis emits hset even when no field was new
		reply = resp.Int(added)
	})
	if done {
		e.notifyKeyspace(cs.DB, key, "hset")
	}
	return reply
}

func cmdHMSet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args)%2 != 0 {
		return errArity("hmset")
	}
	if v := cmdHSet(e, cs, args); v.Kind == resp.KindError {
		return v
	}
	return replyOK
}

func cmdHGet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key, field := string(args[1]), string(args[2])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Null()
		default:
			if v, ok := ent.Obj.(*types.Hash).Get(field); ok {
				reply = resp.BlobStr(v)
			} else {
				reply = resp.Null()
			}
		}
	})
	return reply
}

func cmdHMGet(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	vals := make([]resp.Value, len(args)-2)
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		if ent == nil {
			for i := range vals {
				vals[i] = resp.Null()
			}
			return
		}
		h := ent.Obj.(*types.Hash)
		for i, f := range args[2:] {
			if v, ok := h.Get(string(f)); ok {
				vals[i] = resp.BlobStr(v)
			} else {
				vals[i] = resp.Null()
			}
		}
	})
	if reply.Kind == resp.KindError {
		return reply
	}
	return resp.Arr(vals...)
}

func cmdHGetAll(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		var pairs []resp.Value
		if ent != nil {
			h := ent.Obj.(*types.Hash)
			pairs = make([]resp.Value, 0, h.Len()*2)
			h.Each(func(f, v string) {
				pairs = append(pairs, resp.BlobStr(f), resp.BlobStr(v))
			})
		}
		reply = resp.Map(pairs...)
	})
	return reply
}

func cmdHDel(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	var n int64
	var deleted bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			h := ent.Obj.(*types.Hash)
			for _, f := range args[2:] {
				if h.Del(string(f)) {
					n++
				}
			}
			if n > 0 {
				s.Touch(cs.DB, key, ent)
			}
			if h.Len() == 0 {
				s.Delete(cs.DB, key)
				deleted = true
			}
			reply = resp.Int(n)
		}
	})
	if n > 0 {
		e.notifyKeyspace(cs.DB, key, "hdel")
		if deleted {
			e.notifyKeyspace(cs.DB, key, "del")
		}
	}
	return reply
}

func cmdHExists(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key, field := string(args[1]), string(args[2])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			_, ok := ent.Obj.(*types.Hash).Get(field)
			reply = resp.Int(boolInt(ok))
		}
	})
	return reply
}

func cmdHLen(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			reply = resp.Int(int64(ent.Obj.(*types.Hash).Len()))
		}
	})
	return reply
}

func cmdHKeys(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return hKeysVals(e, cs, args, false)
}

func cmdHVals(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return hKeysVals(e, cs, args, true)
}

func hKeysVals(e *Engine, cs *ConnState, args [][]byte, vals bool) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		out := []resp.Value{}
		if ent != nil {
			ent.Obj.(*types.Hash).Each(func(f, v string) {
				if vals {
					out = append(out, resp.BlobStr(v))
				} else {
					out = append(out, resp.BlobStr(f))
				}
			})
		}
		reply = resp.Arr(out...)
	})
	return reply
}

func cmdHStrLen(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key, field := string(args[1]), string(args[2])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		switch {
		case wt:
			reply = errWrongType
		case ent == nil:
			reply = resp.Int(0)
		default:
			if v, ok := ent.Obj.(*types.Hash).Get(field); ok {
				reply = resp.Int(int64(len(v)))
			} else {
				reply = resp.Int(0)
			}
		}
	})
	return reply
}

func cmdHSetNX(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key, field, val := string(args[1]), string(args[2]), string(args[3])
	var reply resp.Value
	var set bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		var h *types.Hash
		if ent == nil {
			h = types.NewHash()
		} else {
			h = ent.Obj.(*types.Hash)
			if _, ok := h.Get(field); ok {
				reply = resp.Int(0)
				return
			}
		}
		h.Set(field, val)
		if ent == nil {
			storeColl(s, cs.DB, key, shard.TypeHash, h)
		} else {
			s.Touch(cs.DB, key, ent)
		}
		set = true
		reply = resp.Int(1)
	})
	if set {
		e.notifyKeyspace(cs.DB, key, "hset")
	}
	return reply
}

func cmdHIncrBy(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	delta, ok := parseIntStrict(args[3])
	if !ok {
		return errNotInt
	}
	key, field := string(args[1]), string(args[2])
	var reply resp.Value
	var done bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		var h *types.Hash
		var cur int64
		if ent != nil {
			h = ent.Obj.(*types.Hash)
			if vs, ok := h.Get(field); ok {
				v, ok := parseIntStrict([]byte(vs))
				if !ok {
					reply = resp.Err("ERR hash value is not an integer")
					return
				}
				cur = v
			}
		} else {
			h = types.NewHash()
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			reply = errIncrOvf
			return
		}
		cur += delta
		h.Set(field, intToStr(cur))
		if ent == nil {
			storeColl(s, cs.DB, key, shard.TypeHash, h)
		} else {
			s.Touch(cs.DB, key, ent)
		}
		done = true
		reply = resp.Int(cur)
	})
	if done {
		e.notifyKeyspace(cs.DB, key, "hincrby")
	}
	return reply
}

func cmdHIncrByFloat(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	delta, ok := parseFloat(args[3])
	if !ok {
		return errBadFloat
	}
	if math.IsNaN(delta) || math.IsInf(delta, 0) {
		return resp.Err("ERR value is NaN or Infinity")
	}
	key, field := string(args[1]), string(args[2])
	var reply resp.Value
	var done bool
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		var h *types.Hash
		var cur float64
		if ent != nil {
			h = ent.Obj.(*types.Hash)
			if vs, ok := h.Get(field); ok {
				v, ok := parseFloat([]byte(vs))
				if !ok {
					reply = resp.Err("ERR hash value is not a float")
					return
				}
				cur = v
			}
		} else {
			h = types.NewHash()
		}
		cur += delta
		if math.IsNaN(cur) || math.IsInf(cur, 0) {
			reply = resp.Err("ERR increment would produce NaN or Infinity")
			return
		}
		out := formatHumanFloat(cur)
		h.Set(field, out)
		if ent == nil {
			storeColl(s, cs.DB, key, shard.TypeHash, h)
		} else {
			s.Touch(cs.DB, key, ent)
		}
		done = true
		reply = resp.BlobStr(out)
	})
	if done {
		e.notifyKeyspace(cs.DB, key, "hincrbyfloat")
	}
	return reply
}

func cmdHRandField(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	withValues := false
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
		if lowerASCII(args[3]) != "withvalues" {
			return errSyntax
		}
		withValues = true
	}
	if len(args) > 4 {
		return errSyntax
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
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
		h := ent.Obj.(*types.Hash)
		n := h.Len()
		if !hasCount {
			i := rand.IntN(n)
			reply = resp.BlobStr(h.Fields()[i])
			return
		}
		var fields []string
		if count >= 0 {
			k := min(int(count), n)
			fields = make([]string, 0, k)
			all := h.Fields()
			if int(count) >= n {
				// count >= size: every field in encoding (insertion)
				// order, unshuffled — the Redis 7.2 fast path.
				fields = append(fields, all...)
			} else {
				for _, i := range rand.Perm(n)[:k] {
					fields = append(fields, all[i])
				}
			}
		} else {
			fields = make([]string, 0, -count)
			all := h.Fields()
			for i := int64(0); i < -count; i++ {
				fields = append(fields, all[rand.IntN(n)])
			}
		}
		if !withValues {
			out := make([]resp.Value, 0, len(fields))
			for _, f := range fields {
				out = append(out, resp.BlobStr(f))
			}
			reply = resp.Arr(out...)
			return
		}
		out := make([]resp.Value, 0, len(fields))
		for _, f := range fields {
			v, _ := h.Get(f)
			if cs.Proto == 3 {
				out = append(out, resp.Arr(resp.BlobStr(f), resp.BlobStr(v)))
			} else {
				out = append(out, resp.BlobStr(f), resp.BlobStr(v))
			}
		}
		reply = resp.Arr(out...)
	})
	return reply
}

func cmdHScan(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	cursor, ok := parseCursor(args[2])
	if !ok {
		return resp.Err("ERR invalid cursor")
	}
	key := string(args[1])
	var reply resp.Value
	e.do(cs, args[1], func(s *shard.Shard) {
		ent, wt := getColl(s, cs.DB, key, shard.TypeHash)
		if wt {
			reply = errWrongType
			return
		}
		// Redis validates MATCH/COUNT only when the key exists.
		if ent == nil {
			reply = resp.Arr(resp.BlobStr("0"), resp.Arr())
			return
		}
		match, count, errV, failed := scanOpts(args, 3)
		if failed {
			reply = errV
			return
		}
		type pair struct{ f, v string }
		var items []pair
		ent.Obj.(*types.Hash).Each(func(f, v string) {
			items = append(items, pair{f, v})
		})
		kept, next := scanWindow(items, cursor, count, func(p pair) bool {
			return match == nil || GlobMatch(match, []byte(p.f))
		})
		out := make([]resp.Value, 0, len(kept)*2)
		for _, p := range kept {
			out = append(out, resp.BlobStr(p.f), resp.BlobStr(p.v))
		}
		reply = resp.Arr(resp.BlobStr(next), resp.Arr(out...))
	})
	return reply
}

// boolInt maps a bool to a Redis 1/0 int reply value.
func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// intToStr formats an int64 for storage (decimal).
func intToStr(v int64) string {
	return strconv.FormatInt(v, 10)
}
