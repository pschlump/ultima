package commands

import (
	"fmt"
	"math"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// --- DEL / EXISTS (§4.2 fan-out) ---------------------------------------------

func cmdDel(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	keys := args[1:]
	var n int64
	e.Shards.DoMulti(cs.DB, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if s.Delete(cs.DB, string(keys[i])) {
				n++
			}
		}
	})
	return resp.Int(n)
}

func cmdExists(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	keys := args[1:]
	var n int64
	e.Shards.DoMulti(cs.DB, keys, func(s *shard.Shard, idxs []int) {
		for _, i := range idxs {
			if _, ok := s.Lookup(cs.DB, string(keys[i])); ok {
				n++ // duplicates count, as in Redis >= 3.0.3
			}
		}
	})
	return resp.Int(n)
}

// --- EXPIRE family -------------------------------------------------------------

// expireFlags are EXPIRE/PEXPIRE's conditional options (Redis 7.0+).
type expireFlags struct {
	nx, xx, gt, lt bool
}

// parseExpireFlags parses the option tail; arg errors come before the
// missing-key check, as in Redis.
func parseExpireFlags(args [][]byte) (expireFlags, resp.Value, bool) {
	var f expireFlags
	fail := func(v resp.Value) (expireFlags, resp.Value, bool) { return f, v, true }
	for i := 3; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "nx":
			if f.xx || f.gt || f.lt {
				return fail(resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible"))
			}
			f.nx = true
		case "xx":
			if f.nx || f.gt || f.lt {
				return fail(resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible"))
			}
			f.xx = true
		case "gt":
			if f.nx || f.xx {
				return fail(resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible"))
			}
			if f.lt {
				return fail(resp.Err("ERR GT and LT options at the same time are not compatible"))
			}
			f.gt = true
		case "lt":
			if f.nx || f.xx {
				return fail(resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible"))
			}
			if f.gt {
				return fail(resp.Err("ERR GT and LT options at the same time are not compatible"))
			}
			f.lt = true
		default:
			return fail(resp.Err(fmt.Sprintf("ERR Unsupported option %s", string(args[i]))))
		}
	}
	return f, resp.Value{}, false
}

func expireCommon(e *Engine, cs *ConnState, args [][]byte, ms bool) resp.Value {
	v, ok := parseIntStrict(args[2])
	if !ok {
		return errNotInt
	}
	f, errV, failed := parseExpireFlags(args)
	if failed {
		return errV
	}
	now := e.Shards.NowMs()
	var at int64
	if ms {
		at = now + v
	} else {
		if v > (math.MaxInt64-now)/1000 {
			at = math.MaxInt64
		} else {
			at = now + v*1000
		}
	}
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, found := s.Lookup(cs.DB, key)
		if !found {
			reply = resp.Int(0)
			return
		}
		cur := ent.ExpireAtMs // 0 = no TTL, treated as infinite for GT/LT
		blocked := false
		switch {
		case f.nx && cur != 0:
			blocked = true
		case f.xx && cur == 0:
			blocked = true
		case f.gt && (cur == 0 || at <= cur):
			blocked = true // GT: new expiry must be greater than current
		case f.lt && cur != 0 && at >= cur:
			blocked = true // LT: new expiry must be less than current
		}
		if blocked {
			reply = resp.Int(0)
			return
		}
		ent.ExpireAtMs = at
		if at <= now {
			s.Delete(cs.DB, key) // expiry in the past deletes the key
		} else {
			s.PushExpire(cs.DB, key, ent)
		}
		reply = resp.Int(1)
	})
	return reply
}

func cmdExpire(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return expireCommon(e, cs, args, false)
}

func cmdPExpire(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return expireCommon(e, cs, args, true)
}

func ttlCommon(e *Engine, cs *ConnState, args [][]byte, ms bool) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, found := s.Lookup(cs.DB, key)
		if !found {
			reply = resp.Int(-2)
			return
		}
		if ent.ExpireAtMs == 0 {
			reply = resp.Int(-1)
			return
		}
		rem := ent.ExpireAtMs - e.Shards.NowMs()
		if rem < 0 {
			rem = 0
		}
		if ms {
			reply = resp.Int(rem)
		} else {
			reply = resp.Int((rem + 500) / 1000) // Redis rounds to nearest
		}
	})
	return reply
}

func cmdTTL(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return ttlCommon(e, cs, args, false)
}

func cmdPTTL(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return ttlCommon(e, cs, args, true)
}

func cmdPersist(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		ent, found := s.Lookup(cs.DB, key)
		if !found || ent.ExpireAtMs == 0 {
			reply = resp.Int(0)
			return
		}
		ent.ExpireAtMs = 0
		reply = resp.Int(1)
	})
	return reply
}

// --- TYPE / SCAN --------------------------------------------------------------

func cmdType(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	key := string(args[1])
	var reply resp.Value
	e.Shards.Do(cs.DB, args[1], func(s *shard.Shard) {
		if ent, found := s.Lookup(cs.DB, key); found {
			reply = resp.Simple(typeName(ent.Type))
		} else {
			reply = resp.Simple("none")
		}
	})
	return reply
}

// typeName is TYPE's reply for each entry type.
func typeName(t shard.Type) string {
	switch t {
	case shard.TypeHash:
		return "hash"
	case shard.TypeList:
		return "list"
	case shard.TypeSet:
		return "set"
	case shard.TypeZSet:
		return "zset"
	default:
		return "string"
	}
}

func cmdScan(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	cursor, ok := parseCursor(args[1])
	if !ok {
		return resp.Err("ERR invalid cursor")
	}
	match := []byte(nil)
	count := 10
	for i := 2; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "match":
			if i+1 >= len(args) {
				return errSyntax
			}
			match = args[i+1]
			i++
		case "count":
			if i+1 >= len(args) {
				return errSyntax
			}
			v, ok := parseIntStrict(args[i+1])
			if !ok {
				return errNotInt
			}
			if v <= 0 {
				return errSyntax // Redis: non-positive COUNT is a syntax error
			}
			count = int(v)
			i++
		default:
			return errSyntax
		}
	}
	keys, next := e.Shards.Scan(cs.DB, cursor, count)
	if match != nil {
		filtered := keys[:0]
		for _, k := range keys {
			if GlobMatch(match, []byte(k)) {
				filtered = append(filtered, k)
			}
		}
		keys = filtered
	}
	elems := make([]resp.Value, 0, len(keys))
	for _, k := range keys {
		elems = append(elems, resp.BlobStr(k))
	}
	nextStr := uintToStr(next)
	return resp.Arr(resp.BlobStr(nextStr), resp.Arr(elems...))
}

func uintToStr(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
