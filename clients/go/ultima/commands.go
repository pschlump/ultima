// Package ultima typed hot-command builders (design doc §6.2, D15). The
// gRPC and
// WebSocket clients share these: each typed helper on GRPCClient and
// WSClient is a thin wrapper that builds the protobuf Command here and
// runs it through that surface's Exec. The wire semantics are exactly
// Redis's — lib/envelope.ArgsFor renders these into the same engine argv
// the RESP parser would produce.
package ultima

import (
	"time"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
)

// SetOptions carries the SET modifiers (proto SetCommand): NX/XX, the GET
// old-value reply, and a millisecond TTL (the typed form maps to PX —
// second-granularity EXPIRE stays reachable via ExecGeneric).
type SetOptions struct {
	NX  bool
	XX  bool
	Get bool
	PX  time.Duration // <= 0: no expiry
}

// ZAddOptions carries the ZADD modifiers (proto ZAddCommand).
type ZAddOptions struct {
	NX   bool
	XX   bool
	GT   bool
	LT   bool
	CH   bool
	INCR bool
}

// ZMember is one (score, member) pair for ZAdd.
type ZMember struct {
	Score  float64
	Member string
}

func b(s string) []byte { return []byte(s) }

func cmdGet(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: b(key)}}}
}

func cmdSet(key, value string, o SetOptions) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
		Key:   b(key),
		Value: b(value),
		TtlMs: o.PX.Milliseconds(),
		Nx:    o.NX,
		Xx:    o.XX,
		Get:   o.Get,
	}}}
}

func cmdDel(keys []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Del{Del: &ultimav1.DelCommand{Keys: bb(keys)}}}
}

// IncrCommand carries INCR/DECR/INCRBY/DECRBY: the signed step is the
// delta (proto command.proto).
func cmdIncr(key string, delta int64) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Incr{Incr: &ultimav1.IncrCommand{Key: b(key), Delta: delta}}}
}

func cmdIncrFloat(key string, delta float64) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_IncrFloat{IncrFloat: &ultimav1.IncrFloatCommand{Key: b(key), Delta: delta}}}
}

func cmdMGet(keys []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Mget{Mget: &ultimav1.MGetCommand{Keys: bb(keys)}}}
}

func cmdMSet(kv []string) *ultimav1.Command {
	pairs := make([]*ultimav1.MSetPair, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		pairs = append(pairs, &ultimav1.MSetPair{Key: b(kv[i]), Value: b(kv[i+1])})
	}
	return &ultimav1.Command{Cmd: &ultimav1.Command_Mset{Mset: &ultimav1.MSetCommand{Pairs: pairs}}}
}

func cmdAppend(key, value string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Append{Append: &ultimav1.AppendCommand{Key: b(key), Value: b(value)}}}
}

func cmdExists(keys []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Exists{Exists: &ultimav1.ExistsCommand{Keys: bb(keys)}}}
}

// The typed expiry command carries milliseconds and maps to PEXPIRE
// (proto command.proto: second-granularity EXPIRE would be lossy).
func cmdExpire(key string, ttl time.Duration) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Expire{Expire: &ultimav1.ExpireCommand{Key: b(key), TtlMs: ttl.Milliseconds()}}}
}

// The typed TTL command answers in milliseconds (PTTL semantics).
func cmdPTTL(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Ttl{Ttl: &ultimav1.TtlCommand{Key: b(key)}}}
}

func cmdPersist(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Persist{Persist: &ultimav1.PersistCommand{Key: b(key)}}}
}

func cmdHGet(key, field string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Hget{Hget: &ultimav1.HGetCommand{Key: b(key), Field: b(field)}}}
}

func cmdHSet(key string, fv []string) *ultimav1.Command {
	pairs := make([]*ultimav1.FieldValue, 0, len(fv)/2)
	for i := 0; i+1 < len(fv); i += 2 {
		pairs = append(pairs, &ultimav1.FieldValue{Field: b(fv[i]), Value: b(fv[i+1])})
	}
	return &ultimav1.Command{Cmd: &ultimav1.Command_Hset{Hset: &ultimav1.HSetCommand{Key: b(key), Pairs: pairs}}}
}

func cmdHGetAll(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Hgetall{Hgetall: &ultimav1.HGetAllCommand{Key: b(key)}}}
}

func cmdHDel(key string, fields []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Hdel{Hdel: &ultimav1.HDelCommand{Key: b(key), Fields: bb(fields)}}}
}

func cmdHIncrBy(key, field string, delta int64) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Hincrby{Hincrby: &ultimav1.HIncrByCommand{Key: b(key), Field: b(field), Delta: delta}}}
}

func cmdLPush(key string, elems []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Lpush{Lpush: &ultimav1.LPushCommand{Key: b(key), Elems: bb(elems)}}}
}

func cmdRPush(key string, elems []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Rpush{Rpush: &ultimav1.RPushCommand{Key: b(key), Elems: bb(elems)}}}
}

func cmdLPop(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Lpop{Lpop: &ultimav1.LPopCommand{Key: b(key)}}}
}

func cmdRPop(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Rpop{Rpop: &ultimav1.RPopCommand{Key: b(key)}}}
}

func cmdLRange(key string, start, stop int64) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Lrange{Lrange: &ultimav1.LRangeCommand{Key: b(key), Start: start, Stop: stop}}}
}

func cmdLLen(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Llen{Llen: &ultimav1.LLenCommand{Key: b(key)}}}
}

func cmdSAdd(key string, members []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Sadd{Sadd: &ultimav1.SAddCommand{Key: b(key), Members: bb(members)}}}
}

func cmdSRem(key string, members []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Srem{Srem: &ultimav1.SRemCommand{Key: b(key), Members: bb(members)}}}
}

func cmdSMembers(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Smembers{Smembers: &ultimav1.SMembersCommand{Key: b(key)}}}
}

func cmdSIsMember(key, member string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Sismember{Sismember: &ultimav1.SIsMemberCommand{Key: b(key), Member: b(member)}}}
}

func cmdZAdd(key string, o ZAddOptions, members []ZMember) *ultimav1.Command {
	ms := make([]*ultimav1.ScoredMember, 0, len(members))
	for _, m := range members {
		ms = append(ms, &ultimav1.ScoredMember{Score: m.Score, Member: b(m.Member)})
	}
	return &ultimav1.Command{Cmd: &ultimav1.Command_Zadd{Zadd: &ultimav1.ZAddCommand{
		Key: b(key), Members: ms,
		Nx: o.NX, Xx: o.XX, Gt: o.GT, Lt: o.LT, Ch: o.CH, Incr: o.INCR,
	}}}
}

func cmdZScore(key, member string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Zscore{Zscore: &ultimav1.ZScoreCommand{Key: b(key), Member: b(member)}}}
}

func cmdZRange(key string, start, stop int64, withScores bool) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Zrange{Zrange: &ultimav1.ZRangeCommand{
		Key: b(key), Start: start, Stop: stop, Withscores: withScores,
	}}}
}

func cmdZRem(key string, members []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Zrem{Zrem: &ultimav1.ZRemCommand{Key: b(key), Members: bb(members)}}}
}

func cmdZCard(key string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Zcard{Zcard: &ultimav1.ZCardCommand{Key: b(key)}}}
}

// cmdGeneric builds the generic escape hatch (proto CommandRequest): the
// command name plus raw string arguments, parsed by the same engine the
// RESP surface feeds.
func cmdGeneric(cmd string, args []string) *ultimav1.Command {
	return &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
		Command: cmd, Args: bb(args),
	}}}
}

func bb(ss []string) [][]byte {
	out := make([][]byte, 0, len(ss))
	for _, s := range ss {
		out = append(out, b(s))
	}
	return out
}
