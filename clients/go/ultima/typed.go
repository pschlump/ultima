// Package ultima typed hot-command helpers, shared by the gRPC and
// WebSocket clients
// (design doc §6.2/§6.3): both surfaces carry the same protobuf Command
// envelope (D15), so the ~30 typed commands are implemented once here and
// promoted into GRPCClient and WSClient by embedding. Every helper returns
// the engine reply as resp.Value — error replies arrive as KindError
// values, not Go errors; Go errors mean transport/protocol failure.
package ultima

import (
	"context"
	"errors"
	"time"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/resp"
)

// binarySurface is embedded by GRPCClient and WSClient; execFn routes one
// built Command through the concrete transport.
type binarySurface struct {
	execFn func(ctx context.Context, cmd *ultimav1.Command) (resp.Value, error)
}

func (s *binarySurface) run(cmd *ultimav1.Command) (resp.Value, error) {
	return s.execFn(context.Background(), cmd)
}

// Exec runs one generic command (name plus string args).
func (s *binarySurface) Exec(cmd string, args ...string) (resp.Value, error) {
	return s.run(cmdGeneric(cmd, args))
}

// Get — GET key.
func (s *binarySurface) Get(key string) (resp.Value, error) { return s.run(cmdGet(key)) }

// Set — SET key value.
func (s *binarySurface) Set(key, value string) (resp.Value, error) {
	return s.run(cmdSet(key, value, SetOptions{}))
}

// SetOpts — SET key value with NX/XX/GET/PX modifiers.
func (s *binarySurface) SetOpts(key, value string, o SetOptions) (resp.Value, error) {
	return s.run(cmdSet(key, value, o))
}

// Del — DEL key [key ...].
func (s *binarySurface) Del(keys ...string) (resp.Value, error) { return s.run(cmdDel(keys)) }

// Incr — INCR key.
func (s *binarySurface) Incr(key string) (resp.Value, error) { return s.run(cmdIncr(key, 1)) }

// Decr — DECR key.
func (s *binarySurface) Decr(key string) (resp.Value, error) { return s.run(cmdIncr(key, -1)) }

// IncrBy — INCRBY key delta.
func (s *binarySurface) IncrBy(key string, delta int64) (resp.Value, error) {
	return s.run(cmdIncr(key, delta))
}

// DecrBy — DECRBY key delta.
func (s *binarySurface) DecrBy(key string, delta int64) (resp.Value, error) {
	return s.run(cmdIncr(key, -delta))
}

// IncrByFloat — INCRBYFLOAT key delta.
func (s *binarySurface) IncrByFloat(key string, delta float64) (resp.Value, error) {
	return s.run(cmdIncrFloat(key, delta))
}

// MGet — MGET key [key ...].
func (s *binarySurface) MGet(keys ...string) (resp.Value, error) { return s.run(cmdMGet(keys)) }

// MSet — MSET key value [key value ...]; kv alternates keys and values.
func (s *binarySurface) MSet(kv ...string) (resp.Value, error) {
	if len(kv)%2 != 0 {
		return resp.Value{}, errors.New("ultima: MSet requires an even number of key/value arguments")
	}
	return s.run(cmdMSet(kv))
}

// Append — APPEND key value.
func (s *binarySurface) Append(key, value string) (resp.Value, error) {
	return s.run(cmdAppend(key, value))
}

// Exists — EXISTS key [key ...].
func (s *binarySurface) Exists(keys ...string) (resp.Value, error) {
	return s.run(cmdExists(keys))
}

// Expire — sets a TTL; the typed form carries milliseconds and maps to
// PEXPIRE (proto ExpireCommand). Use Exec("EXPIRE", ...) for the
// second-granularity form.
func (s *binarySurface) Expire(key string, ttl time.Duration) (resp.Value, error) {
	return s.run(cmdExpire(key, ttl))
}

// PTTL — PTTL key (the typed TTL command answers in milliseconds).
func (s *binarySurface) PTTL(key string) (resp.Value, error) { return s.run(cmdPTTL(key)) }

// Persist — PERSIST key.
func (s *binarySurface) Persist(key string) (resp.Value, error) { return s.run(cmdPersist(key)) }

// HGet — HGET key field.
func (s *binarySurface) HGet(key, field string) (resp.Value, error) {
	return s.run(cmdHGet(key, field))
}

// HSet — HSET key field value [field value ...]; fv alternates.
func (s *binarySurface) HSet(key string, fv ...string) (resp.Value, error) {
	if len(fv)%2 != 0 {
		return resp.Value{}, errors.New("ultima: HSet requires an even number of field/value arguments")
	}
	return s.run(cmdHSet(key, fv))
}

// HGetAll — HGETALL key.
func (s *binarySurface) HGetAll(key string) (resp.Value, error) { return s.run(cmdHGetAll(key)) }

// HDel — HDEL key field [field ...].
func (s *binarySurface) HDel(key string, fields ...string) (resp.Value, error) {
	return s.run(cmdHDel(key, fields))
}

// HIncrBy — HINCRBY key field delta.
func (s *binarySurface) HIncrBy(key, field string, delta int64) (resp.Value, error) {
	return s.run(cmdHIncrBy(key, field, delta))
}

// LPush — LPUSH key elem [elem ...].
func (s *binarySurface) LPush(key string, elems ...string) (resp.Value, error) {
	return s.run(cmdLPush(key, elems))
}

// RPush — RPUSH key elem [elem ...].
func (s *binarySurface) RPush(key string, elems ...string) (resp.Value, error) {
	return s.run(cmdRPush(key, elems))
}

// LPop — LPOP key.
func (s *binarySurface) LPop(key string) (resp.Value, error) { return s.run(cmdLPop(key)) }

// RPop — RPOP key.
func (s *binarySurface) RPop(key string) (resp.Value, error) { return s.run(cmdRPop(key)) }

// LRange — LRANGE key start stop.
func (s *binarySurface) LRange(key string, start, stop int64) (resp.Value, error) {
	return s.run(cmdLRange(key, start, stop))
}

// LLen — LLEN key.
func (s *binarySurface) LLen(key string) (resp.Value, error) { return s.run(cmdLLen(key)) }

// SAdd — SADD key member [member ...].
func (s *binarySurface) SAdd(key string, members ...string) (resp.Value, error) {
	return s.run(cmdSAdd(key, members))
}

// SRem — SREM key member [member ...].
func (s *binarySurface) SRem(key string, members ...string) (resp.Value, error) {
	return s.run(cmdSRem(key, members))
}

// SMembers — SMEMBERS key.
func (s *binarySurface) SMembers(key string) (resp.Value, error) { return s.run(cmdSMembers(key)) }

// SIsMember — SISMEMBER key member.
func (s *binarySurface) SIsMember(key, member string) (resp.Value, error) {
	return s.run(cmdSIsMember(key, member))
}

// ZAdd — ZADD key score member [score member ...].
func (s *binarySurface) ZAdd(key string, members ...ZMember) (resp.Value, error) {
	return s.run(cmdZAdd(key, ZAddOptions{}, members))
}

// ZAddOpts — ZADD with NX/XX/GT/LT/CH/INCR modifiers.
func (s *binarySurface) ZAddOpts(key string, o ZAddOptions, members ...ZMember) (resp.Value, error) {
	return s.run(cmdZAdd(key, o, members))
}

// ZScore — ZSCORE key member.
func (s *binarySurface) ZScore(key, member string) (resp.Value, error) {
	return s.run(cmdZScore(key, member))
}

// ZRange — ZRANGE key start stop.
func (s *binarySurface) ZRange(key string, start, stop int64) (resp.Value, error) {
	return s.run(cmdZRange(key, start, stop, false))
}

// ZRangeWithScores — ZRANGE key start stop WITHSCORES (flat
// member,score,member,score reply, as on the wire).
func (s *binarySurface) ZRangeWithScores(key string, start, stop int64) (resp.Value, error) {
	return s.run(cmdZRange(key, start, stop, true))
}

// ZRem — ZREM key member [member ...].
func (s *binarySurface) ZRem(key string, members ...string) (resp.Value, error) {
	return s.run(cmdZRem(key, members))
}

// ZCard — ZCARD key.
func (s *binarySurface) ZCard(key string) (resp.Value, error) { return s.run(cmdZCard(key)) }
