// Package envelope is the bridge between the protobuf Command envelope
// (design doc §6.2, decision D15) and the front-end-agnostic command
// engine. It is shared by the gRPC front-end (lib/grpcsrv) and the
// WebSocket front-end (lib/wssrv, §6.3): both unmarshal a
// ultimav1.Command off their transport, hand it to Execute, and marshal
// the returned CommandResponse — the parse shim is the only stage the
// typed messages skip, and both surfaces skip it identically.
package envelope

import (
	"errors"
	"strconv"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/resp"
)

// errEmpty is returned for a Command with no oneof field set; it maps to a
// protocol error on the carrying transport rather than an engine reply.
var errEmpty = errors.New("envelope: command has no cmd field set")

// Execute runs one Command against the engine on behalf of the connection
// cs and returns the response carrying cmd's correlation seq. The optional
// per-command db override applies to this command only and never disturbs
// the session's selected DB. Extra frames the engine queued on the
// connection's outbox (subscribe acks, self-addressed publish pushes) are
// drained and appended after the primary reply with the same seq, in the
// order the RESP front-end would have written them.
func Execute(eng *commands.Engine, cs *commands.ConnState, cmd *ultimav1.Command) []*ultimav1.CommandResponse {
	args, err := ArgsFor(cmd)
	if err != nil {
		return []*ultimav1.CommandResponse{{
			Seq:   cmd.GetSeq(),
			Reply: ToProto(resp.Err("ERR " + err.Error())),
		}}
	}

	if db := cmd.GetDb(); db != 0 {
		if db >= uint64(eng.Shards.MaxDBs()) {
			return []*ultimav1.CommandResponse{{
				Seq:   cmd.GetSeq(),
				Reply: ToProto(resp.Err("ERR DB index is out of range")),
			}}
		}
		saved := cs.DB
		cs.DB = int(db)
		defer func() { cs.DB = saved }()
	}

	v := eng.Execute(cs, args)
	out := make([]*ultimav1.CommandResponse, 0, 1)
	out = append(out, &ultimav1.CommandResponse{Seq: cmd.GetSeq(), Reply: ToProto(v)})
	for _, extra := range cs.DrainOutbox() {
		out = append(out, &ultimav1.CommandResponse{Seq: cmd.GetSeq(), Reply: ToProto(extra)})
	}
	return out
}

// ArgsFor converts a Command into the engine's argv form (args[0] is the
// upper-case command name). Typed messages build canonical arguments
// without any client text having been parsed; the generic escape hatch
// passes its name and args through. The engine remains the sole validator
// of arity and option grammar, so every error string stays byte-exact with
// the RESP surface.
func ArgsFor(cmd *ultimav1.Command) ([][]byte, error) {
	switch c := cmd.GetCmd().(type) {
	case *ultimav1.Command_Get:
		return argv("GET", c.Get.GetKey()), nil
	case *ultimav1.Command_Set:
		a := argv("SET", c.Set.GetKey(), c.Set.GetValue())
		if c.Set.GetTtlMs() > 0 {
			a = append(a, s2b("PX"), int64b(c.Set.GetTtlMs()))
		}
		if c.Set.GetNx() {
			a = append(a, s2b("NX"))
		}
		if c.Set.GetXx() {
			a = append(a, s2b("XX"))
		}
		if c.Set.GetGet() {
			a = append(a, s2b("GET"))
		}
		return a, nil
	case *ultimav1.Command_Del:
		return argv("DEL", c.Del.GetKeys()...), nil
	case *ultimav1.Command_Incr:
		// INCRBY covers INCR/DECR/INCRBY/DECRBY: the delta carries the sign.
		return argv("INCRBY", c.Incr.GetKey(), int64b(c.Incr.GetDelta())), nil
	case *ultimav1.Command_IncrFloat:
		return argv("INCRBYFLOAT", c.IncrFloat.GetKey(), float64b(c.IncrFloat.GetDelta())), nil
	case *ultimav1.Command_Mget:
		return argv("MGET", c.Mget.GetKeys()...), nil
	case *ultimav1.Command_Mset:
		a := argv("MSET")
		for _, p := range c.Mset.GetPairs() {
			a = append(a, p.GetKey(), p.GetValue())
		}
		return a, nil
	case *ultimav1.Command_Append:
		return argv("APPEND", c.Append.GetKey(), c.Append.GetValue()), nil
	case *ultimav1.Command_Exists:
		return argv("EXISTS", c.Exists.GetKeys()...), nil
	case *ultimav1.Command_Expire:
		// The typed form carries milliseconds and maps to PEXPIRE; the
		// second-granularity EXPIRE stays reachable via the generic hatch.
		return argv("PEXPIRE", c.Expire.GetKey(), int64b(c.Expire.GetTtlMs())), nil
	case *ultimav1.Command_Ttl:
		return argv("PTTL", c.Ttl.GetKey()), nil
	case *ultimav1.Command_Persist:
		return argv("PERSIST", c.Persist.GetKey()), nil
	case *ultimav1.Command_Hget:
		return argv("HGET", c.Hget.GetKey(), c.Hget.GetField()), nil
	case *ultimav1.Command_Hset:
		a := argv("HSET", c.Hset.GetKey())
		for _, p := range c.Hset.GetPairs() {
			a = append(a, p.GetField(), p.GetValue())
		}
		return a, nil
	case *ultimav1.Command_Hgetall:
		return argv("HGETALL", c.Hgetall.GetKey()), nil
	case *ultimav1.Command_Hdel:
		return argv("HDEL", append([][]byte{c.Hdel.GetKey()}, c.Hdel.GetFields()...)...), nil
	case *ultimav1.Command_Hincrby:
		return argv("HINCRBY", c.Hincrby.GetKey(), c.Hincrby.GetField(), int64b(c.Hincrby.GetDelta())), nil
	case *ultimav1.Command_Lpush:
		return argv("LPUSH", append([][]byte{c.Lpush.GetKey()}, c.Lpush.GetElems()...)...), nil
	case *ultimav1.Command_Rpush:
		return argv("RPUSH", append([][]byte{c.Rpush.GetKey()}, c.Rpush.GetElems()...)...), nil
	case *ultimav1.Command_Lpop:
		return argv("LPOP", c.Lpop.GetKey()), nil
	case *ultimav1.Command_Rpop:
		return argv("RPOP", c.Rpop.GetKey()), nil
	case *ultimav1.Command_Lrange:
		return argv("LRANGE", c.Lrange.GetKey(), int64b(c.Lrange.GetStart()), int64b(c.Lrange.GetStop())), nil
	case *ultimav1.Command_Llen:
		return argv("LLEN", c.Llen.GetKey()), nil
	case *ultimav1.Command_Sadd:
		return argv("SADD", append([][]byte{c.Sadd.GetKey()}, c.Sadd.GetMembers()...)...), nil
	case *ultimav1.Command_Srem:
		return argv("SREM", append([][]byte{c.Srem.GetKey()}, c.Srem.GetMembers()...)...), nil
	case *ultimav1.Command_Smembers:
		return argv("SMEMBERS", c.Smembers.GetKey()), nil
	case *ultimav1.Command_Sismember:
		return argv("SISMEMBER", c.Sismember.GetKey(), c.Sismember.GetMember()), nil
	case *ultimav1.Command_Zadd:
		a := argv("ZADD", c.Zadd.GetKey())
		if c.Zadd.GetNx() {
			a = append(a, s2b("NX"))
		}
		if c.Zadd.GetXx() {
			a = append(a, s2b("XX"))
		}
		if c.Zadd.GetGt() {
			a = append(a, s2b("GT"))
		}
		if c.Zadd.GetLt() {
			a = append(a, s2b("LT"))
		}
		if c.Zadd.GetCh() {
			a = append(a, s2b("CH"))
		}
		if c.Zadd.GetIncr() {
			a = append(a, s2b("INCR"))
		}
		for _, m := range c.Zadd.GetMembers() {
			a = append(a, float64b(m.GetScore()), m.GetMember())
		}
		return a, nil
	case *ultimav1.Command_Zscore:
		return argv("ZSCORE", c.Zscore.GetKey(), c.Zscore.GetMember()), nil
	case *ultimav1.Command_Zrange:
		a := argv("ZRANGE", c.Zrange.GetKey(), int64b(c.Zrange.GetStart()), int64b(c.Zrange.GetStop()))
		if c.Zrange.GetWithscores() {
			a = append(a, s2b("WITHSCORES"))
		}
		return a, nil
	case *ultimav1.Command_Zrem:
		return argv("ZREM", append([][]byte{c.Zrem.GetKey()}, c.Zrem.GetMembers()...)...), nil
	case *ultimav1.Command_Zcard:
		return argv("ZCARD", c.Zcard.GetKey()), nil
	case *ultimav1.Command_Generic:
		g := c.Generic
		if g.GetCommand() == "" {
			return nil, errors.New("unknown command ''")
		}
		a := make([][]byte, 0, len(g.GetArgs())+1)
		a = append(a, s2b(g.GetCommand()))
		return append(a, g.GetArgs()...), nil
	default:
		return nil, errEmpty
	}
}

// ToProto converts an engine reply into its protobuf mirror. The mapping is
// one-to-one with the RESP3 type system (lib/resp ValueKind); maps arrive
// as ordered pairs rather than RESP's flattened key,value,key,value run.
func ToProto(v resp.Value) *ultimav1.Value {
	switch v.Kind {
	case resp.KindSimpleString:
		return &ultimav1.Value{Kind: &ultimav1.Value_SimpleString{SimpleString: v.Str}}
	case resp.KindError:
		return &ultimav1.Value{Kind: &ultimav1.Value_Error{Error: v.Str}}
	case resp.KindInt:
		return &ultimav1.Value{Kind: &ultimav1.Value_Int{Int: v.Int}}
	case resp.KindDouble:
		return &ultimav1.Value{Kind: &ultimav1.Value_Double{Double: v.Dbl}}
	case resp.KindBool:
		return &ultimav1.Value{Kind: &ultimav1.Value_Bool{Bool: v.Bool}}
	case resp.KindBlobString:
		if v.Blob == nil {
			return &ultimav1.Value{Kind: &ultimav1.Value_Null{Null: true}}
		}
		return &ultimav1.Value{Kind: &ultimav1.Value_BlobString{BlobString: v.Blob}}
	case resp.KindBigNumber:
		return &ultimav1.Value{Kind: &ultimav1.Value_BigNumber{BigNumber: v.Str}}
	case resp.KindVerbatim:
		return &ultimav1.Value{Kind: &ultimav1.Value_Verbatim{
			Verbatim: &ultimav1.Verbatim{Format: v.Fmt, Payload: v.Blob},
		}}
	case resp.KindArray:
		return &ultimav1.Value{Kind: &ultimav1.Value_Array{
			Array: &ultimav1.Array{Elems: toProtoSlice(v.Arr)},
		}}
	case resp.KindMap:
		pairs := make([]*ultimav1.Pair, 0, len(v.Arr)/2)
		for i := 0; i+1 < len(v.Arr); i += 2 {
			pairs = append(pairs, &ultimav1.Pair{Key: ToProto(v.Arr[i]), Value: ToProto(v.Arr[i+1])})
		}
		return &ultimav1.Value{Kind: &ultimav1.Value_Map{Map: &ultimav1.Map{Pairs: pairs}}}
	case resp.KindSet:
		return &ultimav1.Value{Kind: &ultimav1.Value_Set{
			Set: &ultimav1.Set{Elems: toProtoSlice(v.Arr)},
		}}
	case resp.KindPush:
		return &ultimav1.Value{Kind: &ultimav1.Value_Push{
			Push: &ultimav1.Push{Elems: toProtoSlice(v.Arr)},
		}}
	default: // KindNull and anything unrecognised
		return &ultimav1.Value{Kind: &ultimav1.Value_Null{Null: true}}
	}
}

func toProtoSlice(vs []resp.Value) []*ultimav1.Value {
	out := make([]*ultimav1.Value, 0, len(vs))
	for _, e := range vs {
		out = append(out, ToProto(e))
	}
	return out
}

// FromProto is the exact inverse of ToProto: it rebuilds an engine reply
// value from its protobuf mirror. It exists so tests (and future binary
// clients) can render a gRPC/WS reply through resp.AppendValue and diff it
// byte-for-byte against the RESP surface — the M4 parity gate.
func FromProto(v *ultimav1.Value) resp.Value {
	switch k := v.GetKind().(type) {
	case *ultimav1.Value_SimpleString:
		return resp.Simple(k.SimpleString)
	case *ultimav1.Value_Error:
		return resp.Err(k.Error)
	case *ultimav1.Value_Int:
		return resp.Int(k.Int)
	case *ultimav1.Value_Double:
		return resp.Double(k.Double)
	case *ultimav1.Value_Bool:
		return resp.Bool(k.Bool)
	case *ultimav1.Value_BlobString:
		return resp.BlobString(k.BlobString)
	case *ultimav1.Value_BigNumber:
		return resp.Value{Kind: resp.KindBigNumber, Str: k.BigNumber}
	case *ultimav1.Value_Verbatim:
		return resp.Value{Kind: resp.KindVerbatim, Fmt: k.Verbatim.GetFormat(), Blob: k.Verbatim.GetPayload()}
	case *ultimav1.Value_Array:
		return resp.Arr(fromProtoSlice(k.Array.GetElems())...)
	case *ultimav1.Value_Map:
		var flat []resp.Value
		for _, p := range k.Map.GetPairs() {
			flat = append(flat, FromProto(p.GetKey()), FromProto(p.GetValue()))
		}
		return resp.Map(flat...)
	case *ultimav1.Value_Set:
		return resp.Set(fromProtoSlice(k.Set.GetElems())...)
	case *ultimav1.Value_Push:
		return resp.Push(fromProtoSlice(k.Push.GetElems())...)
	default: // Value_Null and empty
		return resp.Null()
	}
}

func fromProtoSlice(vs []*ultimav1.Value) []resp.Value {
	out := make([]resp.Value, 0, len(vs))
	for _, e := range vs {
		out = append(out, FromProto(e))
	}
	return out
}

func argv(name string, args ...[]byte) [][]byte {
	return append([][]byte{s2b(name)}, args...)
}

func s2b(s string) []byte { return []byte(s) }

func int64b(n int64) []byte { return []byte(strconv.FormatInt(n, 10)) }

// float64b renders a float64 exactly (shortest round-trip, no exponent) so
// the engine's string2d-exact parsers reconstruct the identical double.
func float64b(f float64) []byte { return []byte(strconv.FormatFloat(f, 'f', -1, 64)) }
