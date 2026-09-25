package host

// The Value seam between the host and the Lua interpreter (the pure-Go
// port of the wasm backend's wire protocol, host/wire.go in the fork).
// Values cross in both directions: script return values and HostFunc
// arguments (LValue → Value), HostFunc results and staged constants
// (Value → LValue).

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	lua "github.com/yuin/gopher-lua"
)

// ValueKind mirrors the fork's wire tags (PT_* in runtime/luawasm.c).
type ValueKind uint8

const (
	KindNil      ValueKind = 0
	KindFalse    ValueKind = 1
	KindTrue     ValueKind = 2
	KindNumber   ValueKind = 3
	KindString   ValueKind = 4
	KindTable    ValueKind = 5
	KindFunction ValueKind = 6
	KindCycle    ValueKind = 10
	KindDeep     ValueKind = 11
)

// KV is one table entry.
type KV struct {
	Key, Val Value
}

// Value is one Lua value as it crosses the seam.
type Value struct {
	Kind  ValueKind
	Num   float64 // KindNumber
	Str   string  // KindString
	Pairs []KV    // KindTable
	More  bool    // KindTable: the expansion was truncated (args side)
}

// Convenience constructors.
func Nil() Value { return Value{Kind: KindNil} }
func Bool(b bool) Value {
	if b {
		return Value{Kind: KindTrue}
	}
	return Value{Kind: KindFalse}
}
func Number(f float64) Value { return Value{Kind: KindNumber, Num: f} }
func Int(n int64) Value      { return Value{Kind: KindNumber, Num: float64(n)} }
func String(s string) Value  { return Value{Kind: KindString, Str: s} }
func Table(pairs ...KV) Value {
	return Value{Kind: KindTable, Pairs: pairs}
}

// BoolVal reports the boolean of a KindTrue/KindFalse value.
func (v Value) BoolVal() bool { return v.Kind == KindTrue }

// String renders a value for error messages and logs (NOT the Redis reply
// conversion — that is the daemon's job, host-side and locale-free).
func (v Value) String() string {
	switch v.Kind {
	case KindNil:
		return "nil"
	case KindFalse:
		return "false"
	case KindTrue:
		return "true"
	case KindNumber:
		return numRepr(v.Num)
	case KindString:
		return v.Str
	case KindTable:
		return "table"
	case KindFunction:
		return "function"
	case KindCycle:
		return "table (cycle)"
	case KindDeep:
		return "table (too deep)"
	}
	return "?"
}

// numRepr formats a number the way the interpreter renders numbers in
// messages: integers plain, otherwise Go's shortest round-trip %g.
func numRepr(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) &&
		math.Abs(f) < 1e15 {
		return fmt.Sprintf("%d", int64(f))
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// maxExpandDepth caps table expansion (LValue → Value): deeper nesting
// yields KindDeep, a cycle yields KindCycle. The daemon renders both as
// the "too deeply nested" reply (ledgered divergence from Redis's ≥100).
const maxExpandDepth = 64

// lvalToValue converts one Lua value to a host Value, expanding tables
// recursively with the depth/cycle caps. stack tracks the tables on the
// current expansion path (cycle detection).
func lvalToValue(lv lua.LValue, depth int, stack map[*lua.LTable]bool) Value {
	switch t := lv.(type) {
	case *lua.LNilType:
		return Nil()
	case lua.LBool:
		return Bool(bool(t))
	case lua.LNumber:
		return Number(float64(t))
	case lua.LString:
		return String(string(t))
	case *lua.LFunction:
		return Value{Kind: KindFunction}
	case *lua.LTable:
		if depth >= maxExpandDepth {
			return Value{Kind: KindDeep}
		}
		if stack[t] {
			return Value{Kind: KindCycle}
		}
		stack[t] = true
		defer delete(stack, t)
		v := Value{Kind: KindTable}
		t.ForEach(func(k, val lua.LValue) {
			v.Pairs = append(v.Pairs, KV{
				Key: lvalToValue(k, depth+1, stack),
				Val: lvalToValue(val, depth+1, stack),
			})
		})
		return v
	default:
		// userdata/thread/channel: opaque across the seam (the wasm wire
		// protocol had no tags for them either); treat as a function-like
		// opaque value so error rendering says something sane.
		return Value{Kind: KindFunction}
	}
}

// toValue is the depth-0 entry point.
func toValue(lv lua.LValue) Value {
	return lvalToValue(lv, 0, map[*lua.LTable]bool{})
}

// valueToLVal converts a host Value to an LValue (HostFunc results,
// staged constants). Only the kinds the interpreter can accept — nil,
// bool, number, string, table — anything else is a host-side bug.
func valueToLVal(L *lua.LState, v Value) (lua.LValue, error) {
	switch v.Kind {
	case KindNil:
		return lua.LNil, nil
	case KindFalse:
		return lua.LFalse, nil
	case KindTrue:
		return lua.LTrue, nil
	case KindNumber:
		return lua.LNumber(v.Num), nil
	case KindString:
		return lua.LString(v.Str), nil
	case KindTable:
		t := L.NewTable()
		for _, kv := range v.Pairs {
			k, err := valueToLVal(L, kv.Key)
			if err != nil {
				return nil, err
			}
			val, err := valueToLVal(L, kv.Val)
			if err != nil {
				return nil, err
			}
			if k == lua.LNil || val == lua.LNil {
				continue // a nil key is a host bug; a nil value is a no-op entry
			}
			t.RawSet(k, val)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("host: cannot send a %s value to the guest", v)
	}
}

// ValueError lets a HostFunc raise a non-string Lua error value — e.g. a
// table {err="..."}, the error class redis.call raises in Redis (the
// script's pcall then sees a table and the daemon renders the text
// verbatim, instead of the error gaining an engine-added prefix).
type ValueError struct{ V Value }

func (e *ValueError) Error() string { return "host function error: " + e.V.String() }

// asValueError extracts a *ValueError.
func asValueError(err error) (*ValueError, bool) {
	var ve *ValueError
	if errors.As(err, &ve) {
		return ve, true
	}
	return nil, false
}
