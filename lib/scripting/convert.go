package scripting

// Lua⇄RESP conversions (M8, §5.6 of the integration guide). Every rule
// here is probed against live Redis 7.2.7 and recorded in
// docs/Redis-Errors.md (§4 for script returns, §5 for redis.call); number
// formatting is host-side (decision S7) so the guest's number dialect can
// never leak into replies.

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pschlump/gopher-lua/host"
	"github.com/pschlump/ultima/lib/resp"
)

// NumToArg renders a Lua number as a redis.call argument (probed 7.2.7):
// integral and in int64 range → plain integer decimal; otherwise shortest
// round-trip decimal ("0.3333333333333333", "1e+30").
func NumToArg(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) &&
		f >= -9223372036854775808.0 && f < 9223372036854775808.0 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// NumToInt converts a Lua return number to a RESP integer (probed
// 7.2.7): truncation toward zero, clamped to int64, NaN → 0.
func NumToInt(f float64) int64 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= 9223372036854775808.0: // float64(MaxInt64) rounds up to 2^63
		return math.MaxInt64
	case f <= -9223372036854775808.0:
		return math.MinInt64
	default:
		return int64(f)
	}
}

// ToReply converts a script's return values to the client reply. Redis
// uses only the FIRST return value; a script returning nothing answers
// null. respVer is the run's final reply version (redis.setresp): under
// 3, booleans become RESP3 booleans (#t/#f — :1/:0 on a RESP2 client);
// under 2, true is :1 and false is null.
func ToReply(res host.Result, respVer int) resp.Value {
	if len(res.Values) == 0 {
		return resp.Null()
	}
	return LuaToRESP(res.Values[0], respVer)
}

// LuaToRESP converts one Lua value per the probed rules
// (docs/Redis-Errors.md §4).
func LuaToRESP(v host.Value, respVer int) resp.Value {
	switch v.Kind {
	case host.KindNil, host.KindFunction:
		return resp.Null()
	case host.KindFalse:
		if respVer == 3 {
			return resp.Bool(false)
		}
		return resp.Null()
	case host.KindTrue:
		if respVer == 3 {
			return resp.Bool(true)
		}
		return resp.Int(1)
	case host.KindNumber:
		return resp.Int(NumToInt(v.Num))
	case host.KindString:
		return resp.BlobStr(v.Str)
	case host.KindTable:
		return tableToRESP(v, respVer)
	case host.KindCycle, host.KindDeep:
		// Guest expansion caps (64 nesting levels — Redis nests ≥100,
		// ledgered divergence). A clean error, never a trap.
		return resp.Err("ERR Lua table is too deeply nested to convert")
	}
	return resp.Null()
}

// tableToRESP converts a Lua table, in precedence order (every rule
// probed): a string "err" field makes an error reply (verbatim), a
// string "ok" field a simple string, a table "map" field a map reply of
// the inner pairs, a table "set" field a set reply of the inner keys
// (values ignored), a number "double" field a double reply; otherwise
// the sequential array part 1..n is converted recursively, stopping at
// the first hole and ignoring non-sequential keys. Map/set/double
// replies render per the CLIENT's protocol in the wire layer.
func tableToRESP(v host.Value, respVer int) resp.Value {
	if s, ok := tableFieldStr(v, "err"); ok {
		return resp.Err(s)
	}
	if s, ok := tableFieldStr(v, "ok"); ok {
		return resp.Simple(s)
	}
	if t, ok := tableFieldTable(v, "map"); ok {
		pairs := make([]resp.Value, 0, len(t.Pairs)*2)
		for _, p := range t.Pairs {
			pairs = append(pairs, LuaToRESP(p.Key, respVer), LuaToRESP(p.Val, respVer))
		}
		return resp.Map(pairs...)
	}
	if t, ok := tableFieldTable(v, "set"); ok {
		elems := make([]resp.Value, 0, len(t.Pairs))
		for _, p := range t.Pairs {
			elems = append(elems, LuaToRESP(p.Key, respVer))
		}
		return resp.Set(elems...)
	}
	if d, ok := tableFieldNum(v, "double"); ok {
		return resp.Double(d)
	}
	elems := []resp.Value{}
	for i := 1; ; i++ {
		el, found := tableArrayElem(v, i)
		if !found {
			break
		}
		elems = append(elems, LuaToRESP(el, respVer))
	}
	return resp.Arr(elems...)
}

// tableFieldStr returns the string value of a named table field.
func tableFieldStr(v host.Value, name string) (string, bool) {
	for _, p := range v.Pairs {
		if p.Key.Kind == host.KindString && p.Key.Str == name && p.Val.Kind == host.KindString {
			return p.Val.Str, true
		}
	}
	return "", false
}

// tableFieldTable returns the table value of a named table field.
func tableFieldTable(v host.Value, name string) (host.Value, bool) {
	for _, p := range v.Pairs {
		if p.Key.Kind == host.KindString && p.Key.Str == name && p.Val.Kind == host.KindTable {
			return p.Val, true
		}
	}
	return host.Value{}, false
}

// tableFieldNum returns the number value of a named table field.
func tableFieldNum(v host.Value, name string) (float64, bool) {
	for _, p := range v.Pairs {
		if p.Key.Kind == host.KindString && p.Key.Str == name && p.Val.Kind == host.KindNumber {
			return p.Val.Num, true
		}
	}
	return 0, false
}

// tableArrayElem returns the value of the integral array key i (1-based).
func tableArrayElem(v host.Value, i int) (host.Value, bool) {
	for _, p := range v.Pairs {
		if p.Key.Kind == host.KindNumber && p.Key.Num == float64(i) {
			return p.Val, true
		}
	}
	return host.Value{}, false
}

// respToLua converts a redis.call reply to what the script sees (probed
// 7.2.7, docs/Redis-Errors.md §5). Under RESP 2 (the default): null →
// false, status replies → {ok=...} tables, integers → numbers, doubles
// → strings (the RESP2 wire form), aggregates → sequential tables (maps
// flattened). Under RESP 3 (redis.setresp(3)): null → nil, doubles →
// {double=...}, maps → {map={k=v,...}}, sets → {set={member=true,...}}.
func respToLua(v resp.Value, respVer int) host.Value {
	switch v.Kind {
	case resp.KindNull:
		if respVer == 3 {
			return host.Nil()
		}
		return host.Bool(false)
	case resp.KindInt:
		return host.Number(float64(v.Int))
	case resp.KindSimpleString:
		return okTable(v.Str)
	case resp.KindBlobString, resp.KindVerbatim:
		return host.String(string(v.Blob))
	case resp.KindError:
		return errTable(v.Str)
	case resp.KindDouble:
		if respVer == 3 {
			return host.Table(kv("double", host.Number(v.Dbl)))
		}
		return host.String(resp.FormatDouble(v.Dbl))
	case resp.KindBool:
		return host.Bool(v.Bool)
	case resp.KindBigNumber:
		return host.String(v.Str)
	case resp.KindArray, resp.KindPush:
		return arrayToLua(v.Arr, respVer)
	case resp.KindSet:
		if respVer == 3 {
			pairs := make([]host.KV, 0, len(v.Arr))
			for _, e := range v.Arr {
				pairs = append(pairs, host.KV{Key: respToLua(e, respVer), Val: host.Bool(true)})
			}
			return host.Table(kv("set", host.Table(pairs...)))
		}
		return arrayToLua(v.Arr, respVer)
	case resp.KindMap:
		if respVer == 3 {
			pairs := make([]host.KV, 0, len(v.Arr)/2)
			for i := 0; i+1 < len(v.Arr); i += 2 {
				pairs = append(pairs, host.KV{Key: respToLua(v.Arr[i], respVer), Val: respToLua(v.Arr[i+1], respVer)})
			}
			return host.Table(kv("map", host.Table(pairs...)))
		}
		return arrayToLua(v.Arr, respVer)
	}
	return host.Nil()
}

// arrayToLua converts a RESP aggregate's elements to a sequential Lua
// table (1..n).
func arrayToLua(elems []resp.Value, respVer int) host.Value {
	pairs := make([]host.KV, 0, len(elems))
	for i, e := range elems {
		pairs = append(pairs, host.KV{Key: host.Int(int64(i + 1)), Val: respToLua(e, respVer)})
	}
	return host.Table(pairs...)
}

// ErrorReply shapes a script error into the client reply (probed rules,
// docs/Redis-Errors.md §3): a table error with a string err field passes
// verbatim; a string error whose text starts with an error-code word
// (the class redis.call raises — Redis marks those with an error-object
// metatable we cannot reproduce) also passes verbatim; every other value
// gets "ERR " prepended (string messages keep the position prefix
// error() gave them; numbers are rendered with the position). The reply
// ends with the standard " script: <sha>, on @user_script:<line>."
// suffix.
//
// Ledgered collision: a LEVEL-0 user string starting with an error-code
// word (error('ERR already', 0), error('WRONGTYPE foo', 0)) takes the
// verbatim path, where Redis renders "ERR ERR already" — Redis's true
// distinguisher is the metatable, not the text.
func ErrorReply(sha string, se *host.ScriptError) resp.Value {
	line := se.Line
	if line <= 0 {
		// The deadline/kill error carries no line (gopher-lua ledger row
		// 37); Redis's kill message reports the script's current line.
		line = 1
	}
	v := se.ErrValue
	var text string
	if v.Kind == host.KindTable {
		if s, ok := tableFieldStr(v, "err"); ok {
			text = s // verbatim
		} else {
			// Redis 7.2.7 CRASHES on error() tables without an err field
			// (verified — docs/Redis-Errors.md §3); Ultima answers
			// cleanly. There is no parity target.
			text = "ERR Lua error: error object is a table without an 'err' field"
		}
	} else if v.Kind == host.KindString && isErrorCodePrefixed(v.Str) {
		text = v.Str // the redis.call error class: verbatim
	} else {
		text = "ERR " + luaErrorMessage(v, line)
	}
	return resp.Err(fmt.Sprintf("%s script: %s, on @user_script:%d.", text, sha, line))
}

// isErrorCodePrefixed reports whether an error string begins with a
// Redis error-code word (an all-caps token followed by a space) and no
// Lua position prefix — the shape of errors redis.call raises.
func isErrorCodePrefixed(s string) bool {
	if strings.HasPrefix(s, "user_script:") {
		return false
	}
	i := strings.IndexByte(s, ' ')
	if i < 2 {
		return false
	}
	for j := 0; j < i; j++ {
		c := s[j]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// luaErrorMessage renders a non-table error value the way Redis does:
// strings verbatim (error() already positioned them), numbers with the
// position prefix (Redis tostrings them there), other kinds plain.
func luaErrorMessage(v host.Value, line int) string {
	switch v.Kind {
	case host.KindString:
		return v.Str
	case host.KindNumber:
		return fmt.Sprintf("user_script:%d: %s", line, NumToArg(v.Num))
	case host.KindTrue:
		return "true"
	case host.KindFalse:
		return "false"
	case host.KindNil:
		return "nil"
	case host.KindFunction:
		return "function"
	}
	return "unknown error"
}
