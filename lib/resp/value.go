package resp

// Value is the typed command-reply interchange between the command engine
// (lib/commands) and every front-end (decision D3: one command engine,
// three front-ends). It mirrors the RESP3 type system; the RESP front-end
// renders it with Writer.WriteValue, downgrading to RESP2 for pre-HELLO 3
// clients the way Redis does (maps become flat arrays, doubles become bulk
// strings, null becomes $-1, booleans become :1/:0, …).

// ValueKind enumerates the reply types, one per RESP3 type.
type ValueKind int

const (
	KindNull         ValueKind = iota // RESP3 _, RESP2 $-1
	KindSimpleString                  // +
	KindError                         // -
	KindInt                           // :
	KindDouble                        // ,
	KindBool                          // #
	KindBlobString                    // $
	KindBigNumber                     // (
	KindVerbatim                      // = (Fmt carries the 3-byte format)
	KindArray                         // *
	KindMap                           // % (Arr holds key,value,key,value,…)
	KindSet                           // ~ (Arr holds the elements)
	KindPush                          // > (Arr holds the elements)
)

// Value is one typed reply (or one nested element of one).
type Value struct {
	Kind ValueKind
	Str  string  // SimpleString, Error, BigNumber text
	Blob []byte  // BlobString, Verbatim payload
	Int  int64   // Int
	Dbl  float64 // Double
	Bool bool    // Bool
	Fmt  string  // Verbatim format tag ("txt", "mkd")
	Arr  []Value // Array, Map (flattened pairs), Set, Push
}

// Null is the null reply (missing key, unset name, …).
func Null() Value { return Value{Kind: KindNull} }

// Simple is a + reply ("OK", "PONG", …).
func Simple(s string) Value { return Value{Kind: KindSimpleString, Str: s} }

// Err is a - reply; msg must carry its own prefix ("ERR …", "WRONGTYPE …").
func Err(msg string) Value { return Value{Kind: KindError, Str: msg} }

// Int is a : reply.
func Int(n int64) Value { return Value{Kind: KindInt, Int: n} }

// Double is a , reply (RESP3) downgraded to a bulk string under RESP2.
func Double(f float64) Value { return Value{Kind: KindDouble, Dbl: f} }

// Bool is a # reply (RESP3) downgraded to :1/:0 under RESP2.
func Bool(v bool) Value { return Value{Kind: KindBool, Bool: v} }

// BlobString is a $ reply.
func BlobString(b []byte) Value { return Value{Kind: KindBlobString, Blob: b} }

// BlobStr is a $ reply from a string.
func BlobStr(s string) Value { return Value{Kind: KindBlobString, Blob: []byte(s)} }

// Arr is a * reply.
func Arr(elems ...Value) Value { return Value{Kind: KindArray, Arr: elems} }

// Map is a % reply; pairs are (key, value) tuples.
func Map(pairs ...Value) Value { return Value{Kind: KindMap, Arr: pairs} }

// Set is a ~ reply (RESP3) downgraded to an array under RESP2.
func Set(elems ...Value) Value { return Value{Kind: KindSet, Arr: elems} }

// Push is a > reply (RESP3 push, e.g. a pub/sub message or subscribe ack)
// downgraded to an array under RESP2.
func Push(elems ...Value) Value { return Value{Kind: KindPush, Arr: elems} }

// WriteValue renders v onto the writer for the given protocol version
// (2 or 3), applying the RESP2 downgrades.
func (w *Writer) WriteValue(proto int, v Value) {
	if w.err != nil {
		return
	}
	w.b = AppendValue(w.b, proto, v)
}

// AppendValue appends the rendering of v to b; it is the worker behind
// WriteValue and is exported for tests and future front-ends.
func AppendValue(b []byte, proto int, v Value) []byte {
	switch v.Kind {
	case KindSimpleString:
		return AppendString(b, v.Str)
	case KindError:
		return AppendError(b, v.Str)
	case KindInt:
		return AppendInt(b, v.Int)
	case KindBlobString:
		if v.Blob == nil {
			return AppendNull(b)
		}
		return AppendBulk(b, v.Blob)
	case KindArray:
		b = AppendArray(b, len(v.Arr))
		for _, e := range v.Arr {
			b = AppendValue(b, proto, e)
		}
		return b
	case KindNull:
		if proto == 3 {
			return AppendNull3(b)
		}
		return AppendNull(b)
	case KindDouble:
		if proto == 3 {
			return AppendDouble(b, v.Dbl)
		}
		d := appendDouble(v.Dbl)
		return AppendBulk(b, d[:len(d)-2]) // strip CRLF for the bulk form
	case KindBool:
		if proto == 3 {
			return AppendBool(b, v.Bool)
		}
		if v.Bool {
			return AppendInt(b, 1)
		}
		return AppendInt(b, 0)
	case KindBigNumber:
		if proto == 3 {
			return AppendBigNumber(b, v.Str)
		}
		return AppendBulkString(b, v.Str)
	case KindVerbatim:
		if proto == 3 {
			return AppendVerbatim(b, v.Fmt, string(v.Blob))
		}
		return AppendBulk(b, v.Blob)
	case KindMap:
		if proto == 3 {
			b = AppendMap(b, len(v.Arr)/2)
		} else {
			b = AppendArray(b, len(v.Arr))
		}
		for _, e := range v.Arr {
			b = AppendValue(b, proto, e)
		}
		return b
	case KindSet:
		if proto == 3 {
			b = AppendSet(b, len(v.Arr))
		} else {
			b = AppendArray(b, len(v.Arr))
		}
		for _, e := range v.Arr {
			b = AppendValue(b, proto, e)
		}
		return b
	case KindPush:
		if proto == 3 {
			b = AppendPush(b, len(v.Arr))
		} else {
			b = AppendArray(b, len(v.Arr))
		}
		for _, e := range v.Arr {
			b = AppendValue(b, proto, e)
		}
		return b
	}
	return AppendNull(b)
}
