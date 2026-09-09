// Adversarial round-trip tests for the protobuf↔resp.Value bridge. The M4
// RESP↔gRPC wire-byte parity gate (tests/m4_parity_test.go) depends on
// FromProto being the exact inverse of ToProto, so these tests pin the
// identity down for every RESP3 type the codebase can produce — and, just
// as importantly, pin the places where the identity deliberately does NOT
// hold (see TestLossyAsymmetries).
//
// Note on coverage: the RESP3 attribute type (|) has no representation in
// resp.Value at all (no field for it), so there is nothing to round-trip;
// that is a type-system gap in lib/resp, not an envelope asymmetry.
package envelope_test

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/resp"
	"google.golang.org/protobuf/proto"
)

// eqValue compares two resp.Values for exact identity with two documented
// tolerances:
//
//   - any NaN equals any NaN (bit-exact NaN payload preservation is
//     asserted separately in TestNaNDoubleWireBytesStable);
//   - a nil Arr slice equals an empty one: the fork renders both as *0 —
//     there is no *-1 null-array form in this Value model — so the
//     distinction is not wire-visible, and toProtoSlice normalizes nil to
//     empty on the way through.
//
// Blob nil-ness IS significant and compared exactly: a nil blob renders as
// the RESP null ($-1) while an empty non-nil blob renders as $0.
func eqValue(a, b resp.Value) bool {
	if a.Kind != b.Kind || a.Str != b.Str || a.Int != b.Int ||
		a.Bool != b.Bool || a.Fmt != b.Fmt {
		return false
	}
	if a.Kind == resp.KindDouble {
		if math.IsNaN(a.Dbl) || math.IsNaN(b.Dbl) {
			return math.IsNaN(a.Dbl) && math.IsNaN(b.Dbl)
		}
		// Float64bits, not ==, so +0 and -0 stay distinct.
		if math.Float64bits(a.Dbl) != math.Float64bits(b.Dbl) {
			return false
		}
	} else if a.Dbl != b.Dbl {
		return false // Dbl is unused for other kinds; stay exact anyway.
	}
	if (a.Blob == nil) != (b.Blob == nil) || !bytes.Equal(a.Blob, b.Blob) {
		return false
	}
	if len(a.Arr) != len(b.Arr) {
		return false
	}
	for i := range a.Arr {
		if !eqValue(a.Arr[i], b.Arr[i]) {
			return false
		}
	}
	return true
}

// eqProto is the eqValue analogue for protobuf Values: same NaN tolerance,
// same nil/empty-slice tolerance, exact byte comparison.
func eqProto(a, b *ultimav1.Value) bool {
	if a == nil || b == nil {
		return a == b
	}
	switch ak := a.GetKind().(type) {
	case *ultimav1.Value_Null:
		bk, ok := b.GetKind().(*ultimav1.Value_Null)
		return ok && ak.Null == bk.Null
	case *ultimav1.Value_SimpleString:
		bk, ok := b.GetKind().(*ultimav1.Value_SimpleString)
		return ok && ak.SimpleString == bk.SimpleString
	case *ultimav1.Value_Error:
		bk, ok := b.GetKind().(*ultimav1.Value_Error)
		return ok && ak.Error == bk.Error
	case *ultimav1.Value_Int:
		bk, ok := b.GetKind().(*ultimav1.Value_Int)
		return ok && ak.Int == bk.Int
	case *ultimav1.Value_Double:
		bk, ok := b.GetKind().(*ultimav1.Value_Double)
		if !ok {
			return false
		}
		if math.IsNaN(ak.Double) || math.IsNaN(bk.Double) {
			return math.IsNaN(ak.Double) && math.IsNaN(bk.Double)
		}
		return math.Float64bits(ak.Double) == math.Float64bits(bk.Double)
	case *ultimav1.Value_Bool:
		bk, ok := b.GetKind().(*ultimav1.Value_Bool)
		return ok && ak.Bool == bk.Bool
	case *ultimav1.Value_BlobString:
		bk, ok := b.GetKind().(*ultimav1.Value_BlobString)
		return ok && (ak.BlobString == nil) == (bk.BlobString == nil) &&
			bytes.Equal(ak.BlobString, bk.BlobString)
	case *ultimav1.Value_BigNumber:
		bk, ok := b.GetKind().(*ultimav1.Value_BigNumber)
		return ok && ak.BigNumber == bk.BigNumber
	case *ultimav1.Value_Verbatim:
		bk, ok := b.GetKind().(*ultimav1.Value_Verbatim)
		if !ok {
			return false
		}
		return ak.Verbatim.GetFormat() == bk.Verbatim.GetFormat() &&
			bytes.Equal(ak.Verbatim.GetPayload(), bk.Verbatim.GetPayload())
	case *ultimav1.Value_Array:
		bk, ok := b.GetKind().(*ultimav1.Value_Array)
		return ok && eqProtoSlice(ak.Array.GetElems(), bk.Array.GetElems())
	case *ultimav1.Value_Map:
		bk, ok := b.GetKind().(*ultimav1.Value_Map)
		if !ok || len(ak.Map.GetPairs()) != len(bk.Map.GetPairs()) {
			return false
		}
		for i, p := range ak.Map.GetPairs() {
			q := bk.Map.GetPairs()[i]
			if !eqProto(p.GetKey(), q.GetKey()) || !eqProto(p.GetValue(), q.GetValue()) {
				return false
			}
		}
		return true
	case *ultimav1.Value_Set:
		bk, ok := b.GetKind().(*ultimav1.Value_Set)
		return ok && eqProtoSlice(ak.Set.GetElems(), bk.Set.GetElems())
	case *ultimav1.Value_Push:
		bk, ok := b.GetKind().(*ultimav1.Value_Push)
		return ok && eqProtoSlice(ak.Push.GetElems(), bk.Push.GetElems())
	default:
		// No oneof set: equal only if b is likewise empty.
		return b.GetKind() == nil
	}
}

func eqProtoSlice(a, b []*ultimav1.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !eqProto(a[i], b[i]) {
			return false
		}
	}
	return true
}

// normalizeWire collapses an empty non-nil Verbatim payload to nil, for
// comparisons after a proto.Marshal/Unmarshal cycle: Verbatim.Payload is a
// plain (non-oneof) proto3 bytes field, so marshal drops the empty-vs-nil
// distinction. Both forms render identically on the RESP3 wire
// (=4 fmt:\r\n), so the loss is invisible to clients. BlobString is a
// oneof member and keeps its (significant) empty-vs-nil distinction
// through the wire, so it is not normalized here.
func normalizeWire(v resp.Value) resp.Value {
	if v.Kind == resp.KindVerbatim && len(v.Blob) == 0 {
		v.Blob = nil
	}
	for i := range v.Arr {
		v.Arr[i] = normalizeWire(v.Arr[i])
	}
	return v
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// nestedArr wraps leaf in n levels of arrays.
func nestedArr(leaf resp.Value, n int) resp.Value {
	for i := 0; i < n; i++ {
		leaf = resp.Arr(leaf)
	}
	return leaf
}

// roundTripCorpus is every resp.Value variant the codebase can produce.
// Attributes are absent because resp.Value cannot represent them (see the
// file header).
func roundTripCorpus() []struct {
	name string
	v    resp.Value
} {
	big := make([]byte, 1<<20) // 1 MB blob
	for i := range big {
		big[i] = byte(i * 31)
	}
	return []struct {
		name string
		v    resp.Value
	}{
		{"int zero", resp.Int(0)},
		{"int minus one", resp.Int(-1)},
		{"int max", resp.Int(math.MaxInt64)},
		{"int min", resp.Int(math.MinInt64)},

		{"double zero", resp.Double(0)},
		{"double minus zero", resp.Double(math.Copysign(0, -1))},
		{"double pi", resp.Double(math.Pi)},
		{"double max", resp.Double(math.MaxFloat64)},
		{"double smallest denormal", resp.Double(math.SmallestNonzeroFloat64)},
		{"double +inf", resp.Double(math.Inf(1))},
		{"double -inf", resp.Double(math.Inf(-1))},
		{"double nan", resp.Double(math.NaN())},
		{"double nan payload", resp.Double(math.Float64frombits(0x7ff8000000000042))},

		{"simple string", resp.Simple("OK")},
		{"simple string empty", resp.Simple("")},
		{"error with newline and colon", resp.Err("ERR bad thing: line one\nline two: detail")},
		{"error empty", resp.Err("")},

		{"bool true", resp.Bool(true)},
		{"bool false", resp.Bool(false)},

		{"null", resp.Null()},

		{"blob empty", resp.BlobString([]byte{})},
		{"blob all 256 byte values", resp.BlobString(allBytes())},
		{"blob embedded NULs", resp.BlobString([]byte{'a', 0, 0, 'b', 0})},
		{"blob invalid UTF-8", resp.BlobString([]byte{0xff, 0xfe, 0x80, 'x'})},
		{"blob 1MB", resp.BlobString(big)},

		{"verbatim txt", resp.Value{Kind: resp.KindVerbatim, Fmt: "txt", Blob: []byte("hello world")}},
		{"verbatim mkd", resp.Value{Kind: resp.KindVerbatim, Fmt: "mkd", Blob: []byte("# title\n\nbody")}},
		{"verbatim empty payload", resp.Value{Kind: resp.KindVerbatim, Fmt: "txt", Blob: []byte{}}},

		{"big number beyond int64", resp.Value{Kind: resp.KindBigNumber, Str: "3492890328409238509324850943850943825024385"}},
		{"big number negative", resp.Value{Kind: resp.KindBigNumber, Str: "-99999999999999999999999999999"}},

		{"array empty", resp.Arr()},
		{"array nested 10 deep", nestedArr(resp.BlobStr("bottom"), 10)},
		{"array mixed", resp.Arr(resp.Int(1), resp.Double(2.5), resp.Simple("s"), resp.Null(), resp.Bool(true))},

		{"map string keys", resp.Map(resp.BlobStr("k1"), resp.Int(1), resp.BlobStr("k2"), resp.Int(2))},
		{"map non-string keys", resp.Map(
			resp.Int(42), resp.BlobStr("int key"),
			resp.Double(3.5), resp.BlobStr("double key"),
			resp.Arr(resp.Int(1)), resp.BlobStr("array key"),
		)},
		{"map nested", resp.Map(resp.BlobStr("outer"), resp.Map(resp.BlobStr("inner"), resp.Null()))},
		{"map empty", resp.Map()},

		{"set", resp.Set(resp.BlobStr("a"), resp.Int(2), resp.Double(3.5))},
		{"set empty", resp.Set()},

		{"push pubsub message", resp.Push(resp.BlobStr("message"), resp.BlobStr("chan"), resp.BlobStr("payload"))},
		{"push empty", resp.Push()},
	}
}

func TestValueProtoValueRoundTrip(t *testing.T) {
	for _, tc := range roundTripCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			p := envelope.ToProto(tc.v)
			got := envelope.FromProto(p)
			if !eqValue(tc.v, got) {
				t.Fatalf("FromProto(ToProto(v)) mismatch:\n in: %#v\nout: %#v", tc.v, got)
			}
		})
	}
}

// TestValueProtoWireValueRoundTrip goes one step further than the struct
// round trip: marshal the protobuf mirror to the wire and back before
// FromProto. This catches proto3 normalization (zero-value fields, UTF-8
// validation) that struct copies hide.
func TestValueProtoWireValueRoundTrip(t *testing.T) {
	for _, tc := range roundTripCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := proto.Marshal(envelope.ToProto(tc.v))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var p ultimav1.Value
			if err := proto.Unmarshal(raw, &p); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got := envelope.FromProto(&p)
			if !eqValue(normalizeWire(tc.v), got) {
				t.Fatalf("wire round trip mismatch:\n in: %#v\nout: %#v", tc.v, got)
			}
		})
	}
}

// TestProtoValueProtoRoundTrip is the inverse direction, for representative
// protobuf values where the identity holds. The cases where it does not
// hold are pinned in TestLossyAsymmetries.
func TestProtoValueProtoRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		p    *ultimav1.Value
	}{
		{"null true", &ultimav1.Value{Kind: &ultimav1.Value_Null{Null: true}}},
		{"simple", &ultimav1.Value{Kind: &ultimav1.Value_SimpleString{SimpleString: "OK"}}},
		{"error", &ultimav1.Value{Kind: &ultimav1.Value_Error{Error: "ERR x: y\nz"}}},
		{"int min", &ultimav1.Value{Kind: &ultimav1.Value_Int{Int: math.MinInt64}}},
		{"double -0", &ultimav1.Value{Kind: &ultimav1.Value_Double{Double: math.Copysign(0, -1)}}},
		{"double nan", &ultimav1.Value{Kind: &ultimav1.Value_Double{Double: math.NaN()}}},
		{"double -inf", &ultimav1.Value{Kind: &ultimav1.Value_Double{Double: math.Inf(-1)}}},
		{"bool", &ultimav1.Value{Kind: &ultimav1.Value_Bool{Bool: true}}},
		{"blob binary", &ultimav1.Value{Kind: &ultimav1.Value_BlobString{BlobString: allBytes()}}},
		{"blob empty non-nil", &ultimav1.Value{Kind: &ultimav1.Value_BlobString{BlobString: []byte{}}}},
		{"big number", &ultimav1.Value{Kind: &ultimav1.Value_BigNumber{BigNumber: "3492890328409238509324850943850943825024385"}}},
		{"verbatim", &ultimav1.Value{Kind: &ultimav1.Value_Verbatim{Verbatim: &ultimav1.Verbatim{Format: "mkd", Payload: []byte("# t")}}}},
		{"array nested", &ultimav1.Value{Kind: &ultimav1.Value_Array{Array: &ultimav1.Array{Elems: []*ultimav1.Value{
			{Kind: &ultimav1.Value_Int{Int: 1}},
			{Kind: &ultimav1.Value_Array{Array: &ultimav1.Array{Elems: []*ultimav1.Value{
				{Kind: &ultimav1.Value_Null{Null: true}},
			}}}},
		}}}}},
		{"map with int key", &ultimav1.Value{Kind: &ultimav1.Value_Map{Map: &ultimav1.Map{Pairs: []*ultimav1.Pair{
			{Key: &ultimav1.Value{Kind: &ultimav1.Value_Int{Int: 7}}, Value: &ultimav1.Value{Kind: &ultimav1.Value_BlobString{BlobString: []byte("seven")}}},
		}}}}},
		{"set", &ultimav1.Value{Kind: &ultimav1.Value_Set{Set: &ultimav1.Set{Elems: []*ultimav1.Value{
			{Kind: &ultimav1.Value_BlobString{BlobString: []byte("m")}},
		}}}}},
		{"push", &ultimav1.Value{Kind: &ultimav1.Value_Push{Push: &ultimav1.Push{Elems: []*ultimav1.Value{
			{Kind: &ultimav1.Value_BlobString{BlobString: []byte("message")}},
			{Kind: &ultimav1.Value_BlobString{BlobString: []byte("c")}},
			{Kind: &ultimav1.Value_BlobString{BlobString: []byte("p")}},
		}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := envelope.ToProto(envelope.FromProto(tc.p))
			if !eqProto(tc.p, got) {
				t.Fatalf("ToProto(FromProto(p)) mismatch:\n in: %v\nout: %v", tc.p, got)
			}
		})
	}
}

// TestLossyAsymmetries pins the places where the ToProto/FromProto round
// trip is deliberately (or at least knowingly) NOT the identity. Each case
// asserts the exact current behavior so any change is a conscious review.
func TestLossyAsymmetries(t *testing.T) {
	t.Run("nil blob string collapses to null", func(t *testing.T) {
		// ToProto maps resp.BlobString(nil) — the RESP2 $-1 rendering of a
		// null bulk — onto the protobuf Null, so FromProto cannot tell it
		// apart from a real null. Wire-equivalent under RESP2 ($-1); under
		// RESP3 the original renders $-1 while the round-tripped Null
		// renders _.
		p := envelope.ToProto(resp.BlobString(nil))
		if _, ok := p.GetKind().(*ultimav1.Value_Null); !ok {
			t.Fatalf("ToProto(BlobString(nil)) = %T, want Value_Null", p.GetKind())
		}
		got := envelope.FromProto(p)
		if got.Kind != resp.KindNull {
			t.Fatalf("FromProto = %v, want KindNull", got.Kind)
		}
		resp2 := resp.AppendValue(nil, 2, resp.BlobString(nil))
		if string(resp2) != "$-1\r\n" {
			t.Fatalf("RESP2 rendering of nil blob = %q, want $-1", resp2)
		}
	})

	t.Run("proto nil blob string also collapses to null", func(t *testing.T) {
		// The inverse direction: Value_BlobString{nil} comes back as
		// Value_Null, because FromProto yields Blob nil and ToProto then
		// applies the collapse above.
		p := &ultimav1.Value{Kind: &ultimav1.Value_BlobString{BlobString: nil}}
		got := envelope.ToProto(envelope.FromProto(p))
		if _, ok := got.GetKind().(*ultimav1.Value_Null); !ok {
			t.Fatalf("ToProto(FromProto(nil blob)) = %T, want Value_Null", got.GetKind())
		}
	})

	t.Run("empty proto value becomes explicit null", func(t *testing.T) {
		// A Value with no oneof set maps to resp.Null(); sending it back
		// produces the explicit Value_Null{Null: true}.
		p := &ultimav1.Value{}
		got := envelope.ToProto(envelope.FromProto(p))
		n, ok := got.GetKind().(*ultimav1.Value_Null)
		if !ok || !n.Null {
			t.Fatalf("ToProto(FromProto(empty)) = %v, want explicit Null{true}", got)
		}
	})

	t.Run("proto null false becomes null true", func(t *testing.T) {
		// The bool payload of the Null oneof is not preserved; only the
		// oneof selection is.
		p := &ultimav1.Value{Kind: &ultimav1.Value_Null{Null: false}}
		got := envelope.ToProto(envelope.FromProto(p))
		n, ok := got.GetKind().(*ultimav1.Value_Null)
		if !ok || !n.Null {
			t.Fatalf("ToProto(FromProto(Null{false})) = %v, want Null{true}", got)
		}
	})

	t.Run("odd map drops the dangling key", func(t *testing.T) {
		// A KindMap whose flattened Arr has odd length is malformed; ToProto
		// silently drops the trailing unpaired element rather than
		// panicking. The engine never produces this, but a hand-built Value
		// could.
		v := resp.Map(resp.BlobStr("k"), resp.Int(1), resp.BlobStr("dangling"))
		got := envelope.FromProto(envelope.ToProto(v))
		if got.Kind != resp.KindMap || len(got.Arr) != 2 {
			t.Fatalf("odd map round trip = %#v, want 2-element flat map", got)
		}
	})

	t.Run("nil array slice normalizes to empty", func(t *testing.T) {
		// toProtoSlice turns a nil Arr into a non-nil empty slice; there is
		// no null-array form on this fork's wire (both render *0), so the
		// normalization is cosmetic but observable via reflect.
		got := envelope.FromProto(envelope.ToProto(resp.Arr()))
		if got.Kind != resp.KindArray || len(got.Arr) != 0 || got.Arr == nil {
			t.Fatalf("Arr() round trip = %#v, want non-nil empty array", got)
		}
	})
}

// TestNullVersusEmptyArray: KindNull and the empty array are DISTINCT on
// the wire (_ vs *0 under RESP3) and the distinction must survive the
// bridge in both directions.
func TestNullVersusEmptyArray(t *testing.T) {
	null := envelope.FromProto(envelope.ToProto(resp.Null()))
	if null.Kind != resp.KindNull {
		t.Fatalf("null round trip = %v", null.Kind)
	}
	empty := envelope.FromProto(envelope.ToProto(resp.Arr()))
	if empty.Kind != resp.KindArray {
		t.Fatalf("empty array round trip = %v", empty.Kind)
	}
	wireNull := resp.AppendValue(nil, 3, envelope.FromProto(envelope.ToProto(resp.Null())))
	wireArr := resp.AppendValue(nil, 3, envelope.FromProto(envelope.ToProto(resp.Arr())))
	if bytes.Equal(wireNull, wireArr) {
		t.Fatalf("null and empty array collide on the wire: %q", wireNull)
	}
}

// TestNaNDoubleWireBytesStable pins the exact protobuf wire encoding of a
// NaN double: field 5 (fixed64) → tag 0x29 followed by the 8 little-endian
// bytes of the IEEE-754 bit pattern. NaN payloads survive marshal and
// unmarshal bit-exact.
func TestNaNDoubleWireBytesStable(t *testing.T) {
	nans := []uint64{
		math.Float64bits(math.NaN()), // Go's canonical NaN: 0x7ff8000000000001
		0x7ff8000000000042,           // quiet NaN with payload
		0xfff8000000000001,           // negative NaN
	}
	for _, bits := range nans {
		f := math.Float64frombits(bits)
		if !math.IsNaN(f) {
			t.Fatalf("test bug: %#x is not NaN", bits)
		}
		raw, err := proto.Marshal(envelope.ToProto(resp.Double(f)))
		if err != nil {
			t.Fatalf("marshal NaN %#x: %v", bits, err)
		}
		want := []byte{0x29}
		for i := 0; i < 8; i++ {
			want = append(want, byte(bits>>(8*i)))
		}
		if !bytes.Equal(raw, want) {
			t.Fatalf("NaN %#x wire bytes = %x, want %x", bits, raw, want)
		}
		var p ultimav1.Value
		if err := proto.Unmarshal(raw, &p); err != nil {
			t.Fatalf("unmarshal NaN %#x: %v", bits, err)
		}
		got := envelope.FromProto(&p)
		if got.Kind != resp.KindDouble || math.Float64bits(got.Dbl) != bits {
			t.Fatalf("NaN %#x round trip = %#v", bits, got)
		}
	}
}

// TestDoubleRESP3Rendering checks that non-finite doubles survive the
// bridge and render the way Redis renders them on the RESP3 wire
// (addReplyDouble: inf/-inf/nan).
func TestDoubleRESP3Rendering(t *testing.T) {
	cases := []struct {
		f    float64
		want string
	}{
		{math.Inf(1), ",inf\r\n"},
		{math.Inf(-1), ",-inf\r\n"},
		{math.NaN(), ",nan\r\n"},
	}
	for _, tc := range cases {
		got := resp.AppendValue(nil, 3, envelope.FromProto(envelope.ToProto(resp.Double(tc.f))))
		if string(got) != tc.want {
			t.Fatalf("RESP3 render of %v = %q, want %q", tc.f, got, tc.want)
		}
	}
}

// TestDegenerateInputsNoPanic: command execution must never panic on
// client input. These all return a safe zero value or an error.
func TestDegenerateInputsNoPanic(t *testing.T) {
	if got := envelope.FromProto(nil); got.Kind != resp.KindNull {
		t.Fatalf("FromProto(nil) = %v, want KindNull", got.Kind)
	}
	if got := envelope.FromProto(&ultimav1.Value{}); got.Kind != resp.KindNull {
		t.Fatalf("FromProto(empty) = %v, want KindNull", got.Kind)
	}
	// Unknown ValueKind (not a real RESP3 type) falls into ToProto's
	// default branch.
	if p := envelope.ToProto(resp.Value{Kind: resp.ValueKind(99)}); p.GetKind() == nil {
		t.Fatal("ToProto(ValueKind(99)) returned a kindless Value")
	} else if _, ok := p.GetKind().(*ultimav1.Value_Null); !ok {
		t.Fatalf("ToProto(ValueKind(99)) = %T, want Value_Null", p.GetKind())
	}
	// Zero-value resp.Value is KindNull.
	if p := envelope.ToProto(resp.Value{}); p.GetNull() != true {
		t.Fatalf("ToProto(zero Value) = %v, want Null{true}", p)
	}
	// Composite oneofs with nil inner messages.
	if got := envelope.FromProto(&ultimav1.Value{Kind: &ultimav1.Value_Verbatim{Verbatim: nil}}); got.Kind != resp.KindVerbatim {
		t.Fatalf("FromProto(nil verbatim) = %v, want KindVerbatim", got.Kind)
	}
	if got := envelope.FromProto(&ultimav1.Value{Kind: &ultimav1.Value_Map{Map: nil}}); got.Kind != resp.KindMap {
		t.Fatalf("FromProto(nil map) = %v, want KindMap", got.Kind)
	}
	if got := envelope.FromProto(&ultimav1.Value{Kind: &ultimav1.Value_Array{Array: nil}}); got.Kind != resp.KindArray {
		t.Fatalf("FromProto(nil array) = %v, want KindArray", got.Kind)
	}
	// Nil pair slots inside a map.
	m := &ultimav1.Value{Kind: &ultimav1.Value_Map{Map: &ultimav1.Map{Pairs: []*ultimav1.Pair{nil, {}}}}}
	if got := envelope.FromProto(m); got.Kind != resp.KindMap || len(got.Arr) != 4 {
		t.Fatalf("FromProto(map with nil pairs) = %#v", got)
	}
}

func eqArgv(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if (a[i] == nil) != (b[i] == nil) || !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestArgsFor(t *testing.T) {
	garbage := allBytes()
	invalidUTF8 := []byte{0xff, 0xfe, 0x80}
	nuls := []byte{'k', 0, 0, 'e', 0, 'y'}
	long := bytes.Repeat([]byte("ab"), 1<<17) // 256 KiB

	cases := []struct {
		name string
		cmd  *ultimav1.Command
		want [][]byte
	}{
		{"get", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: []byte("k")}}},
			[][]byte{[]byte("GET"), []byte("k")}},
		{"get empty key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: []byte{}}}},
			[][]byte{[]byte("GET"), {}}},
		{"get nil key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{}}},
			[][]byte{[]byte("GET"), nil}},
		{"get nil inner message", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: nil}},
			[][]byte{[]byte("GET"), nil}},
		{"get binary garbage key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: garbage}}},
			[][]byte{[]byte("GET"), garbage}},
		{"get invalid UTF-8 key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: invalidUTF8}}},
			[][]byte{[]byte("GET"), invalidUTF8}},
		{"get NUL key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: nuls}}},
			[][]byte{[]byte("GET"), nuls}},
		{"get whitespace key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: []byte("  \t ")}}},
			[][]byte{[]byte("GET"), []byte("  \t ")}},
		{"get very long key", &ultimav1.Command{Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{Key: long}}},
			[][]byte{[]byte("GET"), long}},

		{"set plain", &ultimav1.Command{Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{Key: []byte("k"), Value: []byte("v")}}},
			[][]byte{[]byte("SET"), []byte("k"), []byte("v")}},
		{"set all flags", &ultimav1.Command{Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
			Key: []byte("k"), Value: garbage, TtlMs: 1500, Nx: true, Xx: true, Get: true}}},
			[][]byte{[]byte("SET"), []byte("k"), garbage, []byte("PX"), []byte("1500"), []byte("NX"), []byte("XX"), []byte("GET")}},
		{"set zero ttl omitted", &ultimav1.Command{Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{Key: []byte("k"), Value: []byte("v"), TtlMs: 0}}},
			[][]byte{[]byte("SET"), []byte("k"), []byte("v")}},

		{"del none", &ultimav1.Command{Cmd: &ultimav1.Command_Del{Del: &ultimav1.DelCommand{}}},
			[][]byte{[]byte("DEL")}},
		{"del many", &ultimav1.Command{Cmd: &ultimav1.Command_Del{Del: &ultimav1.DelCommand{Keys: [][]byte{[]byte("a"), garbage, nuls}}}},
			[][]byte{[]byte("DEL"), []byte("a"), garbage, nuls}},

		{"incr", &ultimav1.Command{Cmd: &ultimav1.Command_Incr{Incr: &ultimav1.IncrCommand{Key: []byte("n"), Delta: 1}}},
			[][]byte{[]byte("INCRBY"), []byte("n"), []byte("1")}},
		{"incr negative", &ultimav1.Command{Cmd: &ultimav1.Command_Incr{Incr: &ultimav1.IncrCommand{Key: []byte("n"), Delta: -42}}},
			[][]byte{[]byte("INCRBY"), []byte("n"), []byte("-42")}},
		{"incr minint64", &ultimav1.Command{Cmd: &ultimav1.Command_Incr{Incr: &ultimav1.IncrCommand{Key: []byte("n"), Delta: math.MinInt64}}},
			[][]byte{[]byte("INCRBY"), []byte("n"), []byte("-9223372036854775808")}},

		{"incrfloat", &ultimav1.Command{Cmd: &ultimav1.Command_IncrFloat{IncrFloat: &ultimav1.IncrFloatCommand{Key: []byte("n"), Delta: 3.25}}},
			[][]byte{[]byte("INCRBYFLOAT"), []byte("n"), []byte("3.25")}},
		{"incrfloat negative zero", &ultimav1.Command{Cmd: &ultimav1.Command_IncrFloat{IncrFloat: &ultimav1.IncrFloatCommand{Key: []byte("n"), Delta: math.Copysign(0, -1)}}},
			[][]byte{[]byte("INCRBYFLOAT"), []byte("n"), []byte("-0")}},

		{"mget", &ultimav1.Command{Cmd: &ultimav1.Command_Mget{Mget: &ultimav1.MGetCommand{Keys: [][]byte{[]byte("a"), invalidUTF8}}}},
			[][]byte{[]byte("MGET"), []byte("a"), invalidUTF8}},
		{"mset", &ultimav1.Command{Cmd: &ultimav1.Command_Mset{Mset: &ultimav1.MSetCommand{Pairs: []*ultimav1.MSetPair{
			{Key: []byte("k1"), Value: []byte("v1")}, {Key: nuls, Value: garbage}}}}},
			[][]byte{[]byte("MSET"), []byte("k1"), []byte("v1"), nuls, garbage}},

		{"append", &ultimav1.Command{Cmd: &ultimav1.Command_Append{Append: &ultimav1.AppendCommand{Key: []byte("k"), Value: garbage}}},
			[][]byte{[]byte("APPEND"), []byte("k"), garbage}},
		{"exists", &ultimav1.Command{Cmd: &ultimav1.Command_Exists{Exists: &ultimav1.ExistsCommand{Keys: [][]byte{[]byte("a"), []byte("b")}}}},
			[][]byte{[]byte("EXISTS"), []byte("a"), []byte("b")}},
		{"expire maps to pexpire", &ultimav1.Command{Cmd: &ultimav1.Command_Expire{Expire: &ultimav1.ExpireCommand{Key: []byte("k"), TtlMs: 60000}}},
			[][]byte{[]byte("PEXPIRE"), []byte("k"), []byte("60000")}},
		{"ttl maps to pttl", &ultimav1.Command{Cmd: &ultimav1.Command_Ttl{Ttl: &ultimav1.TtlCommand{Key: []byte("k")}}},
			[][]byte{[]byte("PTTL"), []byte("k")}},
		{"persist", &ultimav1.Command{Cmd: &ultimav1.Command_Persist{Persist: &ultimav1.PersistCommand{Key: []byte("k")}}},
			[][]byte{[]byte("PERSIST"), []byte("k")}},

		{"hget", &ultimav1.Command{Cmd: &ultimav1.Command_Hget{Hget: &ultimav1.HGetCommand{Key: []byte("h"), Field: invalidUTF8}}},
			[][]byte{[]byte("HGET"), []byte("h"), invalidUTF8}},
		{"hset", &ultimav1.Command{Cmd: &ultimav1.Command_Hset{Hset: &ultimav1.HSetCommand{Key: []byte("h"), Pairs: []*ultimav1.FieldValue{
			{Field: []byte("f"), Value: garbage}}}}},
			[][]byte{[]byte("HSET"), []byte("h"), []byte("f"), garbage}},
		{"hgetall", &ultimav1.Command{Cmd: &ultimav1.Command_Hgetall{Hgetall: &ultimav1.HGetAllCommand{Key: []byte("h")}}},
			[][]byte{[]byte("HGETALL"), []byte("h")}},
		{"hdel", &ultimav1.Command{Cmd: &ultimav1.Command_Hdel{Hdel: &ultimav1.HDelCommand{Key: []byte("h"), Fields: [][]byte{[]byte("f1"), nuls}}}},
			[][]byte{[]byte("HDEL"), []byte("h"), []byte("f1"), nuls}},
		{"hincrby", &ultimav1.Command{Cmd: &ultimav1.Command_Hincrby{Hincrby: &ultimav1.HIncrByCommand{Key: []byte("h"), Field: []byte("f"), Delta: -7}}},
			[][]byte{[]byte("HINCRBY"), []byte("h"), []byte("f"), []byte("-7")}},

		{"lpush", &ultimav1.Command{Cmd: &ultimav1.Command_Lpush{Lpush: &ultimav1.LPushCommand{Key: []byte("l"), Elems: [][]byte{[]byte("e1"), garbage}}}},
			[][]byte{[]byte("LPUSH"), []byte("l"), []byte("e1"), garbage}},
		{"rpush", &ultimav1.Command{Cmd: &ultimav1.Command_Rpush{Rpush: &ultimav1.RPushCommand{Key: []byte("l"), Elems: [][]byte{nuls}}}},
			[][]byte{[]byte("RPUSH"), []byte("l"), nuls}},
		{"lpop", &ultimav1.Command{Cmd: &ultimav1.Command_Lpop{Lpop: &ultimav1.LPopCommand{Key: []byte("l")}}},
			[][]byte{[]byte("LPOP"), []byte("l")}},
		{"rpop", &ultimav1.Command{Cmd: &ultimav1.Command_Rpop{Rpop: &ultimav1.RPopCommand{Key: []byte("l")}}},
			[][]byte{[]byte("RPOP"), []byte("l")}},
		{"lrange", &ultimav1.Command{Cmd: &ultimav1.Command_Lrange{Lrange: &ultimav1.LRangeCommand{Key: []byte("l"), Start: 0, Stop: -1}}},
			[][]byte{[]byte("LRANGE"), []byte("l"), []byte("0"), []byte("-1")}},
		{"llen", &ultimav1.Command{Cmd: &ultimav1.Command_Llen{Llen: &ultimav1.LLenCommand{Key: []byte("l")}}},
			[][]byte{[]byte("LLEN"), []byte("l")}},

		{"sadd", &ultimav1.Command{Cmd: &ultimav1.Command_Sadd{Sadd: &ultimav1.SAddCommand{Key: []byte("s"), Members: [][]byte{[]byte("m"), garbage}}}},
			[][]byte{[]byte("SADD"), []byte("s"), []byte("m"), garbage}},
		{"srem", &ultimav1.Command{Cmd: &ultimav1.Command_Srem{Srem: &ultimav1.SRemCommand{Key: []byte("s"), Members: [][]byte{[]byte("m")}}}},
			[][]byte{[]byte("SREM"), []byte("s"), []byte("m")}},
		{"smembers", &ultimav1.Command{Cmd: &ultimav1.Command_Smembers{Smembers: &ultimav1.SMembersCommand{Key: []byte("s")}}},
			[][]byte{[]byte("SMEMBERS"), []byte("s")}},
		{"sismember", &ultimav1.Command{Cmd: &ultimav1.Command_Sismember{Sismember: &ultimav1.SIsMemberCommand{Key: []byte("s"), Member: invalidUTF8}}},
			[][]byte{[]byte("SISMEMBER"), []byte("s"), invalidUTF8}},

		{"zadd plain", &ultimav1.Command{Cmd: &ultimav1.Command_Zadd{Zadd: &ultimav1.ZAddCommand{Key: []byte("z"), Members: []*ultimav1.ScoredMember{
			{Score: 1.5, Member: []byte("m")}}}}},
			[][]byte{[]byte("ZADD"), []byte("z"), []byte("1.5"), []byte("m")}},
		{"zadd all flags", &ultimav1.Command{Cmd: &ultimav1.Command_Zadd{Zadd: &ultimav1.ZAddCommand{
			Key: []byte("z"), Nx: true, Xx: true, Gt: true, Lt: true, Ch: true, Incr: true,
			Members: []*ultimav1.ScoredMember{{Score: 2, Member: garbage}}}}},
			[][]byte{[]byte("ZADD"), []byte("z"), []byte("NX"), []byte("XX"), []byte("GT"), []byte("LT"), []byte("CH"), []byte("INCR"), []byte("2"), garbage}},
		{"zscore", &ultimav1.Command{Cmd: &ultimav1.Command_Zscore{Zscore: &ultimav1.ZScoreCommand{Key: []byte("z"), Member: []byte("m")}}},
			[][]byte{[]byte("ZSCORE"), []byte("z"), []byte("m")}},
		{"zrange withscores", &ultimav1.Command{Cmd: &ultimav1.Command_Zrange{Zrange: &ultimav1.ZRangeCommand{Key: []byte("z"), Start: 0, Stop: -1, Withscores: true}}},
			[][]byte{[]byte("ZRANGE"), []byte("z"), []byte("0"), []byte("-1"), []byte("WITHSCORES")}},
		{"zrem", &ultimav1.Command{Cmd: &ultimav1.Command_Zrem{Zrem: &ultimav1.ZRemCommand{Key: []byte("z"), Members: [][]byte{[]byte("m"), nuls}}}},
			[][]byte{[]byte("ZREM"), []byte("z"), []byte("m"), nuls}},
		{"zcard", &ultimav1.Command{Cmd: &ultimav1.Command_Zcard{Zcard: &ultimav1.ZCardCommand{Key: []byte("z")}}},
			[][]byte{[]byte("ZCARD"), []byte("z")}},

		{"generic passthrough", &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
			Command: "CONFIG", Args: [][]byte{[]byte("GET"), []byte("maxmemory")}}}},
			[][]byte{[]byte("CONFIG"), []byte("GET"), []byte("maxmemory")}},
		{"generic binary args", &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
			Command: "SET", Args: [][]byte{garbage, invalidUTF8, nuls, long}}}},
			[][]byte{[]byte("SET"), garbage, invalidUTF8, nuls, long}},
		{"generic no args", &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{Command: "DBSIZE"}}},
			[][]byte{[]byte("DBSIZE")}},
		{"generic name not upper-cased", &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{Command: "get", Args: [][]byte{[]byte("k")}}}},
			[][]byte{[]byte("get"), []byte("k")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := envelope.ArgsFor(tc.cmd)
			if err != nil {
				t.Fatalf("ArgsFor: %v", err)
			}
			if !eqArgv(got, tc.want) {
				t.Fatalf("ArgsFor = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestArgsForErrors(t *testing.T) {
	cases := []struct {
		name    string
		cmd     *ultimav1.Command
		wantErr string
	}{
		{"nil command", nil, "envelope: command has no cmd field set"},
		{"no oneof set", &ultimav1.Command{}, "envelope: command has no cmd field set"},
		{"generic empty name", &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{}}}, "unknown command ''"},
		{"generic nil inner", &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: nil}}, "unknown command ''"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := envelope.ArgsFor(tc.cmd)
			if err == nil {
				t.Fatalf("ArgsFor = %q, want error %q", got, tc.wantErr)
			}
			if err.Error() != tc.wantErr {
				t.Fatalf("ArgsFor error = %q, want %q", err, tc.wantErr)
			}
		})
	}
}

// --- fuzz-lite: deterministic randomized round-trip sweep ---

var fuzzInts = []int64{0, -1, 1, math.MaxInt64, math.MinInt64, 255, -256, 1 << 40}

var fuzzDoubles = []float64{
	0, math.Copysign(0, -1), math.Pi, math.MaxFloat64, math.SmallestNonzeroFloat64,
	math.Inf(1), math.Inf(-1), math.NaN(), -1.5e-300,
}

// fuzzRunes keeps generated strings valid UTF-8: proto3 rejects invalid
// UTF-8 in string fields at marshal time (struct copies don't care, but
// the wire path in this test marshals).
var fuzzRunes = []rune("abcXYZ019 \t\n:é☃\U0001F600\x00")

func fuzzString(rng *rand.Rand) string {
	n := rng.Intn(24)
	r := make([]rune, n)
	for i := range r {
		r[i] = fuzzRunes[rng.Intn(len(fuzzRunes))]
	}
	return string(r)
}

// fuzzBlob always returns a non-nil slice: the nil-blob collapse is a
// documented asymmetry pinned in TestLossyAsymmetries, not fuzz material.
func fuzzBlob(rng *rand.Rand) []byte {
	n := rng.Intn(64)
	if rng.Intn(50) == 0 {
		n = 4096
	}
	b := make([]byte, n)
	if _, err := rng.Read(b); err != nil {
		panic(err)
	}
	return b
}

func fuzzValue(rng *rand.Rand, depth int) resp.Value {
	roll := rng.Intn(100)
	if depth >= 4 {
		roll = rng.Intn(60) // leaves only
	}
	switch {
	case roll < 5:
		return resp.Null()
	case roll < 10:
		return resp.Simple(fuzzString(rng))
	case roll < 15:
		return resp.Err("ERR " + fuzzString(rng))
	case roll < 25:
		return resp.Int(fuzzInts[rng.Intn(len(fuzzInts))])
	case roll < 35:
		if rng.Intn(10) == 0 {
			return resp.Double(math.Float64frombits(rng.Uint64())) // often NaN
		}
		return resp.Double(fuzzDoubles[rng.Intn(len(fuzzDoubles))])
	case roll < 40:
		return resp.Bool(rng.Intn(2) == 0)
	case roll < 50:
		return resp.BlobString(fuzzBlob(rng))
	case roll < 55:
		// Bigger than int64 on purpose.
		return resp.Value{Kind: resp.KindBigNumber, Str: fmt.Sprintf("%d%09d", rng.Int63(), rng.Intn(1e9))}
	case roll < 60:
		fmts := []string{"txt", "mkd", "raw"}
		return resp.Value{Kind: resp.KindVerbatim, Fmt: fmts[rng.Intn(len(fmts))], Blob: fuzzBlob(rng)}
	default:
		n := rng.Intn(5)
		elems := make([]resp.Value, n)
		for i := range elems {
			elems[i] = fuzzValue(rng, depth+1)
		}
		switch rng.Intn(4) {
		case 0:
			return resp.Arr(elems...)
		case 1: // map: keys may be any type, like RESP3 allows
			keys := make([]resp.Value, n)
			for i := range keys {
				keys[i] = fuzzValue(rng, depth+1)
			}
			flat := make([]resp.Value, 0, 2*n)
			for i := range keys {
				flat = append(flat, keys[i], elems[i])
			}
			return resp.Map(flat...)
		case 2:
			return resp.Set(elems...)
		default:
			return resp.Push(elems...)
		}
	}
}

func TestFuzzRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(20260908)) // deterministic
	for i := 0; i < 3000; i++ {
		v := fuzzValue(rng, 0)

		// Struct round trip.
		if got := envelope.FromProto(envelope.ToProto(v)); !eqValue(v, got) {
			t.Fatalf("iter %d: struct round trip mismatch:\n in: %#v\nout: %#v", i, v, got)
		}

		// Wire round trip (marshal → unmarshal → FromProto → ToProto).
		p := envelope.ToProto(v)
		raw, err := proto.Marshal(p)
		if err != nil {
			t.Fatalf("iter %d: marshal: %v (value %#v)", i, err, v)
		}
		var back ultimav1.Value
		if err := proto.Unmarshal(raw, &back); err != nil {
			t.Fatalf("iter %d: unmarshal: %v", i, err)
		}
		if !eqProto(p, &back) {
			t.Fatalf("iter %d: proto wire mismatch:\n in: %v\nout: %v", i, p, &back)
		}
		if got := envelope.FromProto(&back); !eqValue(normalizeWire(v), got) {
			t.Fatalf("iter %d: wire round trip mismatch:\n in: %#v\nout: %#v", i, v, got)
		}
	}
}
