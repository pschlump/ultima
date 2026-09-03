package resp

import (
	"testing"
)

func render(t *testing.T, proto int, v Value) string {
	t.Helper()
	var b []byte
	b = AppendValue(b, proto, v)
	return string(b)
}

func TestAppendValueRESP3(t *testing.T) {
	cases := []struct {
		name string
		v    Value
		want string
	}{
		{"simple", Simple("OK"), "+OK\r\n"},
		{"error", Err("ERR bad"), "-ERR bad\r\n"},
		{"int", Int(42), ":42\r\n"},
		{"null3", Null(), "_\r\n"},
		{"blob", BlobString([]byte("hi")), "$2\r\nhi\r\n"},
		{"double", Double(3.14), ",3.14\r\n"},
		{"double-int", Double(3), ",3\r\n"},
		{"bool-t", Bool(true), "#t\r\n"},
		{"bool-f", Bool(false), "#f\r\n"},
		{"bignumber", Value{Kind: KindBigNumber, Str: "3492890328409238509324850943850943825024385"},
			"(3492890328409238509324850943850943825024385\r\n"},
		{"verbatim", Value{Kind: KindVerbatim, Fmt: "txt", Blob: []byte("abc")}, "=7\r\ntxt:abc\r\n"},
		{"array", Arr(BlobStr("a"), Int(1)), "*2\r\n$1\r\na\r\n:1\r\n"},
		{"map", Map(BlobStr("k"), Int(1), BlobStr("j"), Null()),
			"%2\r\n$1\r\nk\r\n:1\r\n$1\r\nj\r\n_\r\n"},
		{"set", Set(Int(1), Int(2)), "~2\r\n:1\r\n:2\r\n"},
		{"push", Push_(BlobStr("msg")), ">1\r\n$3\r\nmsg\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := render(t, 3, tc.v); got != tc.want {
				t.Errorf("render = %q, want %q", got, tc.want)
			}
		})
	}
}

// Push_ is a push value (RESP3 >) used by pub/sub in later milestones.
func Push_(elems ...Value) Value { return Value{Kind: KindPush, Arr: elems} }

func TestAppendValueRESP2Downgrade(t *testing.T) {
	cases := []struct {
		name string
		v    Value
		want string
	}{
		{"null2", Null(), "$-1\r\n"},
		{"double", Double(3), "$1\r\n3\r\n"},
		{"bool-t", Bool(true), ":1\r\n"},
		{"bool-f", Bool(false), ":0\r\n"},
		{"bignumber", Value{Kind: KindBigNumber, Str: "123"}, "$3\r\n123\r\n"},
		{"verbatim", Value{Kind: KindVerbatim, Fmt: "txt", Blob: []byte("abc")}, "$3\r\nabc\r\n"},
		{"map-flat", Map(BlobStr("k"), Int(1), BlobStr("j"), Null()),
			"*4\r\n$1\r\nk\r\n:1\r\n$1\r\nj\r\n$-1\r\n"},
		{"set-array", Set(Int(1), Int(2)), "*2\r\n:1\r\n:2\r\n"},
		{"push-array", Push_(BlobStr("msg")), "*1\r\n$3\r\nmsg\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := render(t, 2, tc.v); got != tc.want {
				t.Errorf("render = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDoubleFormatMatchesRedis(t *testing.T) {
	// Redis d2string (7.2): shortest round-trip %g, integer fast path,
	// minimal-width exponent; inf/-inf/nan spelled out.
	cases := map[float64]string{
		1.5:    "1.5",
		0.1:    "0.1",
		3:      "3",
		0:      "0",
		1e17:   "100000000000000000",
		1e21:   "1e+21",
		1e-5:   "0.00001",
		1e-7:   "1e-7",
		100000: "100000",
	}
	for f, want := range cases {
		if got := FormatDouble(f); got != want {
			t.Errorf("FormatDouble(%v) = %q, want %q", f, got, want)
		}
	}
	if got := render(t, 3, Double(1.5)); got != ",1.5\r\n" {
		t.Errorf("1.5 = %q", got)
	}
	if got := render(t, 3, Double(0.1)); got != ",0.1\r\n" {
		t.Errorf("0.1 = %q", got)
	}
}
