package resp

import (
	"math"
	"strconv"
	"strings"
)

// RESP3 appenders and Writer methods — the fork's addition over upstream
// redcon, which is RESP2-only (design doc §6.1 modification #1).

// AppendMap appends a RESP3 map header (%n).
func AppendMap(b []byte, n int) []byte {
	return appendPrefix(b, '%', int64(n))
}

// AppendSet appends a RESP3 set header (~n).
func AppendSet(b []byte, n int) []byte {
	return appendPrefix(b, '~', int64(n))
}

// AppendPush appends a RESP3 push header (>n).
func AppendPush(b []byte, n int) []byte {
	return appendPrefix(b, '>', int64(n))
}

// AppendDouble appends a RESP3 double (,x). Non-finite values render as
// inf/-inf/nan, matching Redis addReplyDouble.
func AppendDouble(b []byte, f float64) []byte {
	return append(append(b, ','), appendDouble(f)...)
}

// AppendBigNumber appends a RESP3 big number ((n).
func AppendBigNumber(b []byte, s string) []byte {
	b = append(b, '(')
	b = append(b, s...)
	return append(b, '\r', '\n')
}

// AppendBool appends a RESP3 boolean (#t / #f).
func AppendBool(b []byte, v bool) []byte {
	if v {
		return append(b, '#', 't', '\r', '\n')
	}
	return append(b, '#', 'f', '\r', '\n')
}

// AppendNull3 appends a RESP3 null (_).
func AppendNull3(b []byte) []byte {
	return append(b, '_', '\r', '\n')
}

// AppendVerbatim appends a RESP3 verbatim string (=len txt:data).
func AppendVerbatim(b []byte, format, data string) []byte {
	b = appendPrefix(b, '=', int64(len(data)+4))
	b = append(b, format...)
	b = append(b, ':')
	b = append(b, data...)
	return append(b, '\r', '\n')
}

// appendDouble renders f the way Redis does for RESP3 doubles and their
// RESP2 bulk-string downgrade (Redis d2string): shortest round-trip %g
// with a minimal-width exponent and inf/-inf/nan spelled out.
func appendDouble(f float64) []byte {
	return append([]byte(FormatDouble(f)), '\r', '\n')
}

// FormatDouble formats f exactly like Redis's d2string (7.2), used for
// every score/double reply in both protocols: "0" for both zeros, inf/
// -inf/nan spelled out, integral int64-fitting values printed as
// integers, everything else via the same rules as fpconv_dtoa (shortest
// round-trip digits, fixed notation when the exponent is small, else
// scientific with a minimal-width signed exponent like "1e-7").
func FormatDouble(f float64) string {
	switch {
	case f == 0:
		return "0"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	if f >= -9e18 && f <= 9e18 && f == math.Trunc(f) {
		if v := int64(f); float64(v) == f {
			return strconv.FormatInt(v, 10)
		}
	}
	// Shortest round-trip digits via %e: "-d.ddddde±X".
	s := strconv.FormatFloat(f, 'e', -1, 64)
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	ei := strings.IndexByte(s, 'e')
	mant := s[:ei]
	exp, _ := strconv.Atoi(s[ei+1:]) // X: value = 0.digits * 10^(X+1)
	digits := mant
	if di := strings.IndexByte(mant, '.'); di >= 0 {
		digits = mant[:di] + mant[di+1:] // strip the dot
	}
	K := exp - len(digits) + 1 // value = digits * 10^K
	absExp := exp
	if absExp < 0 {
		absExp = -absExp
	}
	var out string
	switch {
	case K >= 0 && absExp < len(digits)+7:
		// plain integer
		out = digits + strings.Repeat("0", K)
	case K < 0 && (K > -7 || absExp < 4):
		// fixed notation
		offset := len(digits) + K
		if offset <= 0 {
			out = "0." + strings.Repeat("0", -offset) + digits
		} else {
			out = digits[:offset] + "." + digits[offset:]
		}
	default:
		// scientific: d[.ddd]e±exp, exponent with minimal digits
		out = digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		out += "e"
		if exp < 0 {
			out += "-"
		} else {
			out += "+"
		}
		out += strconv.Itoa(absExp)
	}
	if neg {
		return "-" + out
	}
	return out
}

// WriteMap writes a RESP3 map header; write 2*count more values after it.
func (w *Writer) WriteMap(count int) {
	if w.err != nil {
		return
	}
	w.b = AppendMap(w.b, count)
}

// WriteSet writes a RESP3 set header; write count more values after it.
func (w *Writer) WriteSet(count int) {
	if w.err != nil {
		return
	}
	w.b = AppendSet(w.b, count)
}

// WritePush writes a RESP3 push header; write count more values after it.
func (w *Writer) WritePush(count int) {
	if w.err != nil {
		return
	}
	w.b = AppendPush(w.b, count)
}

// WriteDouble writes a RESP3 double.
func (w *Writer) WriteDouble(f float64) {
	if w.err != nil {
		return
	}
	w.b = AppendDouble(w.b, f)
}

// WriteBigNumber writes a RESP3 big number from its decimal string.
func (w *Writer) WriteBigNumber(s string) {
	if w.err != nil {
		return
	}
	w.b = AppendBigNumber(w.b, s)
}

// WriteBool writes a RESP3 boolean.
func (w *Writer) WriteBool(v bool) {
	if w.err != nil {
		return
	}
	w.b = AppendBool(w.b, v)
}

// WriteNull3 writes a RESP3 null (_).
func (w *Writer) WriteNull3() {
	if w.err != nil {
		return
	}
	w.b = AppendNull3(w.b)
}

// WriteVerbatim writes a RESP3 verbatim string with a 3-byte format tag
// ("txt" or "mkd").
func (w *Writer) WriteVerbatim(format, data string) {
	if w.err != nil {
		return
	}
	w.b = AppendVerbatim(w.b, format, data)
}
