package resp

import "strconv"

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
// RESP2 bulk-string downgrade: %.17g, with inf/-inf/nan spelled out.
func appendDouble(f float64) []byte {
	s := strconv.FormatFloat(f, 'g', 17, 64)
	if s == "+Inf" {
		s = "inf"
	} else if s == "-Inf" {
		s = "-inf"
	} else if s == "NaN" {
		s = "nan"
	}
	return append([]byte(s), '\r', '\n')
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
