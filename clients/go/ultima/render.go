// Package ultima redis-cli-style reply rendering (§6.4), shared by the three CLIs:
// integers as "(integer) N", null as "(nil)", error replies as
// "(error) …", arrays/sets/pushes numbered "1) …" with nested aggregates
// indented under the parent's prefix, RESP3 maps as "1# k => v" pairs.
package ultima

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/pschlump/ultima/lib/resp"
)

// Format renders one reply value the way redis-cli would print it
// (without the trailing newline).
func Format(v resp.Value) string {
	var sb strings.Builder
	renderValue(&sb, v, "")
	return sb.String()
}

// Fprint writes Format(v) plus a newline to w; a terminal rendering
// failure (broken pipe) is not actionable for the CLIs.
func Fprint(w io.Writer, v resp.Value) {
	_, _ = fmt.Fprintln(w, Format(v))
}

// renderValue appends v's rendering; indent prefixes continuation lines
// of nested aggregates (the redis-cli alignment: a nested element lines
// up under its parent's content).
func renderValue(sb *strings.Builder, v resp.Value, indent string) {
	switch v.Kind {
	case resp.KindArray, resp.KindSet, resp.KindPush:
		if len(v.Arr) == 0 {
			if v.Kind == resp.KindSet {
				sb.WriteString("(empty set)")
			} else {
				sb.WriteString("(empty array)")
			}
			return
		}
		for i, e := range v.Arr {
			head := fmt.Sprintf("%d) ", i+1)
			if i > 0 {
				sb.WriteString("\n" + indent)
			}
			sb.WriteString(head)
			renderValue(sb, e, indent+strings.Repeat(" ", len(head)))
		}
	case resp.KindMap:
		if len(v.Arr) == 0 {
			sb.WriteString("(empty map)")
			return
		}
		for i := 0; i+1 < len(v.Arr); i += 2 {
			head := fmt.Sprintf("%d# ", i/2+1)
			if i > 0 {
				sb.WriteString("\n" + indent)
			}
			sb.WriteString(head)
			renderValue(sb, v.Arr[i], indent+strings.Repeat(" ", len(head)))
			sb.WriteString(" => ")
			renderValue(sb, v.Arr[i+1], indent+strings.Repeat(" ", len(head)))
		}
	default:
		sb.WriteString(scalarText(v))
	}
}

// scalarText renders the non-aggregate kinds.
func scalarText(v resp.Value) string {
	switch v.Kind {
	case resp.KindSimpleString:
		return v.Str
	case resp.KindError:
		return "(error) " + v.Str
	case resp.KindInt:
		return fmt.Sprintf("(integer) %d", v.Int)
	case resp.KindBlobString:
		if v.Blob == nil {
			return "(nil)"
		}
		return `"` + escapeBlob(v.Blob) + `"`
	case resp.KindNull:
		return "(nil)"
	case resp.KindDouble:
		return "(double) " + formatDouble(v.Dbl)
	case resp.KindBool:
		if v.Bool {
			return "(true)"
		}
		return "(false)"
	case resp.KindBigNumber:
		return "(big number) " + v.Str
	case resp.KindVerbatim:
		return string(v.Blob)
	}
	return "(nil)"
}

// formatDouble renders RESP3 doubles the way redis-cli does (inf/nan
// words, shortest round-trip otherwise).
func formatDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// escapeBlob quotes a bulk string the way redis-cli does: printable bytes
// verbatim, the usual C escapes, everything else as \xNN.
func escapeBlob(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		switch c {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if c >= 0x20 && c < 0x7f {
				sb.WriteByte(c)
			} else {
				fmt.Fprintf(&sb, `\x%02x`, c)
			}
		}
	}
	return sb.String()
}
