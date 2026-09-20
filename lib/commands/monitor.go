package commands

import (
	"fmt"
	"strings"

	"github.com/pschlump/ultima/lib/resp"
)

// MONITOR (design doc §6.2; wired onto RESP/WS for M6d, §10.2's live
// monitor screen). Semantics probed against Redis 7.2.7: the reply is
// +OK, then every command executed on OTHER connections streams in as a
// simple string `"<unix>.<micros> [<db> <addr>] "cmd" "arg" ..."` with
// args escaped the way sdscatrepr does (C-style, \xNN for the rest).
// Delivery rides the same per-connection push funnel as pub/sub
// (cs.ensureDeliver), so on /ws/v1 events arrive as unsolicited seq-0
// frames and on RESP they come through the respserver push queue with
// its slow-consumer rule.
//
// Documented divergences from 7.2.7:
//   - A monitoring connection may still issue commands (7.2.7 answers
//     them with "ERR Replica can't interact with the keyspace"); its own
//     commands are simply excluded from its feed.
//   - Credential redaction follows the M4 monitorArgs convention
//     (AUTH collapses to just "auth", HELLO keeps only the protocol
//     version) rather than 7.2.7's "(redacted)" placeholder args.

// cmdMonitor registers this connection as a monitor. A second MONITOR on
// the same connection is idempotent (still one feed, +OK again).
func cmdMonitor(e *Engine, cs *ConnState, _ [][]byte) resp.Value {
	if cs.monitorCancel != nil {
		return resp.Simple("OK")
	}
	deliver := cs.ensureDeliver()
	if deliver == nil {
		return resp.Err("ERR MONITOR is not supported on this connection")
	}
	id := cs.ID
	cs.monitorCancel = e.AddMonitor(func(ev MonitorEvent) {
		if ev.ID == id {
			return
		}
		deliver(resp.Simple(formatMonitorEvent(ev)))
	})
	return resp.Simple("OK")
}

// formatMonitorEvent renders one event in Redis's MONITOR line format
// (without the leading '+' — resp.Simple adds it on the wire).
func formatMonitorEvent(ev MonitorEvent) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d.%06d [%d %s]", ev.When.Unix(), ev.When.Nanosecond()/1000, ev.DB, ev.Addr)
	for _, a := range ev.Args {
		sb.WriteString(` "`)
		sb.WriteString(monitorEscape(a))
		sb.WriteByte('"')
	}
	return sb.String()
}

// monitorEscape escapes an arg the way Redis's sdscatrepr does for
// MONITOR output: C-style for the usual suspects, \xNN for other
// non-printable or non-ASCII bytes.
func monitorEscape(a []byte) string {
	var sb strings.Builder
	for _, c := range a {
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
		case '\a':
			sb.WriteString(`\a`)
		case '\b':
			sb.WriteString(`\b`)
		default:
			if c >= 32 && c < 127 {
				sb.WriteByte(c)
			} else {
				fmt.Fprintf(&sb, `\x%02x`, c)
			}
		}
	}
	return sb.String()
}
