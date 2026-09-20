package commands

import (
	"strings"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
)

// pushCollector builds a ConnState whose StartPush funnel appends every
// pushed value to *got.
func pushCollector(e *Engine, addr string, got *[]resp.Value) *ConnState {
	cs := e.NewConnState(addr)
	cs.StartPush = func() func(resp.Value) {
		return func(v resp.Value) { *got = append(*got, v) }
	}
	return cs
}

func TestMonitorStreamsOtherConns(t *testing.T) {
	e, _ := newTestEngine(t)
	var got []resp.Value
	mon := pushCollector(e, "127.0.0.1:9001", &got)
	defer e.CloseConn(mon)
	other := e.NewConnState("127.0.0.1:9002")
	defer e.CloseConn(other)

	v := run(t, e, mon, "MONITOR")
	if v.Kind != resp.KindSimpleString || v.Str != "OK" {
		t.Fatalf("MONITOR reply = %v, want +OK", v)
	}

	run(t, e, other, "SET", "mon:k", `v"x`)
	if len(got) != 1 {
		t.Fatalf("monitor got %d pushes, want 1", len(got))
	}
	line := got[0].Str
	if got[0].Kind != resp.KindSimpleString {
		t.Fatalf("push kind = %v, want simple string", got[0].Kind)
	}
	if !strings.Contains(line, `[0 127.0.0.1:9002] "SET" "mon:k" "v\"x"`) {
		t.Errorf("monitor line %q missing Redis-format command", line)
	}

	// The monitoring connection's own commands are excluded from its feed.
	run(t, e, mon, "PING")
	if len(got) != 1 {
		t.Fatalf("monitor echoed its own command: %d pushes, want 1", len(got))
	}

	// Second MONITOR is idempotent: still one feed.
	run(t, e, mon, "MONITOR")
	run(t, e, other, "SET", "mon:k", "2")
	if len(got) != 2 {
		t.Fatalf("after second MONITOR got %d pushes, want 2 (one feed)", len(got))
	}
}

func TestMonitorRedactionAndEscapes(t *testing.T) {
	e, _ := newTestEngine(t)
	var got []resp.Value
	mon := pushCollector(e, "127.0.0.1:9001", &got)
	defer e.CloseConn(mon)
	other := e.NewConnState("127.0.0.1:9002")
	defer e.CloseConn(other)
	other.Authed = true

	run(t, e, mon, "MONITOR")
	// AUTH collapses to just its name (M4 monitorArgs convention).
	cs2 := e.NewConnState("127.0.0.1:9003")
	defer e.CloseConn(cs2)
	run(t, e, cs2, "AUTH", "s3cret")
	run(t, e, other, "LPUSH", "mon:l", "a\tb")
	if len(got) != 2 {
		t.Fatalf("got %d pushes, want 2", len(got))
	}
	if !strings.HasSuffix(got[0].Str, `] "auth"`) {
		t.Errorf("AUTH event = %q, want bare \"auth\" (redacted)", got[0].Str)
	}
	if !strings.HasSuffix(got[1].Str, `"LPUSH" "mon:l" "a\tb"`) {
		t.Errorf("LPUSH event = %q, want tab escaped as \\t", got[1].Str)
	}
}

func TestMonitorCloseConnDetaches(t *testing.T) {
	e, _ := newTestEngine(t)
	var got []resp.Value
	mon := pushCollector(e, "127.0.0.1:9001", &got)
	other := e.NewConnState("127.0.0.1:9002")
	defer e.CloseConn(other)

	run(t, e, mon, "MONITOR")
	if n := e.monitorN.Load(); n != 1 {
		t.Fatalf("monitorN = %d, want 1", n)
	}
	e.CloseConn(mon)
	if n := e.monitorN.Load(); n != 0 {
		t.Fatalf("after CloseConn monitorN = %d, want 0", n)
	}
	run(t, e, other, "SET", "mon:k", "v")
	if len(got) != 0 {
		t.Fatalf("detached monitor still got %d pushes", len(got))
	}
}

func TestMonitorRequiresPushFunnel(t *testing.T) {
	e, cs := newTestEngine(t) // no StartPush: synthetic-style connection
	defer e.CloseConn(cs)
	v := run(t, e, cs, "MONITOR")
	if v.Kind != resp.KindError {
		t.Fatalf("MONITOR without push funnel = %v, want error", v)
	}
}
