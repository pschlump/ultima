package commands

// Tests for the M6c introspection layer (§10.1): client registry/kill,
// slowlog ring, latency stats.

import (
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/resp"
)

func TestClientRegistry(t *testing.T) {
	e, cs := newTestEngine(t)

	clients := e.ListClients()
	if len(clients) != 1 {
		t.Fatalf("ListClients = %d entries, want 1", len(clients))
	}
	c := clients[0]
	if c.ID != cs.ID || c.Addr != "127.0.0.1:1" {
		t.Errorf("client = %+v, want id %d addr 127.0.0.1:1", c, cs.ID)
	}
	if c.Surface != "" {
		t.Errorf("surface = %q, want unset", c.Surface)
	}

	run(t, e, cs, "SET", "k", "v")
	run(t, e, cs, "SELECT", "3")
	clients = e.ListClients()
	if got := clients[0].LastCommand; got != "select" {
		t.Errorf("last command = %q, want select", got)
	}
	if got := clients[0].DB; got != 3 {
		t.Errorf("db = %d, want 3 (post-command snapshot)", got)
	}

	e.CloseConn(cs)
	if clients := e.ListClients(); len(clients) != 0 {
		t.Errorf("ListClients after CloseConn = %d entries, want 0", len(clients))
	}
}

func TestKillClient(t *testing.T) {
	e, cs := newTestEngine(t)

	if found, _ := e.KillClient(9999); found {
		t.Error("KillClient(9999) found a nonexistent client")
	}
	if _, killable := e.KillClient(cs.ID); killable {
		t.Error("KillClient without a hook reported killable")
	}

	killed := make(chan struct{}, 1)
	cs.SetKillFunc(func() { killed <- struct{}{} })
	found, killable := e.KillClient(cs.ID)
	if !found || !killable {
		t.Fatalf("KillClient = (%v, %v), want (true, true)", found, killable)
	}
	select {
	case <-killed:
	case <-time.After(time.Second):
		t.Fatal("kill hook was not invoked")
	}
}

func TestSlowlog(t *testing.T) {
	e, cs := newTestEngine(t)

	// Default threshold 10ms: fast commands are not logged.
	run(t, e, cs, "SET", "k", "v")
	if n := e.SlowlogLen(); n != 0 {
		t.Fatalf("slowlog len = %d, want 0 below the threshold", n)
	}

	// Threshold 0 logs everything; entries are newest-first.
	run(t, e, cs, "CONFIG", "SET", "slowlog-log-slower-than", "0")
	run(t, e, cs, "SET", "k2", "v2")
	run(t, e, cs, "GET", "k2")
	entries := e.Slowlog(0)
	if len(entries) < 3 {
		t.Fatalf("slowlog = %d entries, want >= 3 with threshold 0", len(entries))
	}
	if entries[0].Args[0] != "GET" {
		t.Errorf("newest entry = %v, want GET first (newest-first order)", entries[0].Args)
	}
	for _, en := range entries {
		if en.ClientAddr != "127.0.0.1:1" || en.Timestamp == 0 {
			t.Errorf("entry %+v missing client addr or timestamp", en)
		}
	}

	// slowlog-max-len trims the oldest.
	run(t, e, cs, "CONFIG", "SET", "slowlog-max-len", "2")
	run(t, e, cs, "SET", "k3", "v3")
	if n := e.SlowlogLen(); n != 2 {
		t.Errorf("slowlog len = %d, want 2 after max-len shrink", n)
	}

	// -1 disables logging.
	run(t, e, cs, "CONFIG", "SET", "slowlog-log-slower-than", "-1")
	run(t, e, cs, "SET", "k4", "v4")
	if n := e.SlowlogLen(); n != 2 {
		t.Errorf("slowlog len = %d, want 2 (disabled)", n)
	}

	e.SlowlogReset()
	if n := e.SlowlogLen(); n != 0 {
		t.Errorf("slowlog len after reset = %d, want 0", n)
	}
}

func TestSlowlogConfigByteExact(t *testing.T) {
	e, cs := newTestEngine(t)

	if got := e.SlowlogSlowerThan(); got != 10000 {
		t.Errorf("slowlog-log-slower-than default = %d, want 10000", got)
	}
	v := run(t, e, cs, "CONFIG", "SET", "slowlog-log-slower-than", "abc")
	want := "ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - argument couldn't be parsed into an integer"
	if v.Kind != resp.KindError || v.Str != want {
		t.Errorf("bad-integer error = %q, want %q", v.Str, want)
	}
	v = run(t, e, cs, "CONFIG", "SET", "slowlog-max-len", "-1")
	want = "ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument must be between 0 and 9223372036854775807 inclusive"
	if v.Kind != resp.KindError || v.Str != want {
		t.Errorf("range error = %q, want %q", v.Str, want)
	}
}

func TestLatencyStats(t *testing.T) {
	e, cs := newTestEngine(t)

	run(t, e, cs, "SET", "k", "v")
	run(t, e, cs, "GET", "k")
	run(t, e, cs, "GET", "k")

	var get *CommandLatency
	for _, cl := range e.LatencyStats() {
		if cl.Command == "get" {
			cl := cl
			get = &cl
		}
	}
	if get == nil {
		t.Fatal("no latency stats for get")
	}
	if get.Count != 2 || get.AvgUs != get.TotalUs/2 {
		t.Errorf("get latency = %+v, want count 2 with avg = total/2", *get)
	}
}
