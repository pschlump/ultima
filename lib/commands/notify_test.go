package commands

import (
	"sync"
	"testing"

	"github.com/pschlump/ultima/lib/pubsub"
	"github.com/pschlump/ultima/lib/resp"
)

func TestParseNotifyFlags(t *testing.T) {
	valid := []struct {
		in   string
		want uint64
	}{
		{"", 0},
		{"K", nfKeyspace},
		{"E", nfKeyevent},
		{"g", nfGeneric},
		{"$", nfString},
		{"l", nfList},
		{"s", nfSet},
		{"h", nfHash},
		{"z", nfZset},
		{"x", nfExpired},
		{"e", nfEvicted},
		{"t", nfStream},
		{"m", nfKeyMiss},
		{"d", nfModule},
		{"n", nfNew},
		{"A", nfAll},
		{"KE", nfKeyspace | nfKeyevent},
		{"gx", nfGeneric | nfExpired},
		{"KEA", nfKeyspace | nfKeyevent | nfAll},
		{"tmdn", nfStream | nfKeyMiss | nfModule | nfNew},
		{"g$lshzxeKEtmdn", nfGeneric | nfString | nfList | nfSet | nfHash |
			nfZset | nfExpired | nfEvicted | nfKeyspace | nfKeyevent |
			nfStream | nfKeyMiss | nfModule | nfNew},
		{"KKgg", nfKeyspace | nfGeneric}, // repeats are harmless
	}
	for _, tc := range valid {
		if f, ok := parseNotifyFlags(tc.in); !ok || f != tc.want {
			t.Errorf("parseNotifyFlags(%q) = %#x,%v want %#x,true", tc.in, f, ok, tc.want)
		}
	}
	for _, s := range []string{"B", "q", "X", "Y", "Z", "1", " ", "K B", "AKE!", "-"} {
		if f, ok := parseNotifyFlags(s); ok {
			t.Errorf("parseNotifyFlags(%q) = %#x,true want failure", s, f)
		}
	}
}

// TestNotifyKeyspaceGating drives notifyKeyspace against a real broker with
// a capturing subscriber and checks the K/E channel split, the per-class
// gate, the off default, and the keyspace-before-keyevent publish order
// (Redis notify.c).
func TestNotifyKeyspaceGating(t *testing.T) {
	e, _ := newTestEngine(t)
	var mu sync.Mutex
	var frames []resp.Value
	sub := &pubsub.Sub{ID: 99, Deliver: func(v resp.Value) {
		mu.Lock()
		frames = append(frames, v)
		mu.Unlock()
	}}
	e.PubSub.PSubscribe("__keyevent@0__:*", sub)
	e.PubSub.PSubscribe("__keyspace@0__:*", sub)
	e.PubSub.PSubscribe("__keyevent@3__:*", sub) // for the db-routing check

	reset := func(flags string) {
		t.Helper()
		if !e.SetNotifyKeyspaceEvents(flags) {
			t.Fatalf("SetNotifyKeyspaceEvents(%q) rejected", flags)
		}
		mu.Lock()
		frames = nil
		mu.Unlock()
	}
	got := func() []resp.Value {
		mu.Lock()
		defer mu.Unlock()
		return append([]resp.Value(nil), frames...)
	}
	// frameSummary renders a push as channel|payload for compact asserts.
	summary := func(v resp.Value) string {
		if v.Kind != resp.KindPush || len(v.Arr) != 4 {
			t.Fatalf("not a pmessage push: %+v", v)
		}
		return string(v.Arr[2].Blob) + "|" + string(v.Arr[3].Blob)
	}

	// Off by default: nothing is delivered.
	if got := func() []resp.Value { e.notifyKeyspace(0, "k", "set"); return got() }(); len(got) != 0 {
		t.Fatalf("notifications off: got %d frames", len(got))
	}

	// KEA: keyspace first (payload = event), then keyevent (payload = key).
	// (Bare "KE" enables the channels but no event class — nothing fires,
	// exactly as in Redis.)
	reset("KEA")
	e.notifyKeyspace(0, "k", "set")
	fs := got()
	if len(fs) != 2 {
		t.Fatalf("KEA: got %d frames, want 2", len(fs))
	}
	if s := summary(fs[0]); s != "__keyspace@0__:k|set" {
		t.Errorf("frame 0 = %q, want keyspace channel with event payload", s)
	}
	if s := summary(fs[1]); s != "__keyevent@0__:set|k" {
		t.Errorf("frame 1 = %q, want keyevent channel with key payload", s)
	}

	// EA: just the keyevent frame.
	reset("EA")
	e.notifyKeyspace(0, "k", "del")
	fs = got()
	if len(fs) != 1 || summary(fs[0]) != "__keyevent@0__:del|k" {
		t.Fatalf("EA: got %v, want one keyevent del frame", fs)
	}

	// KA: just the keyspace frame.
	reset("KA")
	e.notifyKeyspace(0, "k", "persist")
	fs = got()
	if len(fs) != 1 || summary(fs[0]) != "__keyspace@0__:k|persist" {
		t.Fatalf("KA: got %v, want one keyspace persist frame", fs)
	}

	// Class gate: Eg carries del (generic) but drops set (string).
	reset("Eg")
	e.notifyKeyspace(0, "k", "set")
	if fs := got(); len(fs) != 0 {
		t.Fatalf("Eg: string event delivered: %v", fs)
	}
	e.notifyKeyspace(0, "k", "del")
	if fs := got(); len(fs) != 1 {
		t.Fatalf("Eg: generic event dropped")
	}

	// Empty flags: nothing.
	reset("")
	e.notifyKeyspace(0, "k", "del")
	if fs := got(); len(fs) != 0 {
		t.Fatalf("empty flags: got %v", fs)
	}

	// An unknown event name is dropped even with everything on.
	reset("KEA")
	e.notifyKeyspace(0, "k", "nosuchevent")
	if fs := got(); len(fs) != 0 {
		t.Fatalf("unknown event: got %v", fs)
	}

	// DB index routes into the channel name.
	reset("EA")
	e.notifyKeyspace(3, "k", "expire")
	fs = got()
	if len(fs) != 1 || string(fs[0].Arr[2].Blob) != "__keyevent@3__:expire" {
		t.Fatalf("db routing: got %v", fs)
	}
}
