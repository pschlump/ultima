package wssession

// Unit tests for the resumable-session registry (design doc §9.4, D18):
// stamping and buffering, replay on attach with the gap check, retention
// expiry releasing the retained ConnState (broker subscriptions dropped),
// takeover closing the displaced attachment, and the aborted-reply
// round-trip.

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestRegistry(t *testing.T, window time.Duration, maxMsgs int) (*commands.Engine, *Registry) {
	t.Helper()
	shards := shard.NewEngine(4, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	r := NewRegistry(eng, window, maxMsgs, nil)
	t.Cleanup(r.Close)
	return eng, r
}

// collector is a test Sink recording stamped deliveries.
type collector struct {
	mu     sync.Mutex
	seqs   []uint64
	vals   []resp.Value
	closed bool
}

func (c *collector) sink() (Sink, *collector) {
	return Sink{
		Enqueue: func(seq uint64, v resp.Value) bool {
			c.mu.Lock()
			c.seqs = append(c.seqs, seq)
			c.vals = append(c.vals, v)
			c.mu.Unlock()
			return true
		},
		Close: func() {
			c.mu.Lock()
			c.closed = true
			c.mu.Unlock()
		},
		Token: c,
	}, c
}

func (c *collector) got() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.seqs...)
}

func (c *collector) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func push(s string) resp.Value {
	return resp.Push(resp.BlobStr("message"), resp.BlobStr("ch"), resp.BlobStr(s))
}

func TestDeliverStampsBuffersAndSends(t *testing.T) {
	eng, r := newTestRegistry(t, time.Minute, 100)
	s := r.New("alice", eng.NewConnState("t"))
	sn, c := (&collector{}).sink()
	if _, ok := s.Attach(0, sn, nil); !ok {
		t.Fatal("attach failed")
	}
	s.Deliver(push("a"))
	s.Deliver(push("b"))
	s.Deliver(push("c"))
	got := c.got()
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("live seqs = %v, want [1 2 3]", got)
	}
	if n := s.BufferedLen(); n != 3 {
		t.Fatalf("buffered = %d, want 3", n)
	}
}

func TestAttachReplaysAfterDetach(t *testing.T) {
	eng, r := newTestRegistry(t, time.Minute, 100)
	s := r.New("alice", eng.NewConnState("t"))
	sn1, c1 := (&collector{}).sink()
	_, _ = s.Attach(0, sn1, nil)
	s.Deliver(push("a"))
	s.Deliver(push("b"))
	s.Detach(c1)

	// Detached: pushes buffer without a live sink.
	s.Deliver(push("c"))
	s.Deliver(push("d"))
	s.Deliver(push("e"))

	var headRan bool
	sn2, c2 := (&collector{}).sink()
	aborted, ok := s.Attach(2, sn2, func() { headRan = true })
	if !ok {
		t.Fatal("resume attach failed")
	}
	if !headRan {
		t.Fatal("head callback did not run")
	}
	if got := c2.got(); len(got) != 3 || got[0] != 3 || got[1] != 4 || got[2] != 5 {
		t.Fatalf("replayed seqs = %v, want [3 4 5]", got)
	}
	if len(aborted) != 0 {
		t.Fatalf("aborted = %v, want none", aborted)
	}
	// Live delivery resumes on the new sink.
	s.Deliver(push("f"))
	if got := c2.got(); len(got) != 4 || got[3] != 6 {
		t.Fatalf("live after resume = %v, want seq 6 last", got)
	}
}

func TestAttachGap(t *testing.T) {
	eng, r := newTestRegistry(t, time.Minute, 2)
	s := r.New("alice", eng.NewConnState("t"))
	sn1, c1 := (&collector{}).sink()
	_, _ = s.Attach(0, sn1, nil)
	s.Detach(c1)
	for _, m := range []string{"a", "b", "c", "d", "e"} {
		s.Deliver(push(m))
	}
	// Buffer holds seqs 4,5 only: a resume from 0 cannot be satisfied.
	sn2, _ := (&collector{}).sink()
	if _, ok := s.Attach(0, sn2, nil); ok {
		t.Fatal("attach with unbridgeable gap succeeded")
	}
	// A resume inside the retained window works and replays the tail.
	sn3, c3 := (&collector{}).sink()
	if _, ok := s.Attach(4, sn3, nil); !ok {
		t.Fatal("attach within window failed")
	}
	if got := c3.got(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("replayed = %v, want [5]", got)
	}
}

func TestDetachExpiryReleasesConnState(t *testing.T) {
	eng, r := newTestRegistry(t, 80*time.Millisecond, 100)
	cs := eng.NewConnState("t")
	s := r.New("alice", cs)
	cs.StartPush = func() func(resp.Value) { return s.Deliver }
	if v := eng.Execute(cs, [][]byte{[]byte("SUBSCRIBE"), []byte("ch")}); v.Kind != resp.KindPush {
		t.Fatalf("SUBSCRIBE reply = %v", v)
	}
	if n := eng.PubSub.NumChannels(); n != 1 {
		t.Fatalf("channels before detach = %d, want 1", n)
	}
	sn, c := (&collector{}).sink()
	_, _ = s.Attach(0, sn, nil)
	s.Detach(c)

	deadline := time.Now().Add(5 * time.Second)
	for r.Lookup(s.ID) != nil || eng.PubSub.NumChannels() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("session did not expire; subscription still registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Expired sessions refuse attach.
	if _, ok := s.Attach(0, sn, nil); ok {
		t.Fatal("attach to expired session succeeded")
	}
}

func TestTakeoverClosesPrevious(t *testing.T) {
	eng, r := newTestRegistry(t, time.Minute, 100)
	s := r.New("alice", eng.NewConnState("t"))
	sn1, c1 := (&collector{}).sink()
	_, _ = s.Attach(0, sn1, nil)
	s.Deliver(push("a"))

	sn2, c2 := (&collector{}).sink()
	if _, ok := s.Attach(1, sn2, nil); !ok {
		t.Fatal("takeover attach failed")
	}
	if !c1.isClosed() {
		t.Fatal("displaced attachment was not closed")
	}
	// The displaced connection's Detach must not displace the new sink.
	s.Detach(c1)
	s.Deliver(push("b"))
	if got := c2.got(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("delivery after stale detach = %v, want [2]", got)
	}
}

func TestAbortedRoundTrip(t *testing.T) {
	eng, r := newTestRegistry(t, time.Minute, 100)
	s := r.New("alice", eng.NewConnState("t"))
	sn1, c1 := (&collector{}).sink()
	_, _ = s.Attach(0, sn1, nil)
	s.Detach(c1)
	s.RecordAborted(7, 9)

	sn2, _ := (&collector{}).sink()
	aborted, ok := s.Attach(0, sn2, nil)
	if !ok {
		t.Fatal("attach failed")
	}
	if len(aborted) != 2 || aborted[0] != 7 || aborted[1] != 9 {
		t.Fatalf("aborted = %v, want [7 9]", aborted)
	}
	// Reported once.
	sn3, _ := (&collector{}).sink()
	if aborted, _ := s.Attach(0, sn3, nil); len(aborted) != 0 {
		t.Fatalf("second attach aborted = %v, want none", aborted)
	}
}

func TestTimeTrim(t *testing.T) {
	eng, r := newTestRegistry(t, 40*time.Millisecond, 100)
	s := r.New("alice", eng.NewConnState("t"))
	s.Deliver(push("a"))
	s.Deliver(push("b"))
	time.Sleep(60 * time.Millisecond)
	s.Deliver(push("c")) // trims a,b by age
	if n := s.BufferedLen(); n != 1 {
		t.Fatalf("buffered = %d, want 1", n)
	}
	// A client that missed seq 2 can no longer resume across it.
	sn, _ := (&collector{}).sink()
	if _, ok := s.Attach(1, sn, nil); ok {
		t.Fatal("attach across aged-out pushes succeeded")
	}
}
