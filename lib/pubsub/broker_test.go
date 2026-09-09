package pubsub

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
)

// testGlob is the pattern matcher injected into the broker under test.
// lib/commands.GlobMatch (the production matcher) cannot be used here:
// lib/commands imports lib/pubsub, so importing it back would be an
// import cycle. Supports '*' and literal bytes — enough for every
// pattern these tests use. Its semantics are covered by the GlobMatch
// tests in lib/commands.
func testGlob(pattern, s string) bool {
	i := strings.IndexByte(pattern, '*')
	if i < 0 {
		return pattern == s
	}
	if i > len(s) || pattern[:i] != s[:i] {
		return false
	}
	rest, tail := pattern[i+1:], s[i:]
	if rest == "" {
		return true // trailing star matches the rest
	}
	// Try to match the remainder at every position of the tail.
	for j := 0; j <= len(tail); j++ {
		if testGlob(rest, tail[j:]) {
			return true
		}
	}
	return false
}

// recorder is a Sub.Deliver sink safe for the concurrency test.
type recorder struct {
	mu   sync.Mutex
	vals []resp.Value
}

func (r *recorder) deliver(v resp.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.vals = append(r.vals, v)
}

func (r *recorder) got() []resp.Value {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]resp.Value(nil), r.vals...)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.vals)
}

func messageFrame(channel, payload string) resp.Value {
	return resp.Push(resp.BlobStr("message"), resp.BlobStr(channel), resp.BlobStr(payload))
}

func pmessageFrame(pattern, channel, payload string) resp.Value {
	return resp.Push(resp.BlobStr("pmessage"), resp.BlobStr(pattern),
		resp.BlobStr(channel), resp.BlobStr(payload))
}

func assertFrames(t *testing.T, what string, got []resp.Value, want ...resp.Value) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: frames = %v, want %v", what, got, want)
	}
}

func newBroker() *Broker { return New(testGlob) }

func TestPublishDirectMessage(t *testing.T) {
	b := newBroker()
	r1, r2, r3 := &recorder{}, &recorder{}, &recorder{}
	b.Subscribe("news", &Sub{ID: 1, Deliver: r1.deliver})
	b.Subscribe("news", &Sub{ID: 2, Deliver: r2.deliver})
	b.Subscribe("other", &Sub{ID: 3, Deliver: r3.deliver})

	if n := b.Publish("news", []byte("hello"), 0, nil); n != 2 {
		t.Fatalf("Publish = %d, want 2", n)
	}
	assertFrames(t, "sub1", r1.got(), messageFrame("news", "hello"))
	assertFrames(t, "sub2", r2.got(), messageFrame("news", "hello"))
	assertFrames(t, "sub3 (other channel)", r3.got())

	// No subscribers: no deliveries.
	if n := b.Publish("nobody", []byte("x"), 0, nil); n != 0 {
		t.Fatalf("Publish(nobody) = %d, want 0", n)
	}

	// Subscribe is idempotent per ID: re-registering sub 1 does not
	// double its deliveries.
	b.Subscribe("news", &Sub{ID: 1, Deliver: r1.deliver})
	if n := b.Publish("news", []byte("again"), 0, nil); n != 2 {
		t.Fatalf("Publish after re-Subscribe = %d, want 2", n)
	}
}

func TestPublishPatternMessage(t *testing.T) {
	b := newBroker()
	r1, r2 := &recorder{}, &recorder{}
	b.PSubscribe("ne*", &Sub{ID: 1, Deliver: r1.deliver})
	b.PSubscribe("x*", &Sub{ID: 2, Deliver: r2.deliver})

	if n := b.Publish("news", []byte("p"), 0, nil); n != 1 {
		t.Fatalf("Publish = %d, want 1", n)
	}
	assertFrames(t, "pattern sub", r1.got(), pmessageFrame("ne*", "news", "p"))
	assertFrames(t, "non-matching pattern sub", r2.got())

	// PSubscribe is idempotent per ID.
	b.PSubscribe("ne*", &Sub{ID: 1, Deliver: r1.deliver})
	if n := b.Publish("news", []byte("p"), 0, nil); n != 1 {
		t.Fatalf("Publish after re-PSubscribe = %d, want 1", n)
	}
}

// TestDeliveryCounting pins Redis's delivery-counting semantics
// (documented on Publish): a client subscribed to both the channel and
// a matching pattern gets — and counts — two frames; two matching
// patterns on one client likewise count twice.
func TestDeliveryCounting(t *testing.T) {
	b := newBroker()
	both := &recorder{}
	multi := &recorder{}
	plain := &recorder{}

	// Client 1: channel + one matching pattern.
	b.Subscribe("c", &Sub{ID: 1, Deliver: both.deliver})
	b.PSubscribe("c*", &Sub{ID: 1, Deliver: both.deliver})
	// Client 2: two matching patterns.
	b.PSubscribe("*", &Sub{ID: 2, Deliver: multi.deliver})
	b.PSubscribe("c*", &Sub{ID: 2, Deliver: multi.deliver})
	// Client 3: channel only.
	b.Subscribe("c", &Sub{ID: 3, Deliver: plain.deliver})

	if n := b.Publish("c", []byte("m"), 0, nil); n != 5 {
		t.Fatalf("Publish = %d, want 5 (2+2+1 deliveries, not 3 clients)", n)
	}
	assertFrames(t, "channel+pattern client", both.got(),
		messageFrame("c", "m"), pmessageFrame("c*", "c", "m"))
	// pmessage fan-out goes out in sorted-pattern order ("*" < "c*").
	assertFrames(t, "two-pattern client", multi.got(),
		pmessageFrame("*", "c", "m"), pmessageFrame("c*", "c", "m"))
	assertFrames(t, "channel-only client", plain.got(), messageFrame("c", "m"))
}

// TestPatternFanoutSorted: when several patterns match, the broker sorts
// them, so a client sees its pmessage frames in sorted-pattern order.
func TestPatternFanoutSorted(t *testing.T) {
	b := newBroker()
	r := &recorder{}
	// Registered in scrambled order on purpose.
	for _, p := range []string{"foo*", "f*", "fo*"} {
		b.PSubscribe(p, &Sub{ID: 1, Deliver: r.deliver})
	}
	if n := b.Publish("foo", []byte("x"), 0, nil); n != 3 {
		t.Fatalf("Publish = %d, want 3", n)
	}
	assertFrames(t, "sorted pmessage order", r.got(),
		pmessageFrame("f*", "foo", "x"),
		pmessageFrame("fo*", "foo", "x"),
		pmessageFrame("foo*", "foo", "x"))
}

// TestPublishSelfRouting: frames for the subscriber whose ID is selfID
// go to the self callback (so the publishing connection can append them
// after its own PUBLISH reply), are dropped when self is nil, and are
// counted either way.
func TestPublishSelfRouting(t *testing.T) {
	b := newBroker()
	r1, r2 := &recorder{}, &recorder{}
	b.Subscribe("c", &Sub{ID: 7, Deliver: r1.deliver})
	b.PSubscribe("c*", &Sub{ID: 7, Deliver: r1.deliver})
	b.Subscribe("c", &Sub{ID: 8, Deliver: r2.deliver})

	self := &recorder{}
	if n := b.Publish("c", []byte("m"), 7, self.deliver); n != 3 {
		t.Fatalf("Publish = %d, want 3 (self deliveries still count)", n)
	}
	assertFrames(t, "self callback", self.got(),
		messageFrame("c", "m"), pmessageFrame("c*", "c", "m"))
	assertFrames(t, "self's Deliver must not fire", r1.got())
	assertFrames(t, "other subscriber", r2.got(), messageFrame("c", "m"))

	// self == nil: the self-addressed frames are dropped, still counted.
	if n := b.Publish("c", []byte("m"), 7, nil); n != 3 {
		t.Fatalf("Publish(self=nil) = %d, want 3", n)
	}
	assertFrames(t, "self frames dropped", r1.got())

	// selfID matching no subscriber: normal delivery everywhere.
	if n := b.Publish("c", []byte("m"), 999, self.deliver); n != 3 {
		t.Fatalf("Publish(unknown selfID) = %d, want 3", n)
	}
	assertFrames(t, "sub 7 via Deliver", r1.got(),
		messageFrame("c", "m"), pmessageFrame("c*", "c", "m"))
	assertFrames(t, "self callback unused", self.got(),
		messageFrame("c", "m"), pmessageFrame("c*", "c", "m"))
}

func TestUnsubscribe(t *testing.T) {
	b := newBroker()
	r1, r2 := &recorder{}, &recorder{}
	b.Subscribe("c", &Sub{ID: 1, Deliver: r1.deliver})
	b.Subscribe("c", &Sub{ID: 2, Deliver: r2.deliver})
	b.PSubscribe("c*", &Sub{ID: 1, Deliver: r1.deliver})

	b.Unsubscribe("c", 1)
	if got := b.NumSub("c"); !reflect.DeepEqual(got, []int64{1}) {
		t.Fatalf("NumSub after Unsubscribe = %v, want [1]", got)
	}
	if got := b.Channels(""); !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("Channels = %v, want [c] (sub 2 remains)", got)
	}
	if n := b.Publish("c", []byte("m"), 0, nil); n != 2 {
		t.Fatalf("Publish = %d, want 2 (sub 2 + pattern sub 1)", n)
	}
	assertFrames(t, "unsubscribed sub", r1.got(), pmessageFrame("c*", "c", "m"))
	assertFrames(t, "remaining sub", r2.got(), messageFrame("c", "m"))

	// Removing the last subscriber deletes the channel.
	b.Unsubscribe("c", 2)
	if got := b.Channels(""); len(got) != 0 {
		t.Fatalf("Channels after last Unsubscribe = %v, want empty", got)
	}
	if got := b.NumSub("c"); !reflect.DeepEqual(got, []int64{0}) {
		t.Fatalf("NumSub of empty channel = %v, want [0]", got)
	}
	if n := b.NumChannels(); n != 0 {
		t.Fatalf("NumChannels = %d, want 0", n)
	}

	// Unsubscribe from a channel/ID that does not exist is a no-op.
	b.Unsubscribe("c", 2)
	b.Unsubscribe("never", 1)

	b.PUnsubscribe("c*", 1)
	if n := b.NumPat(); n != 0 {
		t.Fatalf("NumPat after PUnsubscribe = %d, want 0", n)
	}
	if n := b.Publish("c", []byte("m"), 0, nil); n != 0 {
		t.Fatalf("Publish with no subs = %d, want 0", n)
	}
	b.PUnsubscribe("c*", 1) // no-op on a removed pattern
}

func TestUnsubscribeAll(t *testing.T) {
	b := newBroker()
	r1, r2 := &recorder{}, &recorder{}
	b.Subscribe("b", &Sub{ID: 1, Deliver: r1.deliver})
	b.Subscribe("a", &Sub{ID: 1, Deliver: r1.deliver})
	b.Subscribe("a", &Sub{ID: 2, Deliver: r2.deliver})
	b.PSubscribe("x*", &Sub{ID: 1, Deliver: r1.deliver})
	b.PSubscribe("a*", &Sub{ID: 1, Deliver: r1.deliver})
	b.PSubscribe("x*", &Sub{ID: 2, Deliver: r2.deliver})

	channels, patterns := b.UnsubscribeAll(1)
	if !reflect.DeepEqual(channels, []string{"a", "b"}) {
		t.Fatalf("UnsubscribeAll channels = %v, want [a b] (sorted)", channels)
	}
	if !reflect.DeepEqual(patterns, []string{"a*", "x*"}) {
		t.Fatalf("UnsubscribeAll patterns = %v, want [a* x*] (sorted)", patterns)
	}

	// Sub 1 is fully detached; sub 2 is untouched.
	if got := b.Channels(""); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("Channels = %v, want [a]", got)
	}
	if got := b.NumSub("a", "b"); !reflect.DeepEqual(got, []int64{1, 0}) {
		t.Fatalf("NumSub = %v, want [1 0]", got)
	}
	if n := b.NumPat(); n != 1 {
		t.Fatalf("NumPat = %d, want 1 (x* via sub 2)", n)
	}
	if n := b.Publish("a", []byte("m"), 0, nil); n != 1 {
		t.Fatalf("Publish = %d, want 1 (only sub 2)", n)
	}
	assertFrames(t, "detached sub 1", r1.got())
	assertFrames(t, "surviving sub 2", r2.got(), messageFrame("a", "m"))

	// Unknown ID: nothing removed, empty results.
	channels, patterns = b.UnsubscribeAll(99)
	if len(channels) != 0 || len(patterns) != 0 {
		t.Fatalf("UnsubscribeAll(99) = %v, %v, want empty", channels, patterns)
	}
}

func TestIntrospection(t *testing.T) {
	b := newBroker()
	if got := b.Channels(""); len(got) != 0 {
		t.Fatalf("Channels on empty broker = %v, want empty", got)
	}
	if got := b.NumSub(); len(got) != 0 {
		t.Fatalf("NumSub() = %v, want empty", got)
	}

	mk := func(id uint64) *Sub { return &Sub{ID: id} } // nil Deliver: introspection only
	b.Subscribe("news.tech", mk(1))
	b.Subscribe("news.tech", mk(2))
	b.Subscribe("news.art", mk(3))
	b.Subscribe("chat", mk(4))
	b.PSubscribe("news.*", mk(5))
	b.PSubscribe("news.*", mk(6)) // same pattern twice: NumPat counts patterns
	b.PSubscribe("*", mk(7))

	// Sorted, glob-filtered channel list.
	if got := b.Channels(""); !reflect.DeepEqual(got, []string{"chat", "news.art", "news.tech"}) {
		t.Fatalf("Channels(\"\") = %v", got)
	}
	if got := b.Channels("news.*"); !reflect.DeepEqual(got, []string{"news.art", "news.tech"}) {
		t.Fatalf("Channels(\"news.*\") = %v", got)
	}
	if got := b.Channels("nope*"); len(got) != 0 {
		t.Fatalf("Channels(\"nope*\") = %v, want empty", got)
	}

	// NumSub reports per-channel subscriber counts, 0 when none;
	// pattern subscriptions are not counted.
	if got := b.NumSub("news.tech", "chat", "missing"); !reflect.DeepEqual(got, []int64{2, 1, 0}) {
		t.Fatalf("NumSub = %v, want [2 1 0]", got)
	}
	if n := b.NumPat(); n != 2 {
		t.Fatalf("NumPat = %d, want 2", n)
	}
	if n := b.NumChannels(); n != 3 {
		t.Fatalf("NumChannels = %d, want 3", n)
	}
}

// TestNilDeliverSub: a nil Deliver drops messages by contract (unit
// tests without a push front-end) but Publish must not panic and must
// still count the delivery.
func TestNilDeliverSub(t *testing.T) {
	b := newBroker()
	b.Subscribe("c", &Sub{ID: 1})
	b.PSubscribe("c*", &Sub{ID: 2})
	if n := b.Publish("c", []byte("m"), 0, nil); n != 2 {
		t.Fatalf("Publish = %d, want 2", n)
	}
}

// TestKeyspaceChannelMatching: the M5a keyspace-notification channels
// route through ordinary pattern matching; `__keyevent@0__:*` must match
// DB 0 events and must not leak across DBs.
func TestKeyspaceChannelMatching(t *testing.T) {
	b := newBroker()
	r := &recorder{}
	b.PSubscribe("__keyevent@0__:*", &Sub{ID: 1, Deliver: r.deliver})

	if n := b.Publish("__keyevent@0__:expired", []byte("mykey"), 0, nil); n != 1 {
		t.Fatalf("Publish to db0 keyevent = %d, want 1", n)
	}
	assertFrames(t, "db0 keyevent", r.got(),
		pmessageFrame("__keyevent@0__:*", "__keyevent@0__:expired", "mykey"))

	if n := b.Publish("__keyevent@1__:expired", []byte("mykey"), 0, nil); n != 0 {
		t.Fatalf("Publish to db1 keyevent = %d, want 0 (must not match db0 pattern)", n)
	}
	assertFrames(t, "no db1 delivery", r.got(),
		pmessageFrame("__keyevent@0__:*", "__keyevent@0__:expired", "mykey"))
}

// TestConcurrentPublishSubscribe is a race-detector smoke test: publishers
// and subscriber churn run concurrently against the shared broker. The
// broker spawns no goroutines itself, so there is nothing to leak-check.
// Invariant: every delivery Publish counts lands in exactly one recorder
// (selfID 0 matches no subscriber, so nothing is self-routed).
func TestConcurrentPublishSubscribe(t *testing.T) {
	b := newBroker()
	const (
		channels    = 4
		publishers  = 4
		perPublish  = 250
		subscribers = 8
	)

	recs := make([]*recorder, subscribers)
	for i := range recs {
		recs[i] = &recorder{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	totalCounted := 0

	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("payload-%d", p))
			for i := 0; i < perPublish; i++ {
				ch := fmt.Sprintf("ch-%d", (p+i)%channels)
				n := b.Publish(ch, payload, 0, nil)
				mu.Lock()
				totalCounted += n
				mu.Unlock()
			}
		}(p)
	}
	for s := 0; s < subscribers; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			id := uint64(s + 1)
			sub := &Sub{ID: id, Deliver: recs[s].deliver}
			for i := 0; i < 100; i++ {
				ch := fmt.Sprintf("ch-%d", i%channels)
				switch i % 4 {
				case 0:
					b.Subscribe(ch, sub)
				case 1:
					b.PSubscribe("ch-*", sub)
				case 2:
					b.Unsubscribe(ch, id)
					b.PUnsubscribe("ch-*", id)
				case 3:
					b.UnsubscribeAll(id)
				}
			}
			b.UnsubscribeAll(id)
		}(s)
	}
	wg.Wait()

	totalDelivered := 0
	for _, r := range recs {
		totalDelivered += r.count()
	}
	// After the final UnsubscribeAll, late publishes still in flight
	// could have delivered to a snapshot taken before the detach — but
	// wg.Wait() ordering means all publishes precede it. Delivery counts
	// must match exactly what the recorders observed.
	if totalDelivered != totalCounted {
		t.Fatalf("delivered frames = %d, Publish counted %d", totalDelivered, totalCounted)
	}
}
