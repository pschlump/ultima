package commands

import (
	"strings"
	"sync"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
)

// capturedDeliveries collects broker-pushed frames for assertions; the
// slice stands in for the front-end push queue.
type capturedDeliveries struct {
	mu   sync.Mutex
	vals []resp.Value
}

func (c *capturedDeliveries) deliver(v resp.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vals = append(c.vals, v)
}

func (c *capturedDeliveries) get() []resp.Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]resp.Value(nil), c.vals...)
}

// newSubEngine returns an engine plus a connection whose StartPush hook
// captures deliveries (what lib/respserver wires over a socket).
func newSubEngine(t *testing.T) (*Engine, *ConnState, *capturedDeliveries) {
	t.Helper()
	e, cs := newTestEngine(t)
	cd := &capturedDeliveries{}
	cs.StartPush = func() func(resp.Value) { return cd.deliver }
	return e, cs, cd
}

func strOf(v resp.Value) string { return string(v.Blob) + v.Str }

func TestSubscribeAckShapes(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	v := run(t, e, cs, "SUBSCRIBE", "news")
	if v.Kind != resp.KindPush {
		t.Fatalf("SUBSCRIBE ack kind = %v, want push", v.Kind)
	}
	if got := []string{strOf(v.Arr[0]), strOf(v.Arr[1])}; got[0] != "subscribe" || got[1] != "news" {
		t.Fatalf("ack = %v", got)
	}
	if v.Arr[2].Int != 1 {
		t.Fatalf("ack count = %d, want 1", v.Arr[2].Int)
	}

	// Multi-channel subscribe: first ack is the reply, rest in outbox,
	// running counts 2 and 3.
	v = run(t, e, cs, "SUBSCRIBE", "sports", "weather")
	if strOf(v.Arr[1]) != "sports" || v.Arr[2].Int != 2 {
		t.Fatalf("first ack = %v", v)
	}
	out := cs.DrainOutbox()
	if len(out) != 1 {
		t.Fatalf("outbox len = %d, want 1", len(out))
	}
	if strOf(out[0].Arr[1]) != "weather" || out[0].Arr[2].Int != 3 {
		t.Fatalf("second ack = %v", out[0])
	}
	if len(cs.DrainOutbox()) != 0 {
		t.Fatal("outbox not drained")
	}

	// Duplicate subscribe: ack repeats, count unchanged.
	v = run(t, e, cs, "SUBSCRIBE", "news")
	if v.Arr[2].Int != 3 {
		t.Fatalf("dup subscribe count = %d, want 3", v.Arr[2].Int)
	}
	if n := e.PubSub.NumChannels(); n != 3 {
		t.Fatalf("broker channels = %d, want 3", n)
	}
}

func TestPSubscribeAckShape(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	run(t, e, cs, "SUBSCRIBE", "a")
	v := run(t, e, cs, "PSUBSCRIBE", "n*", "w*")
	if strOf(v.Arr[0]) != "psubscribe" || strOf(v.Arr[1]) != "n*" || v.Arr[2].Int != 2 {
		t.Fatalf("psubscribe ack = %v", v)
	}
	out := cs.DrainOutbox()
	if len(out) != 1 || strOf(out[0].Arr[1]) != "w*" || out[0].Arr[2].Int != 3 {
		t.Fatalf("outbox = %v", out)
	}
	if n := e.PubSub.NumPat(); n != 2 {
		t.Fatalf("NumPat = %d, want 2", n)
	}
}

func TestUnsubscribeShapes(t *testing.T) {
	e, cs, _ := newSubEngine(t)

	// No args, no subscriptions: single null-channel ack, count 0.
	v := run(t, e, cs, "UNSUBSCRIBE")
	if len(v.Arr) != 3 || v.Arr[1].Kind != resp.KindNull || v.Arr[2].Int != 0 {
		t.Fatalf("empty unsubscribe ack = %v", v)
	}

	run(t, e, cs, "SUBSCRIBE", "keep", "drop")
	cs.DrainOutbox()

	// With args: acked whether or not subscribed; count = remaining.
	v = run(t, e, cs, "UNSUBSCRIBE", "drop", "ghost")
	if strOf(v.Arr[0]) != "unsubscribe" || strOf(v.Arr[1]) != "drop" || v.Arr[2].Int != 1 {
		t.Fatalf("unsub drop ack = %v", v)
	}
	out := cs.DrainOutbox()
	if len(out) != 1 || strOf(out[0].Arr[1]) != "ghost" || out[0].Arr[2].Int != 1 {
		t.Fatalf("unsub ghost ack = %v", out)
	}

	// No args with one left: single ack, count 0.
	v = run(t, e, cs, "UNSUBSCRIBE")
	if strOf(v.Arr[1]) != "keep" || v.Arr[2].Int != 0 {
		t.Fatalf("unsub-all ack = %v", v)
	}
	if n := e.PubSub.NumChannels(); n != 0 {
		t.Fatalf("broker channels after unsub-all = %d, want 0", n)
	}
}

func TestPublishDeliveryAndCounts(t *testing.T) {
	e, cs1, cap1 := newSubEngine(t)
	cs2 := e.NewConnState("127.0.0.1:2")
	cap2 := &capturedDeliveries{}
	cs2.StartPush = func() func(resp.Value) { return cap2.deliver }
	cs3 := e.NewConnState("127.0.0.1:3")
	cap3 := &capturedDeliveries{}
	cs3.StartPush = func() func(resp.Value) { return cap3.deliver }

	run(t, e, cs1, "SUBSCRIBE", "news")
	run(t, e, cs1, "PSUBSCRIBE", "n*") // cs1 gets each news message twice
	run(t, e, cs2, "SUBSCRIBE", "news")
	run(t, e, cs3, "PSUBSCRIBE", "*")

	pub := e.NewConnState("127.0.0.1:9")
	v := run(t, e, pub, "PUBLISH", "news", "hello")
	// Deliveries, not unique clients: cs1 channel + cs1 pattern +
	// cs2 channel + cs3 pattern = 4.
	if v.Int != 4 {
		t.Fatalf("PUBLISH = %d, want 4", v.Int)
	}

	got1 := cap1.get()
	if len(got1) != 2 {
		t.Fatalf("cs1 deliveries = %d, want 2", len(got1))
	}
	msg, pmsg := got1[0], got1[1]
	if strOf(msg.Arr[0]) != "message" || strOf(msg.Arr[1]) != "news" || strOf(msg.Arr[2]) != "hello" {
		t.Fatalf("message frame = %v", msg)
	}
	if strOf(pmsg.Arr[0]) != "pmessage" || strOf(pmsg.Arr[1]) != "n*" ||
		strOf(pmsg.Arr[2]) != "news" || strOf(pmsg.Arr[3]) != "hello" {
		t.Fatalf("pmessage frame = %v", pmsg)
	}
	if got := cap2.get(); len(got) != 1 || strOf(got[0].Arr[0]) != "message" {
		t.Fatalf("cs2 deliveries = %v", got)
	}
	if got := cap3.get(); len(got) != 1 || strOf(got[0].Arr[0]) != "pmessage" || strOf(got[0].Arr[1]) != "*" {
		t.Fatalf("cs3 deliveries = %v", got)
	}

	// Only cs3's "*" pattern matches "other".
	if v := run(t, e, pub, "PUBLISH", "other", "x"); v.Int != 1 {
		t.Fatalf("PUBLISH other = %d, want 1", v.Int)
	}
	// Nobody listening: count 0.
	run(t, e, cs3, "PUNSUBSCRIBE", "*")
	if v := run(t, e, pub, "PUBLISH", "other", "x"); v.Int != 0 {
		t.Fatalf("PUBLISH other after punsub = %d, want 0", v.Int)
	}
}

func TestPublishSelfDeliveryPostponed(t *testing.T) {
	e, cs, cd := newSubEngine(t)
	// A RESP2 subscribed connection is gated from PUBLISH; self-publish
	// is a RESP3 flow (verified against 7.2.7).
	run(t, e, cs, "HELLO", "3")
	run(t, e, cs, "SUBSCRIBE", "me")
	// A subscribed client publishing to its own channel gets the :count
	// reply first; the message frame is postponed into the outbox
	// (Redis pending_push_messages), NOT the async push queue.
	v := run(t, e, cs, "PUBLISH", "me", "self")
	if v.Int != 1 {
		t.Fatalf("self PUBLISH = %d, want 1", v.Int)
	}
	out := cs.DrainOutbox()
	if len(out) != 1 || strOf(out[0].Arr[0]) != "message" || strOf(out[0].Arr[2]) != "self" {
		t.Fatalf("postponed self message = %v", out)
	}
	if got := cd.get(); len(got) != 0 {
		t.Fatalf("self message must not use the async queue, got %v", got)
	}
}

func TestSubscribeModeGate(t *testing.T) {
	e, cs, _ := newSubEngine(t)

	// No subscriptions: everything runs.
	if v := run(t, e, cs, "GET", "x"); v.Kind != resp.KindNull {
		t.Fatalf("GET outside sub mode = %v", v)
	}
	run(t, e, cs, "SUBSCRIBE", "g")

	// RESP2 gate: lowercase command name, container fullnames.
	want := "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
	if v := run(t, e, cs, "GET", "x"); v.Kind != resp.KindError || v.Str != want {
		t.Fatalf("gate error = %q, want %q", v.Str, want)
	}
	if v := run(t, e, cs, "CONFIG", "GET", "maxmemory"); !strings.Contains(v.Str, "'config|get'") {
		t.Fatalf("gate fullname for CONFIG GET = %q", v.Str)
	}
	if v := run(t, e, cs, "PUBSUB", "NUMSUB", "g"); !strings.Contains(v.Str, "'pubsub|numsub'") {
		t.Fatalf("gate fullname for PUBSUB NUMSUB = %q", v.Str)
	}
	// Unknown container subcommand: resolution error beats the gate.
	if v := run(t, e, cs, "PUBSUB", "BOGUS"); v.Str != "ERR unknown subcommand 'BOGUS'. Try PUBSUB HELP." {
		t.Fatalf("PUBSUB BOGUS in sub mode = %q", v.Str)
	}
	// Arity error beats the gate.
	if v := run(t, e, cs, "GET"); v.Str != "ERR wrong number of arguments for 'get' command" {
		t.Fatalf("bare GET in sub mode = %q", v.Str)
	}
	// Unknown command beats the gate.
	if v := run(t, e, cs, "NOSUCHCMD"); !strings.HasPrefix(v.Str, "ERR unknown command") {
		t.Fatalf("unknown cmd in sub mode = %q", v.Str)
	}
	// EXEC is wrapped as EXECABORT.
	if v := run(t, e, cs, "EXEC"); !strings.HasPrefix(v.Str, "EXECABORT Transaction discarded because of: Can't execute 'exec':") {
		t.Fatalf("EXEC in sub mode = %q", v.Str)
	}

	// PING shapes in subscribe mode (RESP2).
	v := run(t, e, cs, "PING")
	if len(v.Arr) != 2 || strOf(v.Arr[0]) != "pong" || strOf(v.Arr[1]) != "" {
		t.Fatalf("PING in sub mode = %v", v)
	}
	v = run(t, e, cs, "PING", "hey")
	if len(v.Arr) != 2 || strOf(v.Arr[0]) != "pong" || strOf(v.Arr[1]) != "hey" {
		t.Fatalf("PING msg in sub mode = %v", v)
	}

	// Allowed commands run.
	if v := run(t, e, cs, "SUBSCRIBE", "more"); v.Kind != resp.KindPush {
		t.Fatalf("SUBSCRIBE in sub mode = %v", v)
	}
	if v := run(t, e, cs, "UNSUBSCRIBE"); v.Kind != resp.KindPush {
		t.Fatalf("UNSUBSCRIBE in sub mode = %v", v)
	}
	cs.DrainOutbox()
	// Back to zero subscriptions: gate lifts.
	if v := run(t, e, cs, "GET", "x"); v.Kind != resp.KindNull {
		t.Fatalf("GET after unsub-all = %v", v)
	}
}

func TestSubscribeModeGateRESP3(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	run(t, e, cs, "HELLO", "3")
	run(t, e, cs, "SUBSCRIBE", "r3")
	// RESP3 connections are never gated.
	if v := run(t, e, cs, "GET", "x"); v.Kind != resp.KindNull {
		t.Fatalf("RESP3 GET in sub mode = %v", v)
	}
	if v := run(t, e, cs, "PING"); v.Kind != resp.KindSimpleString || v.Str != "PONG" {
		t.Fatalf("RESP3 PING in sub mode = %v", v)
	}
	if v := run(t, e, cs, "PING", "hi"); strOf(v) != "hi" {
		t.Fatalf("RESP3 PING msg in sub mode = %v", v)
	}
	if v := run(t, e, cs, "DBSIZE"); v.Kind != resp.KindInt {
		t.Fatalf("RESP3 DBSIZE in sub mode = %v", v)
	}
}

func TestPubSubIntrospection(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	cs2 := e.NewConnState("127.0.0.1:2")
	// PUBSUB is gated for subscribed RESP2 connections, so introspection
	// runs on a plain third connection.
	obs := e.NewConnState("127.0.0.1:3")

	run(t, e, cs, "SUBSCRIBE", "news", "sports", "nightly")
	run(t, e, cs2, "SUBSCRIBE", "news")
	cs.DrainOutbox()

	v := run(t, e, obs, "PUBSUB", "CHANNELS", "n*")
	if len(v.Arr) != 2 {
		t.Fatalf("CHANNELS n* = %v", v)
	}
	names := map[string]bool{strOf(v.Arr[0]): true, strOf(v.Arr[1]): true}
	if !names["news"] || !names["nightly"] {
		t.Fatalf("CHANNELS n* = %v", names)
	}

	v = run(t, e, obs, "PUBSUB", "NUMSUB", "news", "nosuch")
	if len(v.Arr) != 4 || strOf(v.Arr[0]) != "news" || v.Arr[1].Int != 2 ||
		strOf(v.Arr[2]) != "nosuch" || v.Arr[3].Int != 0 {
		t.Fatalf("NUMSUB = %v", v)
	}

	if v := run(t, e, obs, "PUBSUB", "NUMPAT"); v.Int != 0 {
		t.Fatalf("NUMPAT = %d, want 0", v.Int)
	}
	run(t, e, cs, "PSUBSCRIBE", "a*")
	if v := run(t, e, obs, "PUBSUB", "NUMPAT"); v.Int != 1 {
		t.Fatalf("NUMPAT = %d, want 1", v.Int)
	}
	// Pattern-only subscriptions do not create channels.
	v = run(t, e, obs, "PUBSUB", "CHANNELS")
	if len(v.Arr) != 3 {
		t.Fatalf("CHANNELS with pattern sub = %v", v)
	}

	// Errors (verified against 7.2.7).
	if v := run(t, e, obs, "PUBSUB", "BOGUS"); v.Str != "ERR unknown subcommand 'BOGUS'. Try PUBSUB HELP." {
		t.Fatalf("PUBSUB BOGUS = %q", v.Str)
	}
	if v := run(t, e, obs, "PUBSUB", "CHANNELS", "a", "b"); v.Str != "ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP." {
		t.Fatalf("PUBSUB CHANNELS a b = %q", v.Str)
	}
	if v := run(t, e, obs, "PUBSUB", "NUMPAT", "x"); v.Str != "ERR wrong number of arguments for 'pubsub|numpat' command" {
		t.Fatalf("PUBSUB NUMPAT x = %q", v.Str)
	}
}

func TestResetClearsSubscriptions(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	run(t, e, cs, "SUBSCRIBE", "r1")
	run(t, e, cs, "PSUBSCRIBE", "r*")
	if v := run(t, e, cs, "RESET"); v.Kind != resp.KindSimpleString || v.Str != "RESET" {
		t.Fatalf("RESET = %v", v)
	}
	if cs.subCount() != 0 {
		t.Fatalf("subCount after RESET = %d, want 0", cs.subCount())
	}
	if n := e.PubSub.NumChannels(); n != 0 {
		t.Fatalf("channels after RESET = %d, want 0", n)
	}
	if n := e.PubSub.NumPat(); n != 0 {
		t.Fatalf("patterns after RESET = %d, want 0", n)
	}
	// Gate lifted: GET runs.
	if v := run(t, e, cs, "GET", "x"); v.Kind != resp.KindNull {
		t.Fatalf("GET after RESET = %v", v)
	}
}

func TestCloseConnCleansSubscriptions(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	run(t, e, cs, "SUBSCRIBE", "c1", "c2")
	run(t, e, cs, "PSUBSCRIBE", "p*")
	cs.DrainOutbox()
	e.CloseConn(cs)
	if n := e.PubSub.NumChannels(); n != 0 {
		t.Fatalf("channels after CloseConn = %d, want 0", n)
	}
	if n := e.PubSub.NumPat(); n != 0 {
		t.Fatalf("patterns after CloseConn = %d, want 0", n)
	}
}

func TestSubscribeInsideMulti(t *testing.T) {
	e, cs, _ := newSubEngine(t)
	if v := run(t, e, cs, "MULTI"); v.Str != "OK" {
		t.Fatalf("MULTI = %v", v)
	}
	if v := run(t, e, cs, "SUBSCRIBE", "q1"); v.Str != "QUEUED" {
		t.Fatalf("SUBSCRIBE in MULTI = %v, want QUEUED", v)
	}
	v := run(t, e, cs, "EXEC")
	if len(v.Arr) != 1 || v.Arr[0].Kind != resp.KindPush || strOf(v.Arr[0].Arr[0]) != "subscribe" {
		t.Fatalf("EXEC = %v", v)
	}
	if cs.subCount() != 1 {
		t.Fatalf("subCount after EXEC = %d, want 1", cs.subCount())
	}
	// The subscription is live: a publish reaches the connection.
	pub := e.NewConnState("127.0.0.1:9")
	if v := run(t, e, pub, "PUBLISH", "q1", "m"); v.Int != 1 {
		t.Fatalf("PUBLISH = %d, want 1", v.Int)
	}
}
