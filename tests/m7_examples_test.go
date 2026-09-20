// M7c example-application end-to-end tests (§11.4): the leaderboard
// submitter core (examples/leaderboard/submit) and the chat bot core
// (examples/chat/botcore) run against an in-process server exactly as the
// demos do — over the /ws/v1 surface via the Go client — with
// notify-keyspace-events enabled for the presence-expiry flow.
package tests

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/clients/go/ultima"
	"github.com/pschlump/ultima/examples/chat/botcore"
	"github.com/pschlump/ultima/examples/leaderboard/submit"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/httpapi"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

// m7ExampleServer boots the engine + HTTP/WS surface (the only surface the
// examples use) on ephemeral ports, with keyspace notifications on.
type m7ExampleServer struct {
	eng      *commands.Engine
	httpAddr string
}

func newM7ExampleServer(t *testing.T) *m7ExampleServer {
	t.Helper()
	logger := testLogger()
	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	// K and E alone enable no event classes (Redis semantics: the class
	// letters carry the events, A = all) — KEA, as in the M5a differential
	// scripts.
	if !eng.SetNotifyKeyspaceEvents("KEA") {
		t.Fatal("SetNotifyKeyspaceEvents(KEA) rejected")
	}

	httpLis := listen(t, "127.0.0.1:0")
	r := chi.NewRouter()
	httpapi.NewServer(eng, nil, nil, logger, nil).Register(r)
	reg := wssession.NewRegistry(eng, 0, 0, logger)
	t.Cleanup(reg.Close)
	r.Get("/ws/v1", wssrv.Handler(eng, nil, reg, logger, nil))
	httpSrv := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = httpSrv.Serve(httpLis) }()
	t.Cleanup(func() { _ = httpSrv.Close() })

	return &m7ExampleServer{eng: eng, httpAddr: httpLis.Addr().String()}
}

// push is one pub/sub delivery observed by a test subscriber.
type push struct {
	channel string
	payload string
}

// pushSink returns an OnPush handler funnelling message pushes into ch.
func pushSink(ch chan<- push) func(v ultima.Value, _ uint64) {
	return func(v ultima.Value, _ uint64) {
		parts, ok := ultima.AsStringSlice(v)
		if ok && len(parts) == 3 && parts[0] == "message" {
			ch <- push{channel: parts[1], payload: parts[2]}
		}
	}
}

func dialWS(t *testing.T, addr string, onPush func(v ultima.Value, pushSeq uint64)) *ultima.WSClient {
	t.Helper()
	opts := ultima.WSOptions{Addr: addr}
	if onPush != nil {
		opts.OnPush = onPush
	}
	ws, err := ultima.DialWS(opts)
	if err != nil {
		t.Fatalf("DialWS %s: %s", addr, err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// recvPush waits for one push on ch, failing the test after a generous
// bound.
func recvPush(t *testing.T, ch <-chan push, what string) push {
	t.Helper()
	select {
	case p := <-ch:
		return p
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return push{}
	}
}

// TestM7ExamplesLeaderboard drives the submitter's core against a live
// server: every submitted score lands in lb:scores and every lb:update push
// arrives at a WS subscriber, in submission order.
func TestM7ExamplesLeaderboard(t *testing.T) {
	srv := newM7ExampleServer(t)

	pushes := make(chan push, 256)
	sub := dialWS(t, srv.httpAddr, pushSink(pushes))
	if v, err := sub.Subscribe(submit.UpdateChannel); err != nil || ultima.IsError(v) {
		t.Fatalf("SUBSCRIBE %s: %v %s", submit.UpdateChannel, err, v.Str)
	}
	pub := dialWS(t, srv.httpAddr, nil)

	// Fixed seed: the submissions are deterministic and individually known.
	rnd := rand.New(rand.NewSource(42))
	const n = 25
	const players = 8
	want := make([]submit.Update, 0, n)
	for i := 0; i < n; i++ {
		u, err := submit.One(pub, rnd, players)
		if err != nil {
			t.Fatalf("submit.One: %s", err)
		}
		want = append(want, u)
	}

	// Pushes arrive in order, payloads matching the submissions exactly.
	for i, u := range want {
		p := recvPush(t, pushes, "lb:update push")
		if p.channel != submit.UpdateChannel {
			t.Fatalf("push %d channel = %q, want %q", i, p.channel, submit.UpdateChannel)
		}
		var got submit.Update
		if err := json.Unmarshal([]byte(p.payload), &got); err != nil {
			t.Fatalf("push %d payload %q: %s", i, p.payload, err)
		}
		if got != u {
			t.Fatalf("push %d = %+v, want %+v", i, got, u)
		}
	}

	// The board holds each player's latest score (ZADD last-write-wins).
	last := map[string]float64{}
	for _, u := range want {
		last[u.Player] = u.Score
	}
	for player, score := range last {
		v, err := pub.ZScore(submit.ScoresKey, player)
		if err != nil {
			t.Fatalf("ZSCORE %s: %s", player, err)
		}
		// ZSCORE replies are doubles on the binary surfaces.
		if f, ok := ultima.AsFloat(v); !ok || f != score {
			t.Errorf("ZSCORE %s = %v (ok=%v), want %v", player, v, ok, score)
		}
	}
	if v, err := pub.Exec("ZCARD", submit.ScoresKey); err != nil {
		t.Fatalf("ZCARD: %s", err)
	} else if card, ok := ultima.AsInt(v); !ok || int(card) != len(last) {
		t.Errorf("ZCARD = %v (ok=%v), want %d", v, ok, len(last))
	}

	// The looping form (submit.Run, as the submitter binary drives it)
	// submits at the configured rate until cancelled.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- submit.Run(runCtx, pub, submit.Config{Rate: 200, Players: players},
			rand.New(rand.NewSource(7)), func(string, ...any) {})
	}()
	recvPush(t, pushes, "push from submit.Run")
	recvPush(t, pushes, "push from submit.Run")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("submit.Run: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("submit.Run did not stop on cancel")
	}
}

// TestM7ExamplesChat runs the chat flow: a bot wired exactly like
// examples/chat/bot, a subscriber and a publisher; history (LRANGE) and the
// 100-entry cap; presence via expiring keys + __keyevent@0__:expired.
func TestM7ExamplesChat(t *testing.T) {
	srv := newM7ExampleServer(t)

	pushes := make(chan push, 512)

	// The bot: subscribe chat:lobby, answer commands via botcore.Handle on a
	// separate goroutine (OnPush runs on the client's read loop — handling
	// inline would deadlock), exactly as examples/chat/bot wires it.
	var botWS *ultima.WSClient
	bot := dialWS(t, srv.httpAddr, func(v ultima.Value, _ uint64) {
		parts, ok := ultima.AsStringSlice(v)
		if !ok || len(parts) != 3 || parts[0] != "message" || parts[1] != botcore.Channel {
			return
		}
		go func() { _ = botcore.Handle(botWS, time.Now(), []byte(parts[2])) }()
	})
	botWS = bot
	if v, err := bot.Subscribe(botcore.Channel); err != nil || ultima.IsError(v) {
		t.Fatalf("bot SUBSCRIBE: %v %s", err, v.Str)
	}

	// Alice subscribes; bob publishes.
	alice := dialWS(t, srv.httpAddr, pushSink(pushes))
	if v, err := alice.Subscribe(botcore.Channel); err != nil || ultima.IsError(v) {
		t.Fatalf("alice SUBSCRIBE: %v %s", err, v.Str)
	}
	bob := dialWS(t, srv.httpAddr, nil)

	post := func(user, text string) {
		t.Helper()
		if err := botcore.Post(bob, botcore.Message{User: user, Text: text, Ts: time.Now().UnixMilli()}); err != nil {
			t.Fatalf("post %q: %s", text, err)
		}
	}

	// A plain message is delivered to subscribers and lands in history.
	post("bob", "hello lobby")
	p := recvPush(t, pushes, "chat message")
	if p.channel != botcore.Channel {
		t.Fatalf("channel = %q, want %q", p.channel, botcore.Channel)
	}
	var m botcore.Message
	if err := json.Unmarshal([]byte(p.payload), &m); err != nil {
		t.Fatalf("message payload: %s", err)
	}
	if m.User != "bob" || m.Text != "hello lobby" {
		t.Fatalf("message = %+v", m)
	}

	// A bot command gets the bot's reply delivered (and into history).
	post("bob", "!ping")
	p = recvPush(t, pushes, "!ping echo") // bob's own message first
	var cmd botcore.Message
	if err := json.Unmarshal([]byte(p.payload), &cmd); err != nil || cmd.Text != "!ping" {
		t.Fatalf("echo = %q (%v)", p.payload, err)
	}
	p = recvPush(t, pushes, "bot PONG reply")
	var reply botcore.Message
	if err := json.Unmarshal([]byte(p.payload), &reply); err != nil {
		t.Fatalf("reply payload: %s", err)
	}
	if reply.User != "bot" || reply.Text != "PONG" {
		t.Fatalf("bot reply = %+v, want bot/PONG", reply)
	}

	// !time answers with a timestamp; a plain text gets no reply.
	if s, ok := botcore.Reply(time.Now(), "!time"); !ok || s == "" {
		t.Error("Reply(!time) returned nothing")
	}
	if _, ok := botcore.Reply(time.Now(), "just chatting"); ok {
		t.Error("Reply(plain text) should not answer")
	}

	v, err := alice.LRange(botcore.HistKey, 0, -1)
	if err != nil {
		t.Fatalf("LRANGE: %s", err)
	}
	hist, ok := ultima.AsStringSlice(v)
	if !ok || len(hist) != 3 {
		t.Fatalf("history = %v (ok=%v), want 3 entries", hist, ok)
	}
	var last botcore.Message
	if err := json.Unmarshal([]byte(hist[2]), &last); err != nil || last.User != "bot" || last.Text != "PONG" {
		t.Fatalf("history tail = %q (%v)", hist[2], err)
	}

	// The cap: 120 more posts leave exactly HistCap entries.
	for i := 0; i < 120; i++ {
		post("bob", "flood")
	}
	if v, err := alice.LLen(botcore.HistKey); err != nil {
		t.Fatalf("LLEN: %s", err)
	} else if n, ok := ultima.AsInt(v); !ok || n != botcore.HistCap {
		t.Errorf("LLEN = %v (ok=%v), want %d", v, ok, botcore.HistCap)
	}

	// Presence: a short-PX presence key expires; the subscriber on
	// __keyevent@0__:expired sees the key name as the payload.
	expired := make(chan push, 16)
	watcher := dialWS(t, srv.httpAddr, pushSink(expired))
	if v, err := watcher.Subscribe("__keyevent@0__:expired"); err != nil || ultima.IsError(v) {
		t.Fatalf("watcher SUBSCRIBE: %v %s", err, v.Str)
	}
	if v, err := bob.SetOpts(botcore.PresencePrefix+"bob", "1",
		ultima.SetOptions{PX: 300 * time.Millisecond}); err != nil || ultima.IsError(v) {
		t.Fatalf("presence SET: %v %s", err, v.Str)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		p := recvPush(t, expired, "expired notification")
		if p.channel != "__keyevent@0__:expired" {
			t.Fatalf("channel = %q", p.channel)
		}
		if p.payload == botcore.PresencePrefix+"bob" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("saw expiry of %q, never of the presence key", p.payload)
		}
	}
}
