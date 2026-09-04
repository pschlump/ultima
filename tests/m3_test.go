package tests

// M3 pub/sub integration tests (Ultima-only, real sockets): SUBSCRIBE/
// PUBLISH push delivery over RESP2 and RESP3, pattern messages,
// unsubscribe, the RESP2 subscribe-mode gate, and subscription cleanup
// on connection close. The server is wired exactly like
// tests/integration_test.go (respserver.New on an ephemeral port).

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
)

// m3Server boots the RESP surface on an ephemeral port.
func m3Server(t *testing.T) (addr string, eng *commands.Engine) {
	t.Helper()
	lis := listen(t, "127.0.0.1:0")
	shards := shard.NewEngine(4, 16)
	t.Cleanup(shards.Close)
	eng = commands.NewEngine(shards, "test", 0)
	srv := respserver.New("127.0.0.1:0", eng)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return lis.Addr().String(), eng
}

// m3Client is a minimal raw-socket RESP client decoding one frame at a
// time into a line-oriented string form for assertions.
type m3Client struct {
	conn net.Conn
	r    *bufio.Reader
}

func m3Dial(t *testing.T, addr string) *m3Client {
	t.Helper()
	c, err := m3DialE(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.conn.Close() })
	return c
}

// m3DialE is the error-returning dial for use in stress-test goroutines,
// where t.Fatal must not be called.
func m3DialE(addr string) (*m3Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &m3Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *m3Client) send(t *testing.T, args ...string) {
	t.Helper()
	if err := c.sendE(args...); err != nil {
		t.Fatal(err)
	}
}

func (c *m3Client) sendE(args ...string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	_, err := io.WriteString(c.conn, sb.String())
	return err
}

// recv reads one RESP frame (any type) and renders it canonically:
// aggregates as "(kind elem elem ...)" with nested frames recursed,
// null bulk as "nil".
func (c *m3Client) recv(t *testing.T) string {
	t.Helper()
	v, err := c.recvE()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (c *m3Client) recvE() (string, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", err
	}
	v, err := c.readValE()
	_ = c.conn.SetReadDeadline(time.Time{})
	return v, err
}

func (c *m3Client) readValE() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	body := strings.TrimRight(line[1:], "\r\n")
	switch line[0] {
	case '+', '-', ':', ',', '#', '(':
		return string(line[0]) + body, nil
	case '_':
		return "nil", nil
	case '$', '=':
		n, err := strconv.Atoi(body)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "nil", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*', '%', '~', '>':
		n, err := strconv.Atoi(body)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "nil", nil
		}
		if line[0] == '%' {
			n *= 2
		}
		parts := make([]string, n)
		for i := range parts {
			p, err := c.readValE()
			if err != nil {
				return "", err
			}
			parts[i] = p
		}
		return string(line[0]) + "(" + strings.Join(parts, " ") + ")", nil
	}
	return "", fmt.Errorf("unknown reply type %q", line)
}

// do sends a command and reads its reply.
func (c *m3Client) do(t *testing.T, args ...string) string {
	t.Helper()
	c.send(t, args...)
	return c.recv(t)
}

// doE is the error-returning do for stress-test goroutines.
func (c *m3Client) doE(args ...string) (string, error) {
	if err := c.sendE(args...); err != nil {
		return "", err
	}
	return c.recvE()
}

func TestM3PubSubRESP2(t *testing.T) {
	addr, _ := m3Server(t)
	sub, pub := m3Dial(t, addr), m3Dial(t, addr)

	if got := sub.do(t, "SUBSCRIBE", "news"); got != "*(subscribe news :1)" {
		t.Fatalf("subscribe ack = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "news", "hello"); got != ":1" {
		t.Fatalf("publish = %q", got)
	}
	if got := sub.recv(t); got != "*(message news hello)" {
		t.Fatalf("delivery = %q", got)
	}

	// Multi-channel subscribe: two acks, then delivery on either channel.
	sub.send(t, "SUBSCRIBE", "sports", "weather")
	if got := sub.recv(t); got != "*(subscribe sports :2)" {
		t.Fatalf("ack 1 = %q", got)
	}
	if got := sub.recv(t); got != "*(subscribe weather :3)" {
		t.Fatalf("ack 2 = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "weather", "rain"); got != ":1" {
		t.Fatalf("publish weather = %q", got)
	}
	if got := sub.recv(t); got != "*(message weather rain)" {
		t.Fatalf("weather delivery = %q", got)
	}

	// Unsubscribe stops delivery.
	sub.send(t, "UNSUBSCRIBE", "weather")
	if got := sub.recv(t); got != "*(unsubscribe weather :2)" {
		t.Fatalf("unsub ack = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "weather", "sun"); got != ":0" {
		t.Fatalf("publish after unsub = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "news", "still"); got != ":1" {
		t.Fatalf("publish news = %q", got)
	}
	if got := sub.recv(t); got != "*(message news still)" {
		t.Fatalf("news delivery = %q", got)
	}
}

func TestM3PubSubRESP3(t *testing.T) {
	addr, _ := m3Server(t)
	sub, pub := m3Dial(t, addr), m3Dial(t, addr)

	if got := sub.do(t, "HELLO", "3"); !strings.HasPrefix(got, "%(") {
		t.Fatalf("HELLO 3 = %q", got)
	}
	// RESP3 acks and messages are push (>) frames.
	if got := sub.do(t, "SUBSCRIBE", "c3"); got != ">(subscribe c3 :1)" {
		t.Fatalf("RESP3 subscribe ack = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "c3", "m3"); got != ":1" {
		t.Fatalf("publish = %q", got)
	}
	if got := sub.recv(t); got != ">(message c3 m3)" {
		t.Fatalf("RESP3 delivery = %q", got)
	}
	// RESP3 connections are not gated: ordinary commands work while
	// subscribed, and PING replies normally.
	if got := sub.do(t, "SET", "k3", "v3"); got != "+OK" {
		t.Fatalf("RESP3 SET in sub mode = %q", got)
	}
	if got := sub.do(t, "PING"); got != "+PONG" {
		t.Fatalf("RESP3 PING in sub mode = %q", got)
	}
	// Self-publish: the :count reply precedes the postponed push.
	sub.send(t, "PUBLISH", "c3", "self")
	if got := sub.recv(t); got != ":1" {
		t.Fatalf("self publish reply = %q", got)
	}
	if got := sub.recv(t); got != ">(message c3 self)" {
		t.Fatalf("self publish push = %q", got)
	}
}

func TestM3PubSubPatterns(t *testing.T) {
	addr, _ := m3Server(t)
	sub, pub := m3Dial(t, addr), m3Dial(t, addr)

	if got := sub.do(t, "PSUBSCRIBE", "news.*"); got != "*(psubscribe news.* :1)" {
		t.Fatalf("psubscribe ack = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "news.tech", "ai"); got != ":1" {
		t.Fatalf("publish = %q", got)
	}
	if got := sub.recv(t); got != "*(pmessage news.* news.tech ai)" {
		t.Fatalf("pmessage = %q", got)
	}
	// A non-matching channel is not delivered.
	if got := pub.do(t, "PUBLISH", "sports.tech", "x"); got != ":0" {
		t.Fatalf("non-matching publish = %q", got)
	}
	// Channel + pattern on the same client: two frames, count 2.
	if got := sub.do(t, "SUBSCRIBE", "news.tech"); got != "*(subscribe news.tech :2)" {
		t.Fatalf("subscribe ack = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "news.tech", "dup"); got != ":2" {
		t.Fatalf("dup publish = %q", got)
	}
	if got := sub.recv(t); got != "*(message news.tech dup)" {
		t.Fatalf("dup message = %q", got)
	}
	if got := sub.recv(t); got != "*(pmessage news.* news.tech dup)" {
		t.Fatalf("dup pmessage = %q", got)
	}
}

func TestM3PubSubTwoSubscribers(t *testing.T) {
	addr, _ := m3Server(t)
	s1, s2, pub := m3Dial(t, addr), m3Dial(t, addr), m3Dial(t, addr)

	s1.do(t, "SUBSCRIBE", "fan")
	s2.do(t, "SUBSCRIBE", "fan")
	if got := pub.do(t, "PUBLISH", "fan", "both"); got != ":2" {
		t.Fatalf("publish = %q", got)
	}
	if got := s1.recv(t); got != "*(message fan both)" {
		t.Fatalf("s1 delivery = %q", got)
	}
	if got := s2.recv(t); got != "*(message fan both)" {
		t.Fatalf("s2 delivery = %q", got)
	}
	// PUBSUB NUMSUB reflects live counts.
	if got := pub.do(t, "PUBSUB", "NUMSUB", "fan"); got != "*(fan :2)" {
		t.Fatalf("NUMSUB = %q", got)
	}
}

func TestM3SubscribeModeGateOverWire(t *testing.T) {
	addr, _ := m3Server(t)
	sub := m3Dial(t, addr)

	sub.do(t, "SUBSCRIBE", "gated")
	want := "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
	if got := sub.do(t, "GET", "x"); got != want {
		t.Fatalf("gate error = %q, want %q", got, want)
	}
	if got := sub.do(t, "PING"); got != "*(pong )" {
		t.Fatalf("PING in sub mode = %q", got)
	}
	// Unsubscribing everything lifts the gate.
	sub.do(t, "UNSUBSCRIBE")
	if got := sub.do(t, "GET", "x"); got != "nil" {
		t.Fatalf("GET after unsub = %q", got)
	}
}

func TestM3PubSubCloseCleanup(t *testing.T) {
	addr, eng := m3Server(t)
	sub, obs := m3Dial(t, addr), m3Dial(t, addr)

	sub.do(t, "SUBSCRIBE", "gone")
	sub.do(t, "PSUBSCRIBE", "gone*")
	if got := obs.do(t, "PUBSUB", "CHANNELS"); got != "*(gone)" {
		t.Fatalf("CHANNELS = %q", got)
	}
	// Closing the socket removes the subscriptions.
	if err := sub.conn.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for eng.PubSub.NumChannels() != 0 || eng.PubSub.NumPat() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriptions not cleaned up after close")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := obs.do(t, "PUBSUB", "CHANNELS"); got != "*()" {
		t.Fatalf("CHANNELS after close = %q", got)
	}
}

func TestM3QuitInPushMode(t *testing.T) {
	addr, _ := m3Server(t)
	sub := m3Dial(t, addr)

	sub.do(t, "SUBSCRIBE", "q")
	// The +OK must be written before the socket closes.
	if got := sub.do(t, "QUIT"); got != "+OK" {
		t.Fatalf("QUIT in push mode = %q", got)
	}
	if err := sub.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.r.ReadByte(); err != io.EOF {
		t.Fatalf("read after QUIT = %v, want EOF", err)
	}
}

// --- M3 blocking commands (BLPOP/BLMOVE/BZPOPMIN) over real sockets ---------

func TestM3BlockWakeOverWire(t *testing.T) {
	addr, _ := m3Server(t)
	a, b := m3Dial(t, addr), m3Dial(t, addr)

	// BLPOP parked on conn A (send only); LPUSH on conn B wakes it.
	a.send(t, "BLPOP", "bk", "2")
	time.Sleep(50 * time.Millisecond) // let A park
	if got := b.do(t, "LPUSH", "bk", "hello"); got != ":1" {
		t.Fatalf("LPUSH = %q", got)
	}
	if got := a.recv(t); got != "*(bk hello)" {
		t.Fatalf("woken BLPOP = %q", got)
	}

	// BZPOPMIN parked; ZADD wakes it.
	a.send(t, "BZPOPMIN", "bz", "2")
	time.Sleep(50 * time.Millisecond)
	if got := b.do(t, "ZADD", "bz", "3.5", "zm"); got != ":1" {
		t.Fatalf("ZADD = %q", got)
	}
	if got := a.recv(t); got != "*(bz zm 3.5)" {
		t.Fatalf("woken BZPOPMIN = %q", got)
	}
}

func TestM3BlockTimeoutOverWire(t *testing.T) {
	addr, _ := m3Server(t)
	a := m3Dial(t, addr)

	start := time.Now()
	if got := a.do(t, "BLPOP", "tk", "0.2"); got != "nil" {
		t.Fatalf("BLPOP timeout = %q", got)
	}
	if d := time.Since(start); d < 180*time.Millisecond {
		t.Fatalf("BLPOP returned too early: %v", d)
	}
}

func TestM3BLMoveOverWire(t *testing.T) {
	addr, _ := m3Server(t)
	a, b := m3Dial(t, addr), m3Dial(t, addr)

	// Parked BLMOVE wakes on push and moves the element.
	a.send(t, "BLMOVE", "msrc", "mdst", "RIGHT", "LEFT", "2")
	time.Sleep(50 * time.Millisecond)
	b.do(t, "RPUSH", "msrc", "m1", "m2")
	if got := a.recv(t); got != "m2" {
		t.Fatalf("woken BLMOVE = %q", got)
	}
	if got := b.do(t, "LRANGE", "mdst", "0", "-1"); got != "*(m2)" {
		t.Fatalf("mdst = %q", got)
	}
	// Immediate path.
	if got := a.do(t, "BRPOPLPUSH", "msrc", "mdst", "0.2"); got != "m1" {
		t.Fatalf("BRPOPLPUSH = %q", got)
	}
	if got := b.do(t, "LRANGE", "mdst", "0", "-1"); got != "*(m1 m2)" {
		t.Fatalf("mdst after BRPOPLPUSH = %q", got)
	}
}

// A command pipelined behind a blocking command is answered after the
// block resolves, in order — the connection goroutine parks inside
// Execute, exactly like Redis.
func TestM3BlockPipelinedBehind(t *testing.T) {
	addr, _ := m3Server(t)
	a, b := m3Dial(t, addr), m3Dial(t, addr)

	a.send(t, "BLPOP", "pk", "2")
	a.send(t, "PING")
	time.Sleep(50 * time.Millisecond)
	b.do(t, "LPUSH", "pk", "pv")
	if got := a.recv(t); got != "*(pk pv)" {
		t.Fatalf("BLPOP reply = %q", got)
	}
	if got := a.recv(t); got != "+PONG" {
		t.Fatalf("pipelined PING = %q", got)
	}
}

// --- M3 stress tests (Ultima-only, real sockets) ----------------------------

// TestM3WatchStress is the M3 exit criterion (design doc §14.4): 8
// connections run 200 iterations each of the optimistic-locking loop
// WATCH counter → GET → MULTI → SET counter+1 → EXEC against ONE shared
// key. An EXEC that commits applies exactly one increment; an EXEC that
// finds the watched key dirty replies with a null array and the iteration
// retries without applying. The final value must therefore be exactly
// 8*200 — anything else means a lost update or a committed transaction
// that should have aborted.
func TestM3WatchStress(t *testing.T) {
	addr, _ := m3Server(t)
	const key = "watch-stress-counter"
	const workers, iters = 8, 200

	init := m3Dial(t, addr)
	if got := init.do(t, "SET", key, "0"); got != "+OK" {
		t.Fatalf("init SET = %q", got)
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := m3DialE(addr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer func() { _ = c.conn.Close() }()
			for i := 0; i < iters; i++ {
				for { // retry until EXEC commits
					if _, err := c.doE("WATCH", key); err != nil {
						t.Errorf("WATCH: %v", err)
						return
					}
					cur, err := c.doE("GET", key)
					if err != nil {
						t.Errorf("GET: %v", err)
						return
					}
					n, err := strconv.Atoi(cur)
					if err != nil {
						t.Errorf("counter value %q: %v", cur, err)
						return
					}
					if _, err := c.doE("MULTI"); err != nil {
						t.Errorf("MULTI: %v", err)
						return
					}
					if got, err := c.doE("SET", key, strconv.Itoa(n+1)); err != nil || got != "+QUEUED" {
						t.Errorf("queued SET = %q, %v", got, err)
						return
					}
					r, err := c.doE("EXEC")
					if err != nil {
						t.Errorf("EXEC: %v", err)
						return
					}
					if r == "nil" { // watched-key conflict: retry
						continue
					}
					if r != "*(+OK)" {
						t.Errorf("EXEC = %q, want *(+OK)", r)
					}
					break
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("WATCH stress did not finish within 60s")
	}
	if got := init.do(t, "GET", key); got != strconv.Itoa(workers*iters) {
		t.Fatalf("counter = %q, want %d", got, workers*iters)
	}
}

// TestM3ExecAtomicityStress asserts cross-shard EXEC atomicity (design
// doc §4.2): writers commit MSET pairs {k1,k2} = {n,n} inside EXEC on two
// keys living on DIFFERENT shards, while readers take transactional
// snapshots with MULTI/MGET/EXEC. A plain MGET is a per-shard fan-out
// and could legitimately observe a pair mid-way between two EXEC
// commits, but a transactional read runs inside the same pause as every
// write — so the two values must ALWAYS be equal. A torn pair here means
// one EXEC interleaved with another's commit.
func TestM3ExecAtomicityStress(t *testing.T) {
	addr, eng := m3Server(t)
	k1, k2 := "atomic-key-one", "atomic-key-two"
	if s1, s2 := eng.Shards.ShardIndex([]byte(k1)), eng.Shards.ShardIndex([]byte(k2)); s1 == s2 {
		t.Fatalf("test keys %q/%q both hash to shard %d; rename one", k1, k2, s1)
	}

	const writers, iters = 4, 100
	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(id int) {
			defer writersWG.Done()
			c, err := m3DialE(addr)
			if err != nil {
				t.Errorf("writer dial: %v", err)
				return
			}
			defer func() { _ = c.conn.Close() }()
			for i := 1; i <= iters; i++ {
				v := strconv.Itoa(id*iters + i)
				if _, err := c.doE("MULTI"); err != nil {
					t.Errorf("writer MULTI: %v", err)
					return
				}
				if got, err := c.doE("MSET", k1, v, k2, v); err != nil || got != "+QUEUED" {
					t.Errorf("writer queued MSET = %q, %v", got, err)
					return
				}
				if got, err := c.doE("EXEC"); err != nil || got != "*(+OK)" {
					t.Errorf("writer EXEC = %q, %v", got, err)
					return
				}
			}
		}(w)
	}

	var writersDone atomic.Bool
	var readIters atomic.Int64
	var readersWG sync.WaitGroup
	for r := 0; r < 2; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			c, err := m3DialE(addr)
			if err != nil {
				t.Errorf("reader dial: %v", err)
				return
			}
			defer func() { _ = c.conn.Close() }()
			for !writersDone.Load() {
				if _, err := c.doE("MULTI"); err != nil {
					t.Errorf("reader MULTI: %v", err)
					return
				}
				if got, err := c.doE("MGET", k1, k2); err != nil || got != "+QUEUED" {
					t.Errorf("reader queued MGET = %q, %v", got, err)
					return
				}
				rep, err := c.doE("EXEC")
				if err != nil {
					t.Errorf("reader EXEC: %v", err)
					return
				}
				body, ok := strings.CutPrefix(rep, "*(*(")
				if !ok || !strings.HasSuffix(body, "))") {
					t.Errorf("reader EXEC reply = %q, want *(*(v v))", rep)
					return
				}
				pair := strings.Split(strings.TrimSuffix(body, "))"), " ")
				if len(pair) != 2 || pair[0] != pair[1] {
					t.Errorf("torn transactional read: MGET %q %q = %q", k1, k2, rep)
					return
				}
				readIters.Add(1)
			}
		}()
	}

	writersCh := make(chan struct{})
	go func() { writersWG.Wait(); close(writersCh) }()
	select {
	case <-writersCh:
	case <-time.After(30 * time.Second):
		t.Fatal("atomicity-stress writers did not finish within 30s")
	}
	writersDone.Store(true)
	readersWG.Wait()
	if n := readIters.Load(); n == 0 {
		t.Fatal("readers performed no iterations; stress was vacuous")
	}
}

// TestM3PubSubFanoutStress guards against broker races and queue-full
// drops: 20 subscribers on one channel, one publisher sending 200
// numbered messages; every subscriber must receive all 200 in publish
// order.
func TestM3PubSubFanoutStress(t *testing.T) {
	addr, _ := m3Server(t)
	const subs, msgs = 20, 200

	// Subscribe on the main goroutine so acks are confirmed before the
	// first publish (no missed messages by construction).
	clients := make([]*m3Client, subs)
	for i := range clients {
		clients[i] = m3Dial(t, addr)
		if got := clients[i].do(t, "SUBSCRIBE", "fanout"); got != "*(subscribe fanout :1)" {
			t.Fatalf("subscriber %d ack = %q", i, got)
		}
	}

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *m3Client) {
			defer wg.Done()
			for i := 1; i <= msgs; i++ {
				got, err := c.recvE()
				if err != nil {
					t.Errorf("subscriber recv %d: %v", i, err)
					return
				}
				if want := fmt.Sprintf("*(message fanout %d)", i); got != want {
					t.Errorf("subscriber message %d = %q, want %q", i, got, want)
					return
				}
			}
		}(c)
	}

	pub := m3Dial(t, addr)
	for i := 1; i <= msgs; i++ {
		if got := pub.do(t, "PUBLISH", "fanout", strconv.Itoa(i)); got != ":20" {
			t.Fatalf("publish %d = %q, want :20", i, got)
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("pub/sub fan-out stress did not finish within 30s")
	}
}
