// M8 Lua scripting end-to-end tests (design doc §7 P3, D12): EVAL over
// all three surfaces, the BUSY phase + SCRIPT KILL, the hard watchdog
// deadline (decision S5), the per-VM memory cap, and -race concurrency
// of mixed EVAL/normal traffic under PauseAll.
package tests

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/scripting"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

// m8Engine builds a command engine with the scripting manager installed.
func m8Engine(t *testing.T, cfg scripting.Config) *commands.Engine {
	t.Helper()
	shards := shard.NewEngine(4, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	if cfg.LuaTimeLimitMs == 0 {
		cfg.LuaTimeLimitMs = 5000
	}
	if cfg.CompatVersion == "" {
		cfg.CompatVersion = commands.CompatVersion
	}
	// M8e R3: the pool on by default in tests (the unit suite covers the
	// disabled path).
	if cfg.VMPoolSize == 0 {
		cfg.VMPoolSize = 1
		cfg.VMPoolMax = 64
		cfg.VMRecycleRuns = 100
		cfg.VMRecyclePct = 75
	}
	cfg.RunID = eng.RunID
	m, err := scripting.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	eng.Scripts = m
	return eng
}

// m8RESP boots the RESP surface on an engine with scripting.
func m8RESP(t *testing.T, cfg scripting.Config) (string, *commands.Engine) {
	t.Helper()
	lis := listen(t, "127.0.0.1:0")
	eng := m8Engine(t, cfg)
	srv := respserver.New("127.0.0.1:0", eng)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return lis.Addr().String(), eng
}

func TestM8EvalRESP(t *testing.T) {
	addr, _ := m8RESP(t, scripting.Config{})
	c := m3Dial(t, addr)
	if got := c.do(t, "EVAL", "return 1+1", "0"); got != ":2" {
		t.Errorf("EVAL 1+1 = %q, want :2", got)
	}
	if got := c.do(t, "EVAL", "return redis.call('set',KEYS[1],ARGV[1])", "1", "k", "v"); got != "+OK" {
		t.Errorf("script set = %q, want +OK", got)
	}
	if got := c.do(t, "GET", "k"); got != "v" {
		t.Errorf("GET k = %q, want v", got)
	}
}

func TestM8EvalGRPC(t *testing.T) {
	eng := m8Engine(t, scripting.Config{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpcsrv.New(eng, nil)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	t.Cleanup(func() { _ = lis.Close() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := ultimav1.NewUltimaClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.ExecGeneric(ctx, &ultimav1.CommandRequest{Command: "eval", Args: [][]byte{
		[]byte("return redis.call('set',KEYS[1],ARGV[1])"), []byte("1"), []byte("gk"), []byte("gv")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Reply.GetSimpleString(); got != "OK" {
		t.Errorf("gRPC eval set = %q, want OK", got)
	}
	r, err = c.ExecGeneric(ctx, &ultimav1.CommandRequest{Command: "eval", Args: [][]byte{
		[]byte("return redis.call('get','gk')"), []byte("0")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(r.Reply.GetBlobString()); got != "gv" {
		t.Errorf("gRPC eval get = %q, want gv", got)
	}
}

func TestM8EvalWS(t *testing.T) {
	eng := m8Engine(t, scripting.Config{})
	reg := wssession.NewRegistry(eng, 0, 0, testLogger())
	t.Cleanup(reg.Close)
	r := chi.NewRouter()
	r.Get("/ws/v1", wssrv.Handler(eng, nil, reg, testLogger(), nil))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	c := wsDial(t, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws/v1")

	replies := wsRoundTrip(t, c,
		wsGeneric("eval", "return redis.call('set',KEYS[1],ARGV[1])", "1", "wk", "wv"),
		wsGeneric("eval", "return 1+1", "0"),
		wsGeneric("get", "wk"),
	)
	if got := replies[0].Reply.GetSimpleString(); got != "OK" {
		t.Errorf("WS eval set = %q, want OK", got)
	}
	if got := replies[1].Reply.GetInt(); got != 2 {
		t.Errorf("WS eval 1+1 = %d, want 2", got)
	}
	if got := string(replies[2].Reply.GetBlobString()); got != "wv" {
		t.Errorf("WS get wk = %q, want wv", got)
	}
}

// m8WaitBusy polls until the engine reports BUSY (a script running past
// its soft lua-time-limit). Fixed sleeps race -race runs: wasm VM
// instantiation slows enough that a script may not even have STARTED at
// the old 400 ms marks. The probe is PING, never GET: a data command
// sent BEFORE the gate trips parks in the paused shard queue (the
// script holds PauseAll) and would never be answered — PING runs inline
// and gets "-BUSY" once the gate trips.
func m8WaitBusy(t *testing.T, c *m3Client) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		c.send(t, "PING")
		if got := c.recv(t); strings.HasPrefix(got, "-BUSY") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("engine never reported BUSY")
}

func TestM8BusyAndScriptKill(t *testing.T) {
	addr, _ := m8RESP(t, scripting.Config{LuaTimeLimitMs: 100, HardDeadlineMs: 10000})
	c1 := m3Dial(t, addr)
	c2 := m3Dial(t, addr)

	c1.send(t, "EVAL", "while true do end", "0") // parks until killed
	m8WaitBusy(t, c2)

	c2.send(t, "SCRIPT", "KILL")
	if got := c2.recv(t); got != "+OK" {
		t.Errorf("SCRIPT KILL = %q, want OK", got)
	}
	got := c1.recv(t) // the scripting client sees the kill
	want := "-ERR Script killed by user with SCRIPT KILL... script: 694a5fe1ddb97a4c6a1bf299d9537c7d3d0f84e7, on @user_script:1."
	if got != want {
		t.Errorf("killed script reply = %q, want %q", got, want)
	}
	// Engine is healthy afterwards.
	if got := c2.do(t, "SET", "after", "1"); got != "+OK" {
		t.Errorf("SET after kill = %q", got)
	}
	c2.send(t, "SCRIPT", "KILL")
	if got := c2.recv(t); got != "-NOTBUSY No scripts in execution right now." {
		t.Errorf("SCRIPT KILL idle = %q", got)
	}
}

func TestM8UnkillableAfterWrite(t *testing.T) {
	addr, _ := m8RESP(t, scripting.Config{LuaTimeLimitMs: 100, HardDeadlineMs: 800})
	c1 := m3Dial(t, addr)
	c2 := m3Dial(t, addr)

	c1.send(t, "EVAL", "redis.call('set','wk','1') while true do end", "0")
	m8WaitBusy(t, c2)

	c2.send(t, "SCRIPT", "KILL")
	got := c2.recv(t)
	want := "-UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command."
	if got != want {
		t.Errorf("SCRIPT KILL after write = %q", got)
	}
	// S5: the hard deadline still bounds the run — the write persists.
	got = c1.recv(t)
	if !strings.Contains(got, "hard execution deadline") {
		t.Errorf("watchdog reply = %q, want the hard-deadline text", got)
	}
	c2.send(t, "GET", "wk")
	if got := c2.recv(t); got != "1" {
		t.Errorf("partial effect kept = %q, want 1 (S5)", got)
	}
}

func TestM8HardDeadline(t *testing.T) {
	addr, _ := m8RESP(t, scripting.Config{HardDeadlineMs: 500})
	c := m3Dial(t, addr)
	start := time.Now()
	c.send(t, "EVAL", "local i=0 while true do i=i+1 end return i", "0")
	got := c.recv(t)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("watchdog took %v", elapsed)
	}
	if !strings.Contains(got, "ERR Script killed by the hard execution deadline") {
		t.Errorf("deadline reply = %q", got)
	}
	c.send(t, "PING")
	if got := c.recv(t); got != "+PONG" {
		t.Errorf("PING after deadline = %q", got)
	}
}

func TestM8MemoryCap(t *testing.T) {
	addr, _ := m8RESP(t, scripting.Config{MaxMemoryMB: 8, HardDeadlineMs: 10000})
	c := m3Dial(t, addr)
	c.send(t, "EVAL", "local s='x' while true do s=s..s end", "0")
	got := c.recv(t)
	if !strings.Contains(got, "not enough memory") {
		t.Errorf("memory cap reply = %q", got)
	}
	c.send(t, "SET", "alive", "1") // the daemon is unaffected
	if got := c.recv(t); got != "+OK" {
		t.Errorf("SET after OOM script = %q", got)
	}
}

// CLIENT KILL of a scripting connection mid-run (M8d): the kill closes
// the socket but does NOT interrupt the script (Redis semantics — only
// SCRIPT KILL interrupts, and a write-having script is UNKILLABLE). The
// run continues to the hard deadline; effects before the kill persist
// (S5); the handler goroutine exits when the run ends. RESP has no
// CLIENT KILL subcommand (the management API's POST /clients/{id}/kill
// rides the same eng.KillClient), so the test calls the hook directly.
func TestM8ClientKillMidScript(t *testing.T) {
	addr, eng := m8RESP(t, scripting.Config{LuaTimeLimitMs: 100, HardDeadlineMs: 600})
	c1 := m3Dial(t, addr)
	c2 := m3Dial(t, addr)

	c1.send(t, "CLIENT", "ID")
	var id uint64
	if _, err := fmt.Sscanf(c1.recv(t), ":%d", &id); err != nil || id == 0 {
		t.Fatalf("CLIENT ID: %v", err)
	}

	c1.send(t, "EVAL", "redis.call('set','kk','1') while true do end", "0")
	m8WaitBusy(t, c2) // parked in the loop, past the soft limit

	found, killable := eng.KillClient(id)
	if !found || !killable {
		t.Errorf("KillClient(%d) = %v, %v", id, found, killable)
	}
	// the scripting connection is closed: its read end sees EOF
	_ = c1.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c1.r.ReadByte(); err == nil {
		t.Errorf("killed connection still readable")
	}

	// the BUSY gate is still up: the (unkillable) script keeps running
	c2.send(t, "GET", "anything")
	if got := c2.recv(t); !strings.HasPrefix(got, "-BUSY") {
		t.Errorf("GET during zombie script = %q, want BUSY", got)
	}

	// the hard deadline retires the script; the pre-kill write persists
	time.Sleep(800 * time.Millisecond)
	if got := c2.do(t, "GET", "kk"); got != "1" {
		t.Errorf("partial effect after kill = %q, want 1 (S5)", got)
	}
	if got := c2.do(t, "PING"); got != "+PONG" {
		t.Errorf("PING after kill = %q", got)
	}
}

func TestM8ConcurrentEval(t *testing.T) {
	addr, _ := m8RESP(t, scripting.Config{})
	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()
			c, err := m3DialE(addr)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = c.conn.Close() }()
			for i := 0; i < perWorker; i++ {
				// Mix script writes, plain writes, and pure scripts.
				if err := c.sendE("EVAL", "return redis.call('incr','ctr')", "0"); err != nil {
					errs <- err
					return
				}
				if _, err := c.recvE(); err != nil {
					errs <- err
					return
				}
				if err := c.sendE("EVAL", "return {KEYS[1],ARGV[1]}", "1", "k", fmt.Sprintf("a%d", i)); err != nil {
					errs <- err
					return
				}
				if _, err := c.recvE(); err != nil {
					errs <- err
					return
				}
				if err := c.sendE("INCR", "plain"); err != nil {
					errs <- err
					return
				}
				if _, err := c.recvE(); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	c := m3Dial(t, addr)
	c.send(t, "GET", "ctr")
	if got := c.recv(t); got != fmt.Sprintf("%d", workers*perWorker) {
		t.Errorf("ctr = %q, want %d (atomic script increments)", got, workers*perWorker)
	}
	c.send(t, "GET", "plain")
	if got := c.recv(t); got != fmt.Sprintf("%d", workers*perWorker) {
		t.Errorf("plain = %q, want %d", got, workers*perWorker)
	}
}
