// M7a CLI smoke tests (§6.4): the three operator CLIs are built with
// `go build` into a temp dir and run against a real in-process server —
// one-shot commands (stdout rendering + exit codes, redis-cli-style) and
// piped REPL sessions over stdin, plus a SUBSCRIBE streaming session
// ended by SIGINT.
package tests

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/httpapi"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

// m7Server boots all three surfaces on ephemeral ports (the
// integration_test.go wiring) for the CLI subprocesses.
type m7Server struct {
	respAddr string
	grpcAddr string
	httpAddr string
}

func newM7Server(t *testing.T) *m7Server {
	t.Helper()
	logger := testLogger()
	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)

	respLis := listen(t, "127.0.0.1:0")
	respSrv := respserver.New(respLis.Addr().String(), eng)
	go func() { _ = respSrv.Serve(respLis) }()
	t.Cleanup(func() { _ = respSrv.Close() })

	grpcLis := listen(t, "127.0.0.1:0")
	grpcSrv := grpcsrv.New(eng, nil)
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	t.Cleanup(grpcSrv.GracefulStop)

	httpLis := listen(t, "127.0.0.1:0")
	r := chi.NewRouter()
	httpapi.NewServer(eng, nil, nil, logger, nil).Register(r)
	reg := wssession.NewRegistry(eng, 0, 0, logger)
	t.Cleanup(reg.Close)
	r.Get("/ws/v1", wssrv.Handler(eng, nil, reg, logger, nil))
	httpSrv := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = httpSrv.Serve(httpLis) }()
	t.Cleanup(func() { _ = httpSrv.Shutdown(context.Background()) })

	return &m7Server{
		respAddr: respLis.Addr().String(),
		grpcAddr: grpcLis.Addr().String(),
		httpAddr: httpLis.Addr().String(),
	}
}

// buildCLIs compiles the three CLI binaries into a temp dir.
func buildCLIs(t *testing.T) map[string]string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	out := map[string]string{}
	for _, name := range []string{"ultima-cli", "ultima-ws-cli", "ultima-grpc-cli"} {
		dst := filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", dst, "./cmd/"+name)
		cmd.Dir = root
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %s\n%s", name, err, b)
		}
		out[name] = dst
	}
	return out
}

// runCLI runs one CLI subprocess with the given stdin and returns
// stdout, stderr and the exit code.
func runCLI(t *testing.T, stdin string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !asExitError(err, &ee) {
			t.Fatalf("run %v: %s", args, err)
		}
		code = ee.ExitCode()
	}
	return outBuf.String(), errBuf.String(), code
}

func asExitError(err error, ee **exec.ExitError) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*ee = e
		return true
	}
	return false
}

func splitHostPort(t *testing.T, addr string) (string, string) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

func TestM7CLI(t *testing.T) {
	srv := newM7Server(t)
	bins := buildCLIs(t)
	host, port := splitHostPort(t, srv.respAddr)

	// --- one-shot over RESP ---
	stdout, _, code := runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "SET", "m7:cli:k", "hello")
	if code != 0 || stdout != "OK\n" {
		t.Fatalf("SET one-shot: code=%d stdout=%q", code, stdout)
	}
	stdout, _, code = runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "GET", "m7:cli:k")
	if code != 0 || stdout != "\"hello\"\n" {
		t.Fatalf("GET one-shot: code=%d stdout=%q", code, stdout)
	}
	stdout, _, code = runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "GET", "m7:cli:missing")
	if code != 0 || stdout != "(nil)\n" {
		t.Fatalf("GET missing: code=%d stdout=%q", code, stdout)
	}
	// Error reply: printed to stderr, exit 1 (redis-cli convention).
	_, stderr, code := runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "INCR", "m7:cli:k")
	if code != 1 || !strings.Contains(stderr, "(error) ERR value is not an integer") {
		t.Fatalf("INCR on string: code=%d stderr=%q", code, stderr)
	}
	// Array rendering with numbering.
	if _, _, code := runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "RPUSH", "m7:cli:l", "a", "b"); code != 0 {
		t.Fatalf("RPUSH: code=%d", code)
	}
	stdout, _, _ = runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "LRANGE", "m7:cli:l", "0", "-1")
	if stdout != "1) \"a\"\n2) \"b\"\n" {
		t.Fatalf("LRANGE stdout = %q", stdout)
	}
	// -n selects the DB.
	stdout, _, _ = runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "-n", "5", "DBSIZE")
	if stdout != "(integer) 0\n" {
		t.Fatalf("DBSIZE on db 5 = %q, want empty", stdout)
	}
	// Flag parsing stops at the command name (redis-cli semantics):
	// `ZRANGE k -3 -1` must not eat "-3" as the RESP3 flag.
	if _, _, code := runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "ZADD", "m7:cli:z", "1", "a", "2", "b", "3", "c"); code != 0 {
		t.Fatalf("ZADD: code=%d", code)
	}
	stdout, _, code = runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "ZRANGE", "m7:cli:z", "-3", "-1", "WITHSCORES")
	if code != 0 || !strings.Contains(stdout, `"c"`) {
		t.Fatalf("ZRANGE with negative offsets: code=%d stdout=%q", code, stdout)
	}
	// And the -3 flag still works BEFORE the command.
	stdout, _, code = runCLI(t, "", bins["ultima-cli"], "-h", host, "-p", port, "-3", "PING")
	if code != 0 || stdout != "PONG\n" {
		t.Fatalf("-3 PING: code=%d stdout=%q", code, stdout)
	}

	// --- piped REPL over RESP ---
	stdout, _, code = runCLI(t, "SET m7:cli:r 1\nINCR m7:cli:r\n\nGET m7:cli:r\nquit\nGET ignored\n",
		bins["ultima-cli"], "-h", host, "-p", port)
	if code != 0 {
		t.Fatalf("REPL exit code = %d", code)
	}
	if stdout != "OK\n(integer) 2\n\"2\"\n" {
		t.Fatalf("REPL stdout = %q", stdout)
	}

	// --- WS CLI ---
	stdout, _, code = runCLI(t, "", bins["ultima-ws-cli"], "-addr", srv.httpAddr, "GET", "m7:cli:k")
	if code != 0 || stdout != "\"hello\"\n" {
		t.Fatalf("ws GET one-shot: code=%d stdout=%q", code, stdout)
	}
	stdout, _, code = runCLI(t, "SET m7:cli:w 7\nINCR m7:cli:w\nexit\n",
		bins["ultima-ws-cli"], "-addr", srv.httpAddr)
	if code != 0 || stdout != "OK\n(integer) 8\n" {
		t.Fatalf("ws REPL: code=%d stdout=%q", code, stdout)
	}

	// --- gRPC CLI ---
	stdout, _, code = runCLI(t, "", bins["ultima-grpc-cli"], "-addr", srv.grpcAddr, "GET", "m7:cli:k")
	if code != 0 || stdout != "\"hello\"\n" {
		t.Fatalf("grpc GET one-shot: code=%d stdout=%q", code, stdout)
	}
	stdout, _, code = runCLI(t, "SET m7:cli:g 3\nDECR m7:cli:g\nquit\n",
		bins["ultima-grpc-cli"], "-addr", srv.grpcAddr)
	if code != 0 || stdout != "OK\n(integer) 2\n" {
		t.Fatalf("grpc REPL: code=%d stdout=%q", code, stdout)
	}
}

// TestM7CLISubscribeStreaming: SUBSCRIBE puts the CLI in streaming mode
// printing pushes; SIGINT ends it cleanly.
func TestM7CLISubscribeStreaming(t *testing.T) {
	srv := newM7Server(t)
	bins := buildCLIs(t)
	host, port := splitHostPort(t, srv.respAddr)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"resp", []string{bins["ultima-cli"], "-h", host, "-p", port}},
		{"ws", []string{bins["ultima-ws-cli"], "-addr", srv.httpAddr}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(tc.args, "SUBSCRIBE", "m7:cli:stream:"+tc.name)
			cmd := exec.Command(args[0], args[1:]...)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			lines := make(chan string, 16)
			go func() {
				sc := bufio.NewScanner(stdout)
				for sc.Scan() {
					lines <- sc.Text()
				}
				close(lines)
			}()

			// The subscribe ack prints as a 3-element push frame.
			for _, want := range []string{`1) "subscribe"`, fmt.Sprintf(`2) "m7:cli:stream:%s"`, tc.name), "3) (integer) 1"} {
				select {
				case got := <-lines:
					if got != want {
						t.Fatalf("ack line = %q, want %q", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("waiting for %q: no output", want)
				}
			}

			// Publish from a separate RESP connection.
			pub, err := net.Dial("tcp", srv.respAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pub.Close() }()
			if _, err := fmt.Fprintf(pub, "*3\r\n$7\r\nPUBLISH\r\n$%d\r\n%s\r\n$5\r\nhello\r\n",
				len("m7:cli:stream:"+tc.name), "m7:cli:stream:"+tc.name); err != nil {
				t.Fatal(err)
			}
			// Drain the PUBLISH reply.
			if _, err := bufio.NewReader(pub).ReadString('\n'); err != nil {
				t.Fatal(err)
			}

			for _, want := range []string{`1) "message"`, fmt.Sprintf(`2) "m7:cli:stream:%s"`, tc.name), `3) "hello"`} {
				select {
				case got := <-lines:
					if got != want {
						t.Fatalf("message line = %q, want %q", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("waiting for %q: no output", want)
				}
			}

			// Ctrl-C ends streaming mode with exit code 0.
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			waitErr := make(chan error, 1)
			go func() { waitErr <- cmd.Wait() }()
			select {
			case err := <-waitErr:
				if err != nil {
					t.Fatalf("CLI after SIGINT: %v", err)
				}
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatal("CLI did not exit after SIGINT")
			}
		})
	}
}
