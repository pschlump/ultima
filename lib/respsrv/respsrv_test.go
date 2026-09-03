package respsrv

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
)

// startTestServer launches a RESP server on an ephemeral loopback port and
// returns its address.
func startTestServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		_ = srv.Close()
	})
	return lis.Addr().String()
}

// roundTrip sends raw bytes and reads one reply line (or two for bulk
// echoes) verbatim.
func roundTrip(t *testing.T, addr, send string, lines int) []string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, send); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	out := make([]string, 0, lines)
	for range lines {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	return out
}

func TestInlinePing(t *testing.T) {
	addr := startTestServer(t)
	got := roundTrip(t, addr, "PING\r\n", 1)
	if got[0] != "+PONG\r\n" {
		t.Errorf("inline PING reply = %q, want +PONG\\r\\n", got[0])
	}
}

func TestMultibulkPing(t *testing.T) {
	addr := startTestServer(t)
	got := roundTrip(t, addr, "*1\r\n$4\r\nPING\r\n", 1)
	if got[0] != "+PONG\r\n" {
		t.Errorf("multibulk PING reply = %q, want +PONG\\r\\n", got[0])
	}
}

func TestPingEcho(t *testing.T) {
	addr := startTestServer(t)
	got := roundTrip(t, addr, "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n", 2)
	if got[0] != "$5\r\n" || got[1] != "hello\r\n" {
		t.Errorf("PING hello reply = %q, want bulk echo of hello", got)
	}
}

func TestUnknownCommand(t *testing.T) {
	addr := startTestServer(t)
	got := roundTrip(t, addr, "FLUSHALL\r\n", 1)
	if !strings.HasPrefix(got[0], "-ERR unknown command") {
		t.Errorf("FLUSHALL reply = %q, want -ERR unknown command", got[0])
	}
}

func TestLowercasePing(t *testing.T) {
	addr := startTestServer(t)
	got := roundTrip(t, addr, "ping\r\n", 1)
	if got[0] != "+PONG\r\n" {
		t.Errorf("lowercase ping reply = %q, want +PONG\\r\\n", got[0])
	}
}
