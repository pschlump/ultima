// Package tests holds the integration test: it boots all three listener
// surfaces on ephemeral ports and verifies PING on each, plus a P0
// command round-trip over the RESP surface (M1).
//
// Port approach: addresses come from the config (server.resp_addr /
// grpc_addr / http_addr), so tests set them to "127.0.0.1:0" and read the
// bound address back off the net.Listener. cmd/ultima-server is not
// importable, so this test wires the same lib packages (respserver,
// grpcsrv, handler on a chi mux) exactly as cmd/ultima-server/
// {startup,router}.go do; the real binary is additionally smoke-tested
// by hand each milestone.
package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/httpapi"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// testConfig mirrors production startup: defaults from SetDefaults, then
// ephemeral loopback addresses substituted for the three surfaces.
func testConfig(t *testing.T) *config.Config {
	t.Helper()
	var cfg config.Config
	if err := config.SetDefaults(&cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Server.RespAddr = "127.0.0.1:0"
	cfg.Server.GrpcAddr = "127.0.0.1:0"
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	return &cfg
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return lis
}

func TestPingAllThreeSurfaces(t *testing.T) {
	cfg := testConfig(t)
	logger := testLogger()

	// --- RESP surface: shard engine + command engine + resp fork ---
	respLis := listen(t, cfg.Server.RespAddr)
	shards := shard.NewEngine(cfg.Server.ShardCount, cfg.Server.MaxDBs)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	respSrv := respserver.New(cfg.Server.RespAddr, eng)
	go func() { _ = respSrv.Serve(respLis) }()
	t.Cleanup(func() { _ = respSrv.Close() })

	conn, err := net.Dial("tcp", respLis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	rdr := bufio.NewReader(conn)

	// readReply consumes exactly one RESP2/RESP3 reply and returns its
	// first line (for prefix assertions).
	readReply := func() string {
		t.Helper()
		var readVal func() string
		readVal = func() string {
			line, err := rdr.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			switch line[0] {
			case '$', '=': // bulk / verbatim: payload follows
				n, err := strconv.Atoi(strings.TrimRight(line[1:], "\r\n"))
				if err != nil {
					t.Fatal(err)
				}
				if n >= 0 {
					if _, err := rdr.Discard(n + 2); err != nil {
						t.Fatal(err)
					}
				}
			case '*', '%', '~', '>': // aggregate: count elements follow
				n, err := strconv.Atoi(strings.TrimRight(line[1:], "\r\n"))
				if err != nil {
					t.Fatal(err)
				}
				if line[0] == '%' {
					n *= 2
				}
				for i := 0; i < n; i++ {
					readVal()
				}
			}
			return line
		}
		return readVal()
	}

	sendRecv := func(req, wantPrefix string) string {
		t.Helper()
		if _, err := io.WriteString(conn, req); err != nil {
			t.Fatal(err)
		}
		reply := readReply()
		if !strings.HasPrefix(reply, wantPrefix) {
			t.Errorf("req %q reply = %q, want prefix %q", req, reply, wantPrefix)
		}
		return reply
	}

	// multibulk PING (what redis-cli sends) and inline/telnet PING
	sendRecv("*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	sendRecv("PING\r\n", "+PONG\r\n")
	// P0 round-trip: SET with PX, GET, INCR, TYPE
	sendRecv("*5\r\n$3\r\nSET\r\n$2\r\nit\r\n$2\r\n42\r\n$2\r\nPX\r\n$5\r\n60000\r\n", "+OK\r\n")
	sendRecv("*2\r\n$4\r\nINCR\r\n$2\r\nit\r\n", ":43\r\n")
	sendRecv("*2\r\n$3\r\nGET\r\n$2\r\nit\r\n", "$2\r\n")
	sendRecv("*2\r\n$4\r\nPTTL\r\n$2\r\nit\r\n", ":")
	sendRecv("*2\r\n$4\r\nTYPE\r\n$2\r\nit\r\n", "+string\r\n")
	// RESP3 negotiation
	if reply := sendRecv("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n", "%"); !strings.Contains(reply, "7") {
		t.Errorf("HELLO 3 map header = %q", reply)
	}

	// --- gRPC surface ---
	grpcLis := listen(t, cfg.Server.GrpcAddr)
	grpcSrv := grpcsrv.New(eng, nil)
	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()
	t.Cleanup(grpcSrv.GracefulStop)

	gconn, err := grpc.NewClient(grpcLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gconn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := ultimav1.NewUltimaClient(gconn).Ping(ctx, &ultimav1.PingRequest{Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetMessage() != "hello" {
		t.Errorf("gRPC Ping message = %q, want echo of hello", resp.GetMessage())
	}
	resp, err = ultimav1.NewUltimaClient(gconn).Ping(ctx, &ultimav1.PingRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetMessage() != "PONG" {
		t.Errorf("gRPC bare Ping message = %q, want PONG", resp.GetMessage())
	}

	// --- HTTP/WS surface ---
	httpLis := listen(t, cfg.Server.HTTPAddr)
	r := chi.NewRouter()
	// No persistence manager or auth service in the surface test; the
	// management API mounts its own middleware chain (§10.1).
	httpapi.NewServer(eng, nil, nil, logger, nil).Register(r)
	reg := wssession.NewRegistry(eng, 0, 0, logger)
	t.Cleanup(reg.Close)
	r.Get("/ws/v1", wssrv.Handler(eng, nil, reg, logger, nil))
	httpSrv := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := httpSrv.Serve(httpLis); err != nil && err != http.ErrServerClosed {
			t.Errorf("http serve: %v", err)
		}
	}()
	t.Cleanup(func() { _ = httpSrv.Shutdown(context.Background()) })

	base := "http://" + httpLis.Addr().String()
	hresp, err := http.Get(base + "/api/v1/ping") //nolint:bodyclose // closed below
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hresp.Body.Close() }()
	if hresp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/ping status = %d, want 200", hresp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(hresp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["message"] != "PONG" {
		t.Errorf("GET /api/v1/ping message = %q, want PONG", body["message"])
	}

	for _, path := range []string{"/health", "/ready"} {
		hr, err := http.Get(base + path) //nolint:bodyclose // closed inline
		if err != nil {
			t.Fatal(err)
		}
		_ = hr.Body.Close()
		if hr.StatusCode != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, hr.StatusCode)
		}
	}

	// --- WS command endpoint on the same port (binary protobuf, §6.3) ---
	wsURL := "ws://" + httpLis.Addr().String() + "/ws/v1"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Close() }()
	frame, err := proto.Marshal(&ultimav1.Command{Cmd: &ultimav1.Command_Set{
		Set: &ultimav1.SetCommand{Key: []byte("ws:it"), Value: []byte("v")}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatal(err)
	}
	_, payload, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var wsReply ultimav1.CommandResponse
	if err := proto.Unmarshal(payload, &wsReply); err != nil {
		t.Fatal(err)
	}
	if got := wsReply.Reply.GetSimpleString(); got != "OK" {
		t.Errorf("WS SET reply = %q, want OK", got)
	}
}
