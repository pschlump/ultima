// Package tests holds the M0 integration test: it boots all three listener
// surfaces on ephemeral ports and verifies PING on each.
//
// Port approach: addresses come from the config (server.resp_addr /
// grpc_addr / http_addr), so tests set them to "127.0.0.1:0" and read the
// bound address back off the net.Listener. cmd/ultima-server is not
// importable, so this test wires the same lib packages (respsrv, grpcsrv,
// handler on a chi mux) exactly as cmd/ultima-server/{startup,router}.go
// do; the real binary is additionally smoke-tested by hand each milestone.
package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/handler"
	"github.com/pschlump/ultima/lib/respsrv"
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

	// --- RESP surface ---
	respLis := listen(t, cfg.Server.RespAddr)
	respSrv := respsrv.New(logger)
	go func() { _ = respSrv.Serve(respLis) }()
	t.Cleanup(func() { _ = respSrv.Close() })

	conn, err := net.Dial("tcp", respLis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		t.Fatal(err)
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if reply != "+PONG\r\n" {
		t.Errorf("RESP PING reply = %q, want +PONG\\r\\n", reply)
	}

	// --- gRPC surface ---
	grpcLis := listen(t, cfg.Server.GrpcAddr)
	grpcSrv := grpcsrv.New()
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
	r.Use(middleware.Recoverer)
	r.Use(handler.RequestLogger(logger))
	handler.Register(r, logger)
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

	// --- WS stub on the same port ---
	wsURL := "ws://" + httpLis.Addr().String() + "/ws/v1"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Close() }()
	if err := ws.WriteMessage(websocket.TextMessage, []byte("PING")); err != nil {
		t.Fatal(err)
	}
	_, pong, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(pong) != "PONG" {
		t.Errorf("WS PING reply = %q, want PONG", pong)
	}
}
