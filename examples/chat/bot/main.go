// Command bot is the chat example's companion (design doc §11.4 #2): it
// joins chat:lobby, posts a welcome message, answers !ping and !time,
// enables the keyspace notifications the page's presence list needs, and
// serves the chat page from the embedded static/ tree — one command runs
// the whole demo:
//
//	go run ./examples/chat/bot   # then open http://127.0.0.1:8091
package main

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pschlump/ultima/clients/go/ultima"
	"github.com/pschlump/ultima/examples/chat/botcore"
)

//go:embed static
var staticFS embed.FS

func main() {
	addr := flag.String("addr", "127.0.0.1:6381", "Ultima HTTP/WS address (host:port)")
	httpAddr := flag.String("http", ":8091", "address to serve the chat page on")
	flag.Parse()

	// OnPush runs on the client's read loop, so the reply round-trips must
	// happen on a separate goroutine (handling inline would deadlock). The
	// handler references the client itself, hence the atomic hand-off.
	var wsp atomic.Pointer[ultima.WSClient]
	client := ultima.New(ultima.Options{
		HTTPAddr: *addr,
		OnPush: func(v ultima.Value, _ uint64) {
			parts, ok := ultima.AsStringSlice(v)
			if !ok || len(parts) != 3 || parts[0] != "message" || parts[1] != botcore.Channel {
				return
			}
			w := wsp.Load()
			if w == nil {
				return
			}
			go func() {
				if err := botcore.Handle(w, time.Now(), []byte(parts[2])); err != nil {
					log.Printf("handle: %s", err)
				}
			}()
		},
	})
	defer func() { _ = client.Close() }()
	ws, err := client.WS()
	if err != nil {
		log.Fatalf("connect ws://%s/ws/v1: %s", *addr, err)
	}
	wsp.Store(ws)
	log.Printf("chat bot via %s", ws)

	// The page's presence list renders departures from expired presence keys.
	if v, err := ws.Exec("CONFIG", "SET", "notify-keyspace-events", "Ex"); err != nil || ultima.IsError(v) {
		log.Printf("warning: could not enable keyspace notifications, presence departures will not show (%s, %s)", v.Str, err)
	} else {
		log.Printf("enabled notify-keyspace-events=Ex (presence expiry events)")
	}

	if _, err := ws.Subscribe(botcore.Channel); err != nil {
		log.Fatalf("subscribe %s: %s", botcore.Channel, err)
	}
	if err := botcore.Post(ws, botcore.Message{
		User: "bot",
		Text: "welcome to " + botcore.Channel + " — try !ping or !time",
		Ts:   time.Now().UnixMilli(),
	}); err != nil {
		log.Fatalf("welcome: %s", err)
	}
	log.Printf("joined %s", botcore.Channel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pages, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("embed: %s", err)
	}
	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           http.FileServer(http.FS(pages)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Printf("chat page on http://127.0.0.1%s/ (Ultima at %s)", *httpAddr, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %s", err)
	}
}
