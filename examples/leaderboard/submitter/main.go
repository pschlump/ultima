// Command submitter is the leaderboard example's companion (design doc
// §11.4 #1): it simulates score submissions against a running ultima-server
// over /ws/v1 (clients/go) and serves the live leaderboard page from the
// embedded static/ tree — one command runs the whole demo:
//
//	go run ./examples/leaderboard/submitter   # then open http://127.0.0.1:8090
package main

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pschlump/ultima/clients/go/ultima"
	"github.com/pschlump/ultima/examples/leaderboard/submit"
)

//go:embed static
var staticFS embed.FS

func main() {
	addr := flag.String("addr", "127.0.0.1:6381", "Ultima HTTP/WS address (host:port)")
	rate := flag.Float64("rate", 5, "score submissions per second")
	players := flag.Int("players", 20, "simulated player pool size")
	httpAddr := flag.String("http", ":8090", "address to serve the leaderboard page on")
	flag.Parse()

	client := ultima.New(ultima.Options{HTTPAddr: *addr})
	defer func() { _ = client.Close() }()
	ws, err := client.WS()
	if err != nil {
		log.Fatalf("connect ws://%s/ws/v1: %s", *addr, err)
	}
	log.Printf("submitting scores via %s", ws)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := submit.Run(ctx, ws, submit.Config{Rate: *rate, Players: *players},
			rand.New(rand.NewSource(time.Now().UnixNano())), log.Printf); err != nil {
			log.Printf("submitter: %s", err)
		}
	}()

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
	log.Printf("leaderboard page on http://127.0.0.1%s/ (Ultima at %s)", *httpAddr, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %s", err)
	}
}
