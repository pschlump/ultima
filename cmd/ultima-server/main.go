// Command ultima-server is the Ultima daemon: one process, three network
// surfaces (RESP :6379, gRPC :6380, HTTP/WS :6381 — design doc §3).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pschlump/ultima/lib/config"
)

func main() {
	cfgPath := flag.String("cfg", "ultima.cfg.json", "path to JSON config file")
	showVersion := flag.Bool("version", false, "print build stamp and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("ultima-server %s (commit %s, branch %s, built %s for %s)\n",
			Version, GitCommit, GitBranchName, BuildDate, BuildTarget)
		os.Exit(0)
	}

	var cfg config.Config
	if err := config.FromFile(*cfgPath, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "ultima-server: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevelValue()}))
	slog.SetDefault(logger)

	cfg.CheckStartupPosture(logger)

	srv, err := start(&cfg, logger)
	if err != nil {
		logger.Error("startup failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	logger.Info("shutdown requested")
	if err := srv.shutdown(context.Background()); err != nil {
		logger.Error("shutdown error", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}
