package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
)

// servers bundles the three listeners plus the shard engine for
// coordinated shutdown. Listeners are bound up front (so ":0" test
// configs resolve to real ports) before any serve loop starts.
type servers struct {
	respSrv *resp.Server
	respLis net.Listener
	grpcLis net.Listener
	grpcSrv *grpc.Server
	httpSrv *http.Server
	httpLis net.Listener
	shards  *shard.Engine
}

func respPortOf(lis net.Listener) int {
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		return 0
	}
	p, _ := strconv.Atoi(port)
	return p
}

func start(cfg *config.Config, logger *slog.Logger) (*servers, error) {
	s := &servers{}

	var err error
	if s.respLis, err = net.Listen("tcp", cfg.Server.RespAddr); err != nil {
		return nil, fmt.Errorf("resp listen %s: %w", cfg.Server.RespAddr, err)
	}
	if s.grpcLis, err = net.Listen("tcp", cfg.Server.GrpcAddr); err != nil {
		_ = s.respLis.Close()
		return nil, fmt.Errorf("grpc listen %s: %w", cfg.Server.GrpcAddr, err)
	}
	if s.httpLis, err = net.Listen("tcp", cfg.Server.HTTPAddr); err != nil {
		_ = s.respLis.Close()
		_ = s.grpcLis.Close()
		return nil, fmt.Errorf("http listen %s: %w", cfg.Server.HTTPAddr, err)
	}

	s.shards = shard.NewEngine(cfg.Server.ShardCount, cfg.Server.MaxDBs)
	eng := commands.NewEngine(s.shards, Version, respPortOf(s.respLis))
	eng.SetRequirePass(cfg.Server.RequirePass)
	eng.SetMaxMemory(int64(cfg.Server.MaxMemoryMB) << 20)

	s.respSrv = respserver.New(cfg.Server.RespAddr, eng)
	go func() {
		if err := s.respSrv.Serve(s.respLis); err != nil {
			logger.Error("resp listener died", "err", err)
		}
	}()

	grpcServer := grpcsrv.New(eng)
	s.grpcSrv = grpcServer
	go func() {
		if err := grpcServer.Serve(s.grpcLis); err != nil {
			logger.Error("grpc listener died", "err", err)
		}
	}()

	s.httpSrv = &http.Server{
		Handler:           newRouter(logger, eng),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.httpSrv.Serve(s.httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http listener died", "err", err)
		}
	}()

	logger.Info("ultima-server listening",
		"resp_addr", s.respLis.Addr().String(),
		"grpc_addr", s.grpcLis.Addr().String(),
		"http_addr", s.httpLis.Addr().String(),
		"shards", s.shards.ShardCount(),
		"dbs", s.shards.MaxDBs(),
	)
	return s, nil
}

func (s *servers) shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var errs []error

	// gRPC: GracefulStop blocks until RPCs drain, so cap it with the ctx.
	stopped := make(chan struct{})
	go func() {
		s.grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		errs = append(errs, ctx.Err())
	}

	errs = append(errs, s.httpSrv.Shutdown(ctx))
	errs = append(errs, s.respSrv.Close()) // closes the listener and all conns
	s.shards.Close()
	return errors.Join(errs...)
}
