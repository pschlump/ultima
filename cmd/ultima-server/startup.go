package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/persist"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
)

// servers bundles the three listeners plus the shard engine and persist
// manager for coordinated shutdown. Listeners are bound up front (so
// ":0" test configs resolve to real ports) but serve loops start only
// after persistence restore completes (§13.1 restore-before-serve).
type servers struct {
	respSrv *resp.Server
	respLis net.Listener
	grpcLis net.Listener
	grpcSrv *grpc.Server
	httpSrv *http.Server
	httpLis net.Listener
	shards  *shard.Engine
	persist *persist.Manager
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
	if !eng.SetNotifyKeyspaceEvents(cfg.Server.NotifyKeyspaceEvents) {
		return nil, fmt.Errorf("invalid notify_keyspace_events %q: use characters from 'Ag$lshzxeKEtmdn'", cfg.Server.NotifyKeyspaceEvents)
	}
	pol, ok := shard.ParseEvictPolicy(cfg.Server.MaxMemoryPolicy)
	if !ok {
		return nil, fmt.Errorf("invalid maxmemory_policy %q: use one of '%s'", cfg.Server.MaxMemoryPolicy, strings.Join(shard.EvictPolicyNames, ", "))
	}
	s.shards.SetPolicy(pol)

	// Persistence (M5c, §13.1): the manager restores synchronously (AOF
	// when appendonly, else the snapshot) BEFORE any serve loop starts —
	// listeners are bound but not yet accepting, so no client can observe
	// an unrestored keyspace.
	switch strings.ToLower(cfg.Persist.AppendFsync) {
	case "always", "everysec", "no":
	default:
		return nil, fmt.Errorf("invalid appendfsync %q: use one of 'always', 'everysec', 'no'", cfg.Persist.AppendFsync)
	}
	s.persist = persist.NewManager(persist.Config{
		Dir:           cfg.Persist.Dir,
		DbFilename:    cfg.Persist.DbFilename,
		AppendDirname: cfg.Persist.AppendDirname,
		AppendOnly:    cfg.Persist.AppendOnly,
		AppendFsync:   strings.ToLower(cfg.Persist.AppendFsync),
		Save:          cfg.Persist.Save,
		Compress:      cfg.Persist.SnapshotCompress,
	}, s.shards, logger)
	if err := s.persist.Start(eng); err != nil {
		return nil, err
	}

	// Auth (M6a, §9): when auth.enabled, build the account/JWT/TOTP
	// service — a missing or unparseable Ed25519 key pair or accounts file
	// fails startup (D20, no auto-generation).
	var authSvc *auth.Service
	if cfg.Auth.Enabled {
		authSvc, err = auth.NewService(cfg.Auth, cfg.Persist.Dir, logger)
		if err != nil {
			return nil, err
		}
	}

	s.respSrv = respserver.New(cfg.Server.RespAddr, eng)
	go func() {
		if err := s.respSrv.Serve(s.respLis); err != nil {
			logger.Error("resp listener died", "err", err)
		}
	}()

	grpcServer := grpcsrv.New(eng, authSvc)
	s.grpcSrv = grpcServer
	go func() {
		if err := grpcServer.Serve(s.grpcLis); err != nil {
			logger.Error("grpc listener died", "err", err)
		}
	}()

	s.httpSrv = &http.Server{
		Handler:           newRouter(logger, eng, s.persist, authSvc),
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
	// Persist before the shard engine stops (§13.1): final fsync (+ the
	// shutdown snapshot when save rules are configured and unsaved writes
	// remain) needs live shard goroutines.
	s.persist.Close()
	s.shards.Close()
	return errors.Join(errs...)
}
