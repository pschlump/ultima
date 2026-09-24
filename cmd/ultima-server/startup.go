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
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/persist"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/scripting"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
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
	httpLis []net.Listener
	shards  *shard.Engine
	persist *persist.Manager
	wssess  *wssession.Registry
	scripts *scripting.Manager
}

func respPortOf(lis net.Listener) int {
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		return 0
	}
	p, _ := strconv.Atoi(port)
	return p
}

// parseIPNets parses a comma-separated CIDR/IP list (the
// server.metrics_allow config value); bare IPs become /32 or /128.
func parseIPNets(list string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "/") {
			_, n, err := net.ParseCIDR(part)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", part, err)
			}
			out = append(out, n)
			continue
		}
		ip := net.ParseIP(part)
		if ip == nil {
			return nil, fmt.Errorf("%q: not a CIDR or IP", part)
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return out, nil
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
	// The HTTP/WS surface binds http_addr plus every http_addrs entry.
	httpAddrs, err := cfg.Server.HTTPListenAddrs()
	if err != nil {
		_ = s.respLis.Close()
		_ = s.grpcLis.Close()
		return nil, err
	}
	for _, addr := range httpAddrs {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			// An address that isn't on any local interface (a laptop that
			// moved networks) is skipped with a warning; real conflicts
			// (port in use, bad address) stay fatal.
			if errors.Is(err, syscall.EADDRNOTAVAIL) {
				logger.Warn("http listen address not on this host, skipping", "addr", addr)
				continue
			}
			_ = s.respLis.Close()
			_ = s.grpcLis.Close()
			for _, l := range s.httpLis {
				_ = l.Close()
			}
			return nil, fmt.Errorf("http listen %s: %w", addr, err)
		}
		s.httpLis = append(s.httpLis, lis)
	}
	if len(s.httpLis) == 0 {
		_ = s.respLis.Close()
		_ = s.grpcLis.Close()
		return nil, fmt.Errorf("http: no listen address available (all of %q unavailable)", strings.Join(httpAddrs, ", "))
	}

	s.shards = shard.NewEngine(cfg.Server.ShardCount, cfg.Server.MaxDBs)
	eng := commands.NewEngine(s.shards, Version, respPortOf(s.respLis))
	eng.SetLogger(logger)
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

	// Lua scripting (M8, §7 P3, D12): the scripting manager wraps the
	// SHA-pinned gopher-lua production blob; the script.* config group
	// feeds the soft BUSY limit, the hard watchdog deadline (S5), the
	// per-VM memory budget, and the deterministic RNG seed base (S6).
	s.scripts, err = scripting.New(scripting.Config{
		LuaTimeLimitMs: cfg.Script.LuaTimeLimitMs,
		HardDeadlineMs: cfg.Script.HardDeadlineMs,
		MaxMemoryMB:    cfg.Script.MaxMemoryMB,
		RngSeed:        cfg.Script.RngSeed,
		RunID:          eng.RunID,
		CompatVersion:  commands.CompatVersion,
		Logger:         logger,
		VMPoolSize:     cfg.Script.VMPoolSize,
		VMPoolMax:      cfg.Script.VMPoolMax,
		VMRecycleRuns:  cfg.Script.VMRecycleRuns,
		VMRecyclePct:   cfg.Script.VMRecyclePct,
	})
	if err != nil {
		return nil, fmt.Errorf("scripting: %w", err)
	}
	eng.Scripts = s.scripts
	eng.SetScriptMaxMemoryMB(cfg.Script.MaxMemoryMB)

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

	// Resumable WS sessions (M6b, §9.4, D18): the registry bounds replay
	// retention by auth.ws_replay_buffer_ms / ws_replay_buffer_max_msgs.
	s.wssess = wssession.NewRegistry(eng,
		time.Duration(cfg.Auth.WSReplayBufferMs)*time.Millisecond,
		cfg.Auth.WSReplayBufferMaxMsgs, logger)

	// /metrics IP allowlist (M6c, §10.1): comma-separated CIDRs, bare IPs
	// become /32 or /128.
	metricsAllow, err := parseIPNets(cfg.Server.MetricsAllow)
	if err != nil {
		return nil, fmt.Errorf("invalid metrics_allow: %w", err)
	}

	// /ws/v1 Origin allowlist (M6d, §10.2): comma-separated origins or
	// hosts; only consulted when auth is enabled.
	var originAllow []string
	for _, o := range strings.Split(cfg.Server.WSOriginAllow, ",") {
		if o = strings.TrimSpace(o); o != "" {
			originAllow = append(originAllow, o)
		}
	}

	s.httpSrv = &http.Server{
		Handler:           newRouter(logger, eng, s.persist, authSvc, s.wssess, metricsAllow, originAllow, cfg.Server.PprofEnabled),
		ReadHeaderTimeout: 10 * time.Second,
	}
	// One http.Server over every bound listener; Shutdown(ctx) closes them
	// all (Serve registers each listener with the server).
	for _, lis := range s.httpLis {
		go func() {
			if err := s.httpSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("http listener died", "addr", lis.Addr().String(), "err", err)
			}
		}()
	}

	httpBound := make([]string, 0, len(s.httpLis))
	for _, lis := range s.httpLis {
		httpBound = append(httpBound, lis.Addr().String())
	}
	logger.Info("ultima-server listening",
		"resp_addr", s.respLis.Addr().String(),
		"grpc_addr", s.grpcLis.Addr().String(),
		"http_addr", strings.Join(httpBound, ","),
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
	s.wssess.Close()                       // expire retained WS sessions (§9.4)
	_ = s.scripts.Close()                  // no new script VMs (M8)
	// Persist before the shard engine stops (§13.1): final fsync (+ the
	// shutdown snapshot when save rules are configured and unsaved writes
	// remain) needs live shard goroutines.
	s.persist.Close()
	s.shards.Close()
	return errors.Join(errs...)
}
