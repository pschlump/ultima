// Package grpcsrv hosts Ultima's gRPC front-end (design doc §6.2). It
// serves the typed Command envelope (decision D15): a pipelined bidi
// stream (Exec), a unary batch (ExecBatch), and a unary generic
// convenience form (ExecGeneric), plus the two push streams — Subscribe
// (classic pub/sub) and Monitor (the MONITOR equivalent, fed by
// Engine.AddMonitor). Every command — typed or generic — runs through the
// one shared command engine via lib/envelope; this package is only the
// transport.
package grpcsrv

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/resp"
)

// Server implements ultimav1.UltimaServer.
type Server struct {
	ultimav1.UnimplementedUltimaServer
	eng *commands.Engine
}

// New returns a *grpc.Server with the Ultima service registered against
// the shared command engine. Server reflection is enabled so grpcurl and
// similar tooling work unconfigured. When authSvc is non-nil (M6a,
// auth.enabled), unary and stream interceptors demand a valid
// `authorization: bearer …` access token (§9.3) and the verified identity
// lands on each ConnState (Authed/User).
func New(eng *commands.Engine, authSvc *auth.Service) *grpc.Server {
	var opts []grpc.ServerOption
	if authSvc != nil {
		opts = append(opts,
			grpc.UnaryInterceptor(authSvc.UnaryInterceptor()),
			grpc.StreamInterceptor(authSvc.StreamInterceptor()),
		)
	}
	s := grpc.NewServer(opts...)
	ultimav1.RegisterUltimaServer(s, &Server{eng: eng})
	reflection.Register(s)
	return s
}

// applyIdentity copies the interceptor-verified identity (M6a) onto the
// connection state; a no-op when auth is disabled.
func applyIdentity(ctx context.Context, cs *commands.ConnState) {
	if id, ok := auth.IdentityFrom(ctx); ok {
		cs.Authed = true
		cs.User = id.Username
	}
}

// Ping echoes the request message, answering "PONG" when none was sent —
// the gRPC twin of RESP PING.
func (s *Server) Ping(_ context.Context, req *ultimav1.PingRequest) (*ultimav1.PingResponse, error) {
	msg := req.GetMessage()
	if msg == "" {
		msg = "PONG"
	}
	return &ultimav1.PingResponse{Message: msg}, nil
}

// Exec is the pipelined, ordered command stream (§6.2). Commands execute
// in arrival order on one per-stream ConnState (so SELECT, MULTI/EXEC and
// AUTH via the generic envelope behave exactly as they do on a RESP
// connection) and every reply carries the request's correlation seq. The
// stream ends when the client half-closes it or sends QUIT.
func (s *Server) Exec(stream ultimav1.Ultima_ExecServer) error {
	cs := s.eng.NewConnState(addrOf(stream.Context()))
	cs.Proto = 3          // binary clients get full RESP3-grade fidelity
	cs.SetSurface("grpc") // M6c client introspection (§10.1)
	applyIdentity(stream.Context(), cs)
	defer s.eng.CloseConn(cs)
	for {
		cmd, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, r := range envelope.Execute(s.eng, cs, cmd) {
			if err := stream.Send(r); err != nil {
				return err
			}
		}
		if cs.Quit {
			return nil
		}
	}
}

// ExecBatch runs a batch of commands on a fresh per-call ConnState and
// returns one response per command, in order. Responses carry their
// command's seq, as on the stream.
func (s *Server) ExecBatch(ctx context.Context, req *ultimav1.BatchRequest) (*ultimav1.BatchResponse, error) {
	cs := s.eng.NewConnState(addrOf(ctx))
	cs.Proto = 3
	cs.SetSurface("grpc") // M6c client introspection (§10.1); no kill hook: a server stream cannot be force-closed
	applyIdentity(ctx, cs)
	defer s.eng.CloseConn(cs)
	out := &ultimav1.BatchResponse{
		Responses: make([]*ultimav1.CommandResponse, 0, len(req.Commands)),
	}
	for _, cmd := range req.Commands {
		out.Responses = append(out.Responses, envelope.Execute(s.eng, cs, cmd)...)
	}
	return out, nil
}

// ExecGeneric is the tooling/CLI convenience form: one generic command per
// call on a fresh ConnState.
func (s *Server) ExecGeneric(ctx context.Context, req *ultimav1.CommandRequest) (*ultimav1.CommandResponse, error) {
	cs := s.eng.NewConnState(addrOf(ctx))
	cs.Proto = 3
	cs.SetSurface("grpc") // M6c client introspection (§10.1); no kill hook: a server stream cannot be force-closed
	applyIdentity(ctx, cs)
	defer s.eng.CloseConn(cs)
	cmd := &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: req}}
	rs := envelope.Execute(s.eng, cs, cmd)
	return rs[0], nil
}

// pushQueueCap bounds a Subscribe stream's outbound queue; beyond it the
// client is a slow consumer and the stream is torn down — the same rule
// lib/respserver and lib/wssrv apply.
const pushQueueCap = 4096

// Subscribe is the pub/sub push stream (§6.2): the request lists channels
// and patterns, the replies are the subscribe acks followed by one
// PushEvent per published message until the client goes away. Deliveries
// arrive on publisher goroutines, so they flow through a bounded queue
// drained by a single sender goroutine (stream.Send is not safe for
// concurrent use). Closing the stream drops all of its subscriptions via
// CloseConn.
func (s *Server) Subscribe(req *ultimav1.SubscribeRequest, stream ultimav1.Ultima_SubscribeServer) error {
	if len(req.Channels) == 0 && len(req.Patterns) == 0 {
		return status.Error(codes.InvalidArgument, "Subscribe needs at least one channel or pattern")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	cs := s.eng.NewConnState(addrOf(ctx))
	cs.Proto = 3
	cs.SetSurface("grpc") // M6c client introspection (§10.1); no kill hook: a server stream cannot be force-closed
	applyIdentity(ctx, cs)
	defer s.eng.CloseConn(cs) // drops this stream's subscriptions

	events := make(chan *ultimav1.PushEvent, pushQueueCap)
	cs.StartPush = func() func(resp.Value) {
		return func(v resp.Value) {
			select {
			case events <- pushEventOf(v):
			case <-ctx.Done():
			default:
				// Slow consumer: cancel the stream, which unsubscribes via
				// the deferred CloseConn.
				cancel()
			}
		}
	}

	sendErr := make(chan error, 1)
	go func() {
		for {
			select {
			case ev := <-events:
				if err := stream.Send(ev); err != nil {
					sendErr <- err
					return
				}
			case <-ctx.Done():
				sendErr <- nil
				return
			}
		}
	}()

	// Run the subscriptions through the engine so the ack frames (and the
	// subscribe bookkeeping) are identical to RESP's: Execute returns the
	// first ack and queues the rest on the connection outbox.
	subscribe := func(name string, targets []string) error {
		if len(targets) == 0 {
			return nil
		}
		args := [][]byte{[]byte(name)}
		for _, tgt := range targets {
			args = append(args, []byte(tgt))
		}
		first := s.eng.Execute(cs, args)
		if first.Kind == resp.KindError {
			return status.Errorf(codes.InvalidArgument, "%s failed: %s", name, first.Str)
		}
		for _, f := range append([]resp.Value{first}, cs.DrainOutbox()...) {
			events <- pushEventOf(f)
		}
		return nil
	}
	if err := subscribe("SUBSCRIBE", req.Channels); err != nil {
		return err
	}
	if err := subscribe("PSUBSCRIBE", req.Patterns); err != nil {
		return err
	}

	return <-sendErr
}

// pushEventOf converts an engine push frame into its PushEvent form. Push
// frames are RESP3 arrays: [message channel payload],
// [pmessage pattern channel payload], or [{p}{,un}subscribe name count].
func pushEventOf(v resp.Value) *ultimav1.PushEvent {
	ev := &ultimav1.PushEvent{}
	if len(v.Arr) == 0 {
		return ev
	}
	kind := string(v.Arr[0].Blob)
	switch kind {
	case "message":
		if len(v.Arr) >= 3 {
			ev.Channel = string(v.Arr[1].Blob)
			ev.Payload = v.Arr[2].Blob
		}
	case "pmessage":
		if len(v.Arr) >= 4 {
			ev.Pattern = string(v.Arr[1].Blob)
			ev.Channel = string(v.Arr[2].Blob)
			ev.Payload = v.Arr[3].Blob
		}
	default: // subscribe/psubscribe/unsubscribe/punsubscribe acks
		ev.IsAck = true
		if len(v.Arr) >= 3 {
			if kind == "psubscribe" || kind == "punsubscribe" {
				ev.Pattern = string(v.Arr[1].Blob)
			} else {
				ev.Channel = string(v.Arr[1].Blob)
			}
			ev.Count = v.Arr[2].Int
		}
	}
	return ev
}

// Monitor is the MONITOR-equivalent stream (§6.2): one CommandEvent per
// command executed anywhere on the server, until the client goes away.
// Engine sinks run on the executing connection's goroutine, so events flow
// through a bounded queue; a slow consumer's stream is cancelled rather
// than stalling the engine.
func (s *Server) Monitor(_ *ultimav1.MonitorRequest, stream ultimav1.Ultima_MonitorServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	events := make(chan *ultimav1.CommandEvent, pushQueueCap)
	detach := s.eng.AddMonitor(func(ev commands.MonitorEvent) {
		ce := &ultimav1.CommandEvent{
			UnixMs:     ev.When.UnixMilli(),
			Db:         uint64(ev.DB), //nolint:gosec // DB indexes are small and non-negative
			ClientAddr: ev.Addr,
		}
		if len(ev.Args) > 0 {
			ce.Command = string(ev.Args[0])
			ce.Args = ev.Args[1:]
		}
		select {
		case events <- ce:
		case <-ctx.Done():
		default:
			cancel() // slow consumer
		}
	})
	defer detach()

	for {
		select {
		case ce := <-events:
			if err := stream.Send(ce); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// addrOf reports the peer address for ConnState bookkeeping (CLIENT LIST).
func addrOf(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "grpc"
}
