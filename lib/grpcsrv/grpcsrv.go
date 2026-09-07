// Package grpcsrv hosts Ultima's gRPC front-end (design doc §6.2). It
// serves the typed Command envelope (decision D15): a pipelined bidi
// stream (Exec), a unary batch (ExecBatch), and a unary generic
// convenience form (ExecGeneric). Every command — typed or generic — runs
// through the one shared command engine via lib/envelope; this package is
// only the transport. The Subscribe and Monitor push streams are declared
// in the IDL and arrive with the pub/sub bridge later in M4.
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
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/envelope"
)

// Server implements ultimav1.UltimaServer.
type Server struct {
	ultimav1.UnimplementedUltimaServer
	eng *commands.Engine
}

// New returns a *grpc.Server with the Ultima service registered against
// the shared command engine. Server reflection is enabled so grpcurl and
// similar tooling work unconfigured.
func New(eng *commands.Engine) *grpc.Server {
	s := grpc.NewServer()
	ultimav1.RegisterUltimaServer(s, &Server{eng: eng})
	reflection.Register(s)
	return s
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
	cs.Proto = 3 // binary clients get full RESP3-grade fidelity
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
	defer s.eng.CloseConn(cs)
	cmd := &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: req}}
	rs := envelope.Execute(s.eng, cs, cmd)
	return rs[0], nil
}

// Subscribe is declared in the M4 IDL; the pub/sub bridge that delivers
// broker pushes onto this stream lands with the WS half of M4.
func (s *Server) Subscribe(_ *ultimav1.SubscribeRequest, _ ultimav1.Ultima_SubscribeServer) error {
	return status.Error(codes.Unimplemented, "Subscribe arrives with the push bridge later in M4")
}

// Monitor is declared in the M4 IDL; MONITOR itself is not yet implemented
// in the command engine.
func (s *Server) Monitor(_ *ultimav1.MonitorRequest, _ ultimav1.Ultima_MonitorServer) error {
	return status.Error(codes.Unimplemented, "Monitor is not yet implemented")
}

// addrOf reports the peer address for ConnState bookkeeping (CLIENT LIST).
func addrOf(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "grpc"
}
