// Package grpcsrv hosts Ultima's gRPC front-end (design doc §6.2). M0
// implements only the Ultima.Ping health check on the generated
// ultima.v1.Ultima service.
package grpcsrv

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
)

// Server implements ultimav1.UltimaServer.
type Server struct {
	ultimav1.UnimplementedUltimaServer
}

// New returns a *grpc.Server with the Ultima service registered. Server
// reflection is enabled so grpcurl and similar tooling work unconfigured.
func New() *grpc.Server {
	s := grpc.NewServer()
	ultimav1.RegisterUltimaServer(s, &Server{})
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
