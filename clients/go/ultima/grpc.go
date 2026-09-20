// Package ultima gRPC client for the binary surface (design doc §6.2): one bidi Exec
// stream with client-side seq correlation (the server echoes the Command's
// seq on the CommandResponse, so pipelined calls match replies through a
// pending map), plus the unary ExecBatch/ExecGeneric/Ping RPCs and the
// Subscribe/Monitor server streams. JWT auth rides the standard
// `authorization: bearer` call metadata (§9.3, lib/auth interceptors).
package ultima

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/resp"
)

// GRPCClient is a goroutine-safe client for the gRPC surface (§6.2).
// Concurrent calls share the single bidi Exec stream; each is matched to
// its reply by seq.
type GRPCClient struct {
	binarySurface

	conn   *grpc.ClientConn
	cli    ultimav1.UltimaClient
	cancel context.CancelFunc

	mu      sync.Mutex // guards stream sends, pending and seq
	stream  ultimav1.Ultima_ExecClient
	seq     uint64
	pending map[uint64]chan grpcResult
	err     error // terminal stream failure; set once
}

type grpcResult struct {
	val resp.Value
	err error
}

// GRPCOption configures DialGRPC.
type GRPCOption func(*grpcOptions)

type grpcOptions struct {
	tokenFunc func(ctx context.Context) (string, error)
}

// WithGRPCToken authenticates every RPC with a static access token.
func WithGRPCToken(token string) GRPCOption {
	return WithGRPCTokenFunc(func(context.Context) (string, error) { return token, nil })
}

// WithGRPCTokenFunc authenticates every RPC with the token the provider
// returns at call time (a TokenManager's Token method, so refreshed
// tokens are picked up automatically).
func WithGRPCTokenFunc(f func(ctx context.Context) (string, error)) GRPCOption {
	return func(o *grpcOptions) { o.tokenFunc = f }
}

// bearerCreds is grpc.PerRPCCredentials carrying a bearer token from the
// provider; RequireTransportSecurity is false because the v1 client is
// plaintext-only (TLS is out of scope per the M7 plan).
type bearerCreds struct {
	f func(ctx context.Context) (string, error)
}

func (c bearerCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := c.f(ctx)
	if err != nil || tok == "" {
		return nil, err
	}
	return map[string]string{"authorization": "bearer " + tok}, nil
}

func (bearerCreds) RequireTransportSecurity() bool { return false }

// DialGRPC connects to the gRPC surface at addr ("host:port") and opens
// the bidi Exec stream eagerly, so a dial failure surfaces here.
func DialGRPC(addr string, opts ...GRPCOption) (*GRPCClient, error) {
	o := grpcOptions{}
	for _, f := range opts {
		f(&o)
	}
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if o.tokenFunc != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerCreds{f: o.tokenFunc}))
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &GRPCClient{
		conn:    conn,
		cli:     ultimav1.NewUltimaClient(conn),
		cancel:  cancel,
		pending: map[uint64]chan grpcResult{},
	}
	c.execFn = c.execCommand
	c.stream, err = c.cli.Exec(ctx)
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, err
	}
	go c.recvLoop()
	return c, nil
}

// Close tears down the stream and the connection; pending calls fail.
func (c *GRPCClient) Close() error {
	c.cancel()
	return c.conn.Close()
}

// recvLoop dispatches replies to the pending map; a stream failure is
// terminal (no reconnect in v1 — the CLI exits, library callers re-dial).
func (c *GRPCClient) recvLoop() {
	for {
		r, err := c.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("ultima: gRPC Exec stream closed by server")
			}
			c.failAll(err)
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[r.GetSeq()]
		delete(c.pending, r.GetSeq())
		c.mu.Unlock()
		if ok {
			ch <- grpcResult{val: envelope.FromProto(r.GetReply())}
		}
	}
}

func (c *GRPCClient) failAll(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
	}
	for seq, ch := range c.pending {
		ch <- grpcResult{err: err}
		delete(c.pending, seq)
	}
}

// execCommand runs one typed Command over the bidi stream.
func (c *GRPCClient) execCommand(_ context.Context, cmd *ultimav1.Command) (resp.Value, error) {
	ch := make(chan grpcResult, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return resp.Value{}, c.err
	}
	c.seq++
	cmd.Seq = c.seq
	c.pending[cmd.Seq] = ch
	err := c.stream.Send(cmd)
	c.mu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, cmd.GetSeq())
		c.mu.Unlock()
		return resp.Value{}, err
	}
	r := <-ch
	return r.val, r.err
}

// ExecGeneric is the unary one-command convenience RPC (the CLI path).
func (c *GRPCClient) ExecGeneric(ctx context.Context, cmd string, args ...string) (resp.Value, error) {
	r, err := c.cli.ExecGeneric(ctx, &ultimav1.CommandRequest{Command: cmd, Args: bb(args)})
	if err != nil {
		return resp.Value{}, err
	}
	return envelope.FromProto(r.GetReply()), nil
}

// ExecBatch runs generic commands as one unary batch (same envelope, no
// stream) and returns one reply per command, in order.
func (c *GRPCClient) ExecBatch(ctx context.Context, commands ...[]string) ([]resp.Value, error) {
	cmds := make([]*ultimav1.Command, 0, len(commands))
	for _, a := range commands {
		if len(a) == 0 {
			return nil, errors.New("ultima: empty command in batch")
		}
		cmds = append(cmds, cmdGeneric(a[0], a[1:]))
	}
	return c.ExecBatchTyped(ctx, cmds...)
}

// ExecBatchTyped is ExecBatch over fully typed Commands.
func (c *GRPCClient) ExecBatchTyped(ctx context.Context, cmds ...*ultimav1.Command) ([]resp.Value, error) {
	r, err := c.cli.ExecBatch(ctx, &ultimav1.BatchRequest{Commands: cmds})
	if err != nil {
		return nil, err
	}
	out := make([]resp.Value, 0, len(r.GetResponses()))
	for _, rr := range r.GetResponses() {
		out = append(out, envelope.FromProto(rr.GetReply()))
	}
	return out, nil
}

// Subscribe opens the pub/sub push stream (§6.2 SubscribeRequest:
// channels and patterns) and invokes handler for every PushEvent —
// subscribe acks included (IsAck, Count) — until ctx is cancelled or the
// stream fails. There is no mid-stream re-subscribe; open a second
// Subscribe call for more channels.
func (c *GRPCClient) Subscribe(ctx context.Context, channels, patterns []string, handler func(*ultimav1.PushEvent)) error {
	stream, err := c.cli.Subscribe(ctx, &ultimav1.SubscribeRequest{Channels: channels, Patterns: patterns})
	if err != nil {
		return err
	}
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		handler(ev)
	}
}

// Monitor opens the MONITOR-equivalent stream (§6.2) and invokes handler
// for every observed command until ctx is cancelled or the stream fails.
func (c *GRPCClient) Monitor(ctx context.Context, handler func(*ultimav1.CommandEvent)) error {
	stream, err := c.cli.Monitor(ctx, &ultimav1.MonitorRequest{})
	if err != nil {
		return err
	}
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		handler(ev)
	}
}

// Ping is the unary Ping RPC: an empty message answers PONG.
func (c *GRPCClient) Ping(ctx context.Context, message string) (string, error) {
	r, err := c.cli.Ping(ctx, &ultimav1.PingRequest{Message: message})
	if err != nil {
		return "", err
	}
	return r.GetMessage(), nil
}
