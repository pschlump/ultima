// M4 parity gate (exit criterion "parity with RESP replies"): one command
// script is executed against the same engine twice — once over a real RESP
// socket (RESP3 negotiated), once over the gRPC generic envelope — and the
// replies are diffed as RESP3 wire bytes: each gRPC CommandResponse goes
// back through envelope.FromProto and resp.AppendValue, the very renderer
// the RESP front-end uses, so byte equality is exact parity, error strings
// included. The script sticks to deterministic commands (no TTL values, no
// multi-member set/hash iteration order, no INFO).
package tests

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
)

// parityScript is run once per surface with a per-surface key prefix so the
// two runs don't interfere through shared state.
var parityScript = [][]string{
	{"set", "{p}k", "1"},
	{"get", "{p}k"},
	{"incr", "{p}k"},
	{"decr", "{p}k"},
	{"append", "{p}k", "ab"},
	{"strlen", "{p}k"},
	{"set", "{p}f", "0.5"},
	{"incrbyfloat", "{p}f", "0.25"},
	{"mset", "{p}a", "1", "{p}b", "2"},
	{"mget", "{p}a", "{p}b", "{p}c"},
	{"del", "{p}a", "{p}b"},
	{"exists", "{p}k", "{p}nope"},
	{"type", "{p}k"},
	{"pttl", "{p}k"},
	{"pexpire", "{p}k", "60000"},
	{"persist", "{p}k"},
	{"get", "{p}missing"},
	{"hset", "{p}h", "f1", "v1", "f2", "v2"},
	{"hget", "{p}h", "f1"},
	{"hgetall", "{p}h"},
	{"hdel", "{p}h", "f2"},
	{"rpush", "{p}l", "a", "b", "c"},
	{"lrange", "{p}l", "0", "-1"},
	{"lpop", "{p}l"},
	{"sadd", "{p}s", "only"},
	{"smembers", "{p}s"},
	{"sismember", "{p}s", "only"},
	{"zadd", "{p}z", "1.5", "m1"},
	{"zadd", "{p}z", "2.5", "m2"},
	{"zscore", "{p}z", "m1"},
	{"zrange", "{p}z", "0", "-1", "withscores"},
	{"zcard", "{p}z"},
	// error paths: wrong type, unknown command, bad arity
	{"incr", "{p}l"},
	{"nosuchcmd", "plain"}, // no {p}: the error string embeds the args
	{"get"},
	{"get", "{p}h"}, // hash fetched as string
}

func TestGRPCParityWithRESP(t *testing.T) {
	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)

	// RESP surface on a real socket.
	respLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = respLis.Close() })
	respSrv := respserver.New("127.0.0.1:0", eng)
	go func() { _ = respSrv.Serve(respLis) }()
	t.Cleanup(func() { _ = respSrv.Close() })

	// gRPC surface on the same engine.
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = grpcLis.Close() })
	grpcSrv := grpcsrv.New(eng, nil)
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	t.Cleanup(grpcSrv.GracefulStop)
	gconn, err := grpc.NewClient(grpcLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gconn.Close() })
	cli := ultimav1.NewUltimaClient(gconn)

	respWire := parityOverRESP(t, respLis.Addr().String(), "r:")
	grpcWire := parityOverGRPC(t, cli, "g:")

	if len(respWire) != len(parityScript) || len(grpcWire) != len(parityScript) {
		t.Fatalf("reply counts: RESP %d, gRPC %d, script %d",
			len(respWire), len(grpcWire), len(parityScript))
	}
	for i, cmd := range parityScript {
		if !bytes.Equal(respWire[i], grpcWire[i]) {
			t.Errorf("step %d %v: RESP wire %q != gRPC wire %q",
				i, cmd, respWire[i], grpcWire[i])
		}
	}
}

// parityOverRESP runs the script on a raw RESP3 socket and returns each
// reply's exact wire bytes.
func parityOverRESP(t *testing.T, addr, prefix string) [][]byte {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rdr := bufio.NewReader(conn)

	send := func(args ...string) {
		t.Helper()
		var b bytes.Buffer
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
		}
		if _, err := conn.Write(b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}

	send("HELLO", "3")
	if _, err := readRESPFrame(rdr); err != nil {
		t.Fatal(err)
	}
	out := make([][]byte, 0, len(parityScript))
	for _, cmd := range parityScript {
		args := make([]string, len(cmd))
		for i, a := range cmd {
			if a == "{p}" || (len(a) > 3 && a[:3] == "{p}") {
				a = prefix + a[3:]
			}
			args[i] = a
		}
		send(args...)
		frame, err := readRESPFrame(rdr)
		if err != nil {
			t.Fatalf("reading reply for %v: %v", cmd, err)
		}
		out = append(out, frame)
	}
	return out
}

// parityOverGRPC runs the script through the generic envelope on one Exec
// stream and renders each reply back to RESP3 wire bytes via FromProto +
// AppendValue — the same renderer the RESP front-end uses.
func parityOverGRPC(t *testing.T, cli ultimav1.UltimaClient, prefix string) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := cli.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := make([][]byte, 0, len(parityScript))
	for i, cmd := range parityScript {
		req := &ultimav1.CommandRequest{Command: cmd[0]}
		for _, a := range cmd[1:] {
			if len(a) > 3 && a[:3] == "{p}" {
				a = prefix + a[3:]
			}
			req.Args = append(req.Args, []byte(a))
		}
		if err := stream.Send(&ultimav1.Command{
			Seq: uint64(i + 1), //nolint:gosec // small test values
			Cmd: &ultimav1.Command_Generic{Generic: req},
		}); err != nil {
			t.Fatal(err)
		}
		r, err := stream.Recv()
		if err != nil {
			t.Fatalf("step %d %v: %v", i, cmd, err)
		}
		out = append(out, resp.AppendValue(nil, 3, envelope.FromProto(r.Reply)))
	}
	return out
}

// readRESPFrame consumes exactly one RESP2/RESP3 reply from r and returns
// its raw wire bytes, recursively framing aggregate types.
func readRESPFrame(r *bufio.Reader) ([]byte, error) {
	var raw bytes.Buffer
	line, err := readLineRaw(r, &raw)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	switch line[0] {
	case '$', '=', '!': // bulk / verbatim / blob error: payload follows
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n >= 0 {
			if _, err := io.CopyN(&raw, r, int64(n)+2); err != nil {
				return nil, err
			}
		}
	case '*', '%', '~', '>': // aggregates: element count follows
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if line[0] == '%' {
			n *= 2
		}
		for i := 0; i < n; i++ {
			sub, err := readRESPFrame(r)
			if err != nil {
				return nil, err
			}
			raw.Write(sub)
		}
	}
	return raw.Bytes(), nil
}

func readLineRaw(r *bufio.Reader, raw *bytes.Buffer) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	raw.WriteString(line)
	return string(bytes.TrimRight([]byte(line), "\r\n")), nil
}
