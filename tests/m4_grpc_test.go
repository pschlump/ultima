// M4 gRPC front-end tests (design doc §6.2, decision D15): the typed
// Command envelope over the Exec bidi stream, the ExecBatch unary batch,
// and the ExecGeneric convenience form, all against the one shared command
// engine — replies must match what the RESP surface would return.
package tests

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/shard"
)

// grpcTestClient boots the gRPC front-end exactly as cmd/ultima-server
// does (shard engine → command engine → grpcsrv) on an ephemeral port.
func grpcTestClient(t *testing.T) ultimav1.UltimaClient {
	t.Helper()
	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpcsrv.New(eng, nil)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	t.Cleanup(func() { _ = lis.Close() })

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ultimav1.NewUltimaClient(conn)
}

// execRoundTrip sends the commands on one Exec stream and returns the
// replies (one per command, in order).
func execRoundTrip(t *testing.T, c ultimav1.UltimaClient, cmds ...*ultimav1.Command) []*ultimav1.Value {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range cmds {
		if err := stream.Send(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var out []*ultimav1.Value
	for {
		r, err := stream.Recv()
		if err != nil { // io.EOF: server closed after CloseSend
			break
		}
		out = append(out, r.Reply)
	}
	if len(out) != len(cmds) {
		t.Fatalf("Exec returned %d replies for %d commands", len(out), len(cmds))
	}
	for i, r := range out {
		if r == nil {
			t.Fatalf("reply %d has no value", i)
		}
	}
	return out
}

func TestGRPCExecTypedRoundTrip(t *testing.T) {
	c := grpcTestClient(t)

	replies := execRoundTrip(t, c,
		&ultimav1.Command{Seq: 1, Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
			Key: []byte("g:k"), Value: []byte("42")}}},
		&ultimav1.Command{Seq: 2, Cmd: &ultimav1.Command_Incr{Incr: &ultimav1.IncrCommand{
			Key: []byte("g:k"), Delta: 8}}},
		&ultimav1.Command{Seq: 3, Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{
			Key: []byte("g:k")}}},
		&ultimav1.Command{Seq: 4, Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{
			Key: []byte("g:absent")}}},
	)
	if got := replies[0].GetSimpleString(); got != "OK" {
		t.Errorf("SET reply = %q, want OK", got)
	}
	if got := replies[1].GetInt(); got != 50 {
		t.Errorf("INCRBY reply = %d, want 50", got)
	}
	if got := string(replies[2].GetBlobString()); got != "50" {
		t.Errorf("GET reply = %q, want 50", got)
	}
	if !replies[3].GetNull() {
		t.Errorf("GET missing key reply = %v, want null", replies[3])
	}
}

// TestGRPCExecSeqAndDB covers seq echoing (pipelined correlation) and the
// per-command db override: a write in db 5 must be invisible from db 0.
func TestGRPCExecSeqAndDB(t *testing.T) {
	c := grpcTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	send := []*ultimav1.Command{
		{Seq: 77, Db: 5, Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
			Key: []byte("g:dbk"), Value: []byte("five")}}},
		{Seq: 78, Db: 5, Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{
			Key: []byte("g:dbk")}}},
		{Seq: 79, Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{
			Key: []byte("g:dbk")}}},
	}
	for _, cmd := range send {
		if err := stream.Send(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	seqs := map[uint64]*ultimav1.Value{}
	for {
		r, err := stream.Recv()
		if err != nil {
			break
		}
		seqs[r.Seq] = r.Reply
	}
	if len(seqs) != 3 {
		t.Fatalf("got %d replies, want 3", len(seqs))
	}
	if got := string(seqs[78].GetBlobString()); got != "five" {
		t.Errorf("db-5 GET = %q, want five", got)
	}
	if !seqs[79].GetNull() {
		t.Errorf("db-0 GET of db-5 key = %v, want null", seqs[79])
	}
}

// TestGRPCExecCollections exercises typed hash/list/set/zset commands and
// the RESP3-grade reply types (map, array, exact double).
func TestGRPCExecCollections(t *testing.T) {
	c := grpcTestClient(t)

	replies := execRoundTrip(t, c,
		&ultimav1.Command{Cmd: &ultimav1.Command_Hset{Hset: &ultimav1.HSetCommand{
			Key: []byte("g:h"), Pairs: []*ultimav1.FieldValue{
				{Field: []byte("a"), Value: []byte("1")},
				{Field: []byte("b"), Value: []byte("2")},
			}}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Hgetall{Hgetall: &ultimav1.HGetAllCommand{
			Key: []byte("g:h")}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Rpush{Rpush: &ultimav1.RPushCommand{
			Key: []byte("g:l"), Elems: [][]byte{[]byte("x"), []byte("y")}}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Lrange{Lrange: &ultimav1.LRangeCommand{
			Key: []byte("g:l"), Start: 0, Stop: -1}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Zadd{Zadd: &ultimav1.ZAddCommand{
			Key: []byte("g:z"), Members: []*ultimav1.ScoredMember{
				{Score: 3.141592653589793, Member: []byte("pi")},
			}}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Zscore{Zscore: &ultimav1.ZScoreCommand{
			Key: []byte("g:z"), Member: []byte("pi")}}},
	)

	if got := replies[0].GetInt(); got != 2 {
		t.Errorf("HSET reply = %d, want 2", got)
	}
	m := replies[1].GetMap()
	if m == nil || len(m.Pairs) != 2 {
		t.Fatalf("HGETALL reply = %v, want 2-pair map", replies[1])
	}
	if got := string(m.Pairs[0].Key.GetBlobString()); got != "a" {
		t.Errorf("HGETALL pair 0 key = %q, want a", got)
	}
	if got := replies[2].GetInt(); got != 2 {
		t.Errorf("RPUSH reply = %d, want 2", got)
	}
	arr := replies[3].GetArray()
	if arr == nil || len(arr.Elems) != 2 || string(arr.Elems[1].GetBlobString()) != "y" {
		t.Errorf("LRANGE reply = %v, want [x y]", replies[3])
	}
	if got := replies[4].GetInt(); got != 1 {
		t.Errorf("ZADD reply = %d, want 1", got)
	}
	// Double fidelity: the score must come back bit-exact (D15 — no text
	// round-trip through %.17g on the binary surface).
	if got := replies[5].GetDouble(); got != 3.141592653589793 {
		t.Errorf("ZSCORE reply = %v, want exact 3.141592653589793", got)
	}
}

// TestGRPCExecErrors checks that typed commands surface the engine's
// byte-exact Redis error strings as error values (not transport errors).
func TestGRPCExecErrors(t *testing.T) {
	c := grpcTestClient(t)

	replies := execRoundTrip(t, c,
		&ultimav1.Command{Cmd: &ultimav1.Command_Rpush{Rpush: &ultimav1.RPushCommand{
			Key: []byte("g:wl"), Elems: [][]byte{[]byte("x")}}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Incr{Incr: &ultimav1.IncrCommand{
			Key: []byte("g:wl"), Delta: 1}}},
		&ultimav1.Command{Db: 16, Cmd: &ultimav1.Command_Get{Get: &ultimav1.GetCommand{
			Key: []byte("g:wl")}}},
	)
	if got := replies[1].GetError(); got != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("INCR on list error = %q, want WRONGTYPE", got)
	}
	if got := replies[2].GetError(); got != "ERR DB index is out of range" {
		t.Errorf("db-16 GET error = %q, want out of range", got)
	}
}

// TestGRPCGeneric covers the escape hatch (CommandRequest) on both the
// stream and the unary ExecGeneric form.
func TestGRPCGeneric(t *testing.T) {
	c := grpcTestClient(t)

	replies := execRoundTrip(t, c,
		&ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
			Command: "set", Args: [][]byte{[]byte("g:gen"), []byte("v")}}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
			Command: "get", Args: [][]byte{[]byte("g:gen")}}}},
		&ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
			Command: "nosuchcmd", Args: [][]byte{[]byte("x")}}}},
	)
	if got := replies[0].GetSimpleString(); got != "OK" {
		t.Errorf("generic set reply = %q, want OK", got)
	}
	if got := string(replies[1].GetBlobString()); got != "v" {
		t.Errorf("generic get reply = %q, want v", got)
	}
	if got := replies[2].GetError(); got == "" {
		t.Errorf("unknown generic command reply = %v, want an error", replies[2])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.ExecGeneric(ctx, &ultimav1.CommandRequest{
		Command: "strlen", Args: [][]byte{[]byte("g:gen")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Reply.GetInt(); got != 1 {
		t.Errorf("ExecGeneric STRLEN = %d, want 1", got)
	}
}

// TestGRPCBatch covers the unary ExecBatch form: one ConnState per call,
// one response per command, seqs echoed.
func TestGRPCBatch(t *testing.T) {
	c := grpcTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.ExecBatch(ctx, &ultimav1.BatchRequest{Commands: []*ultimav1.Command{
		{Seq: 10, Cmd: &ultimav1.Command_Mset{Mset: &ultimav1.MSetCommand{
			Pairs: []*ultimav1.MSetPair{
				{Key: []byte("g:b1"), Value: []byte("v1")},
				{Key: []byte("g:b2"), Value: []byte("v2")},
			}}}},
		{Seq: 11, Cmd: &ultimav1.Command_Mget{Mget: &ultimav1.MGetCommand{
			Keys: [][]byte{[]byte("g:b1"), []byte("g:b2"), []byte("g:b3")}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Responses) != 2 {
		t.Fatalf("ExecBatch returned %d responses, want 2", len(out.Responses))
	}
	if out.Responses[0].Seq != 10 || out.Responses[1].Seq != 11 {
		t.Errorf("batch seqs = %d,%d, want 10,11", out.Responses[0].Seq, out.Responses[1].Seq)
	}
	if got := out.Responses[0].Reply.GetSimpleString(); got != "OK" {
		t.Errorf("batch MSET reply = %q, want OK", got)
	}
	elems := out.Responses[1].Reply.GetArray().GetElems()
	if len(elems) != 3 || string(elems[0].GetBlobString()) != "v1" ||
		string(elems[1].GetBlobString()) != "v2" || !elems[2].GetNull() {
		t.Errorf("batch MGET reply = %v, want [v1 v2 null]", out.Responses[1].Reply)
	}
}

// TestGRPCExecMulti runs MULTI/EXEC over the generic envelope on a stream —
// the per-stream ConnState must give transactions the same semantics as a
// RESP connection.
func TestGRPCExecMulti(t *testing.T) {
	c := grpcTestClient(t)

	gen := func(name string, args ...string) *ultimav1.Command {
		a := &ultimav1.CommandRequest{Command: name}
		for _, s := range args {
			a.Args = append(a.Args, []byte(s))
		}
		return &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: a}}
	}
	replies := execRoundTrip(t, c,
		gen("MULTI"),
		gen("SET", "g:tx", "1"),
		gen("INCR", "g:tx"),
		gen("EXEC"),
	)
	if got := replies[0].GetSimpleString(); got != "OK" {
		t.Errorf("MULTI reply = %q, want OK", got)
	}
	if got := replies[1].GetSimpleString(); got != "QUEUED" {
		t.Errorf("SET in MULTI reply = %q, want QUEUED", got)
	}
	execArr := replies[3].GetArray()
	if execArr == nil || len(execArr.Elems) != 2 || execArr.Elems[1].GetInt() != 2 {
		t.Errorf("EXEC reply = %v, want [OK 2]", replies[3])
	}
}

// TestGRPCSubscribe covers the pub/sub push stream (§6.2): subscribe acks,
// then one PushEvent per published message, pattern deliveries included.
func TestGRPCSubscribe(t *testing.T) {
	c := grpcTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := c.Subscribe(ctx, &ultimav1.SubscribeRequest{
		Channels: []string{"g:chan"},
		Patterns: []string{"g:news.*"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Two acks first: the channel subscribe and the pattern subscribe.
	ack1, err := sub.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !ack1.IsAck || ack1.Channel != "g:chan" || ack1.Count != 1 {
		t.Errorf("ack 1 = %v, want subscribe ack g:chan count 1", ack1)
	}
	ack2, err := sub.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !ack2.IsAck || ack2.Pattern != "g:news.*" || ack2.Count != 2 {
		t.Errorf("ack 2 = %v, want psubscribe ack g:news.* count 2", ack2)
	}

	// Publish from a second gRPC connection via the generic envelope.
	pub, err := c.ExecGeneric(ctx, &ultimav1.CommandRequest{
		Command: "publish", Args: [][]byte{[]byte("g:chan"), []byte("m1")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := pub.Reply.GetInt(); got != 1 {
		t.Fatalf("PUBLISH reply = %d, want 1", got)
	}
	if _, err := c.ExecGeneric(ctx, &ultimav1.CommandRequest{
		Command: "publish", Args: [][]byte{[]byte("g:news.tech"), []byte("m2")}}); err != nil {
		t.Fatal(err)
	}

	ev, err := sub.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ev.IsAck || ev.Channel != "g:chan" || string(ev.Payload) != "m1" {
		t.Errorf("event 1 = %v, want message g:chan m1", ev)
	}
	ev, err = sub.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ev.IsAck || ev.Pattern != "g:news.*" || ev.Channel != "g:news.tech" || string(ev.Payload) != "m2" {
		t.Errorf("event 2 = %v, want pmessage g:news.* g:news.tech m2", ev)
	}

	// Cancelling the subscription drops it: a later publish reaches nobody.
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := c.ExecGeneric(context.Background(), &ultimav1.CommandRequest{
			Command: "pubsub", Args: [][]byte{[]byte("numsub"), []byte("g:chan")}})
		if err != nil {
			t.Fatal(err)
		}
		if elems := n.Reply.GetArray().GetElems(); len(elems) == 2 && elems[1].GetInt() == 0 {
			return // unsubscribed
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("subscription still live 2s after stream cancel")
}

// TestGRPCMonitor covers the MONITOR-equivalent stream: one CommandEvent
// per executed command, credentials redacted.
func TestGRPCMonitor(t *testing.T) {
	c := grpcTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mon, err := c.Monitor(ctx, &ultimav1.MonitorRequest{})
	if err != nil {
		t.Fatal(err)
	}

	// Barrier: the server registers the monitor sink asynchronously after
	// the RPC starts. Ping from a background goroutine (Recv blocks until
	// an event arrives, so pinging inline would stall after a missed
	// probe) until one ping shows up on the stream.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := c.ExecGeneric(ctx, &ultimav1.CommandRequest{Command: "ping"}); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		ev, err := mon.Recv()
		if err == nil && strings.EqualFold(ev.Command, "ping") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("monitor stream produced no events")
		}
	}

	// Drive a few commands from a separate connection. The typed SET and
	// the generic GET must both appear; the AUTH must appear redacted.
	if _, err := c.ExecGeneric(ctx, &ultimav1.CommandRequest{
		Command: "auth", Args: [][]byte{[]byte("s3cret")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecBatch(ctx, &ultimav1.BatchRequest{Commands: []*ultimav1.Command{
		{Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{Key: []byte("m:k"), Value: []byte("v")}}},
		{Cmd: &ultimav1.Command_Generic{Generic: &ultimav1.CommandRequest{
			Command: "get", Args: [][]byte{[]byte("m:k")}}}},
	}}); err != nil {
		t.Fatal(err)
	}

	want := []string{"auth", "set", "get"}
	seen := map[string]*ultimav1.CommandEvent{}
	for len(seen) < len(want) {
		ev, err := mon.Recv()
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range want {
			if strings.EqualFold(ev.Command, w) {
				if _, dup := seen[w]; !dup {
					seen[w] = ev
				}
			}
		}
	}
	if len(seen["auth"].Args) != 0 {
		t.Errorf("AUTH monitor event carried args %q, want redacted", seen["auth"].Args)
	}
	if got := seen["set"].Args; len(got) != 2 || string(got[0]) != "m:k" || string(got[1]) != "v" {
		t.Errorf("SET monitor args = %q, want [m:k v]", got)
	}
	if seen["get"].UnixMs <= 0 {
		t.Errorf("monitor event unix_ms = %d, want > 0", seen["get"].UnixMs)
	}
}
