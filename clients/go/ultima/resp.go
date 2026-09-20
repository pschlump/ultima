// Package ultima is the Go client library for the Ultima server (design
// doc §11.1): one package covering all three network surfaces — RESP
// (§6.1), gRPC (§6.2) and WebSocket (§6.3) — plus the REST management API
// (§10.1) and the shared JWT token manager (§9.3). The three operator
// CLIs (§6.4: cmd/ultima-cli, cmd/ultima-ws-cli, cmd/ultima-grpc-cli) are
// thin shells over this package.
//
// Every surface returns replies as lib/resp.Value — the same typed reply
// the command engine produces internally (decision D3) — so one rendering
// and inspection path (render.go, the As* helpers in client.go) serves
// all three.
package ultima

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pschlump/ultima/lib/resp"
)

// RESPClient is a client for the RESP surface (§6.1), wire-compatible
// with redis-cli: commands go out as arrays of bulk strings and replies
// are decoded into resp.Value by a RESP2/RESP3 parser (including push
// frames). A zero-value protocol version speaks RESP2 (the server
// downgrades RESP3-only types, like Redis); WithRESPProto(3) negotiates
// RESP3 via HELLO.
type RESPClient struct {
	conn net.Conn
	r    *bufio.Reader

	mu          sync.Mutex       // serializes Exec round-trips; guards pushHandler
	proto       int              // negotiated protocol version (2 or 3)
	pushHandler func(resp.Value) // RESP3 push frames interleaved with replies
	stream      atomic.Bool      // a streaming command (SUBSCRIBE/MONITOR) owns the conn
	closed      atomic.Bool
}

// RESPOption configures DialRESP.
type RESPOption func(*respOptions)

type respOptions struct {
	password string // requirepass AUTH on connect
	db       int    // SELECT on connect (< 0: skip)
	proto    int    // 2 or 3 (0 = 2, no HELLO)
	timeout  time.Duration
}

// WithRESPPassword authenticates the connection with AUTH (requirepass)
// right after connect.
func WithRESPPassword(pw string) RESPOption { return func(o *respOptions) { o.password = pw } }

// WithRESPDB issues SELECT db after connect (and after AUTH).
func WithRESPDB(db int) RESPOption { return func(o *respOptions) { o.db = db } }

// WithRESPProto negotiates RESP3 via HELLO after connect (and after AUTH,
// so an authenticated HELLO is not required).
func WithRESPProto(v int) RESPOption { return func(o *respOptions) { o.proto = v } }

// WithRESPDialTimeout sets the TCP dial timeout.
func WithRESPDialTimeout(d time.Duration) RESPOption {
	return func(o *respOptions) { o.timeout = d }
}

// DialRESP connects to the RESP surface at addr ("host:port") and runs
// the configured connect-time setup (AUTH, HELLO, SELECT — in that order,
// like redis-cli).
func DialRESP(addr string, opts ...RESPOption) (*RESPClient, error) {
	o := respOptions{db: -1, timeout: 10 * time.Second}
	for _, f := range opts {
		f(&o)
	}
	conn, err := net.DialTimeout("tcp", addr, o.timeout)
	if err != nil {
		return nil, err
	}
	c := &RESPClient{conn: conn, r: bufio.NewReader(conn), proto: 2}
	fail := func(err error) (*RESPClient, error) {
		_ = conn.Close()
		return nil, err
	}
	// AUTH before HELLO: with requirepass set, HELLO itself is gated
	// behind NOAUTH, and HELLO's AUTH form only exists for one-shot
	// password negotiation — the redis-cli order is AUTH, then HELLO.
	if o.password != "" {
		if v, err := c.roundTrip("AUTH", o.password); err != nil {
			return fail(err)
		} else if v.Kind == resp.KindError {
			return fail(errors.New(v.Str))
		}
	}
	if o.proto == 3 {
		v, err := c.roundTrip("HELLO", "3")
		if err != nil {
			return fail(err)
		}
		if v.Kind == resp.KindError {
			return fail(errors.New(v.Str))
		}
		c.proto = 3
	}
	if o.db >= 0 {
		if v, err := c.roundTrip("SELECT", strconv.Itoa(o.db)); err != nil {
			return fail(err)
		} else if v.Kind == resp.KindError {
			return fail(errors.New(v.Str))
		}
	}
	return c, nil
}

// Close closes the connection. A blocked Stream call returns with an
// error.
func (c *RESPClient) Close() error {
	c.closed.Store(true)
	return c.conn.Close()
}

// Exec sends one command (args[0] is the command name) and returns its
// reply. Error replies come back as resp.Value{Kind: KindError} — not as
// Go errors — matching redis-cli semantics; only transport/protocol
// failures produce a non-nil error. Arguments accept string, []byte,
// ints, floats and bools.
//
// If a push frame (KindPush — pub/sub delivery on a RESP3 connection)
// arrives while waiting for the reply, it is handed to the handler
// registered with SetPushHandler and reading continues.
func (c *RESPClient) Exec(args ...any) (resp.Value, error) {
	if c.stream.Load() {
		return resp.Value{}, errors.New("ultima: connection is in streaming mode (SUBSCRIBE/MONITOR)")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.roundTrip(args...)
}

// roundTrip is the lock-holder request/reply exchange.
func (c *RESPClient) roundTrip(args ...any) (resp.Value, error) {
	if err := writeCommand(c.conn, args); err != nil {
		return resp.Value{}, err
	}
	for {
		v, err := c.readValue()
		if err != nil {
			return resp.Value{}, err
		}
		// RESP3 push frames may interleave with command replies; route
		// them to the push handler and keep waiting for the reply.
		if v.Kind == resp.KindPush && c.pushHandler != nil {
			c.pushHandler(v)
			continue
		}
		return v, nil
	}
}

// SetPushHandler installs a handler for RESP3 push frames that arrive
// interleaved with command replies. It is called synchronously from the
// Exec read loop and must not call back into the client.
func (c *RESPClient) SetPushHandler(h func(resp.Value)) {
	c.mu.Lock()
	c.pushHandler = h
	c.mu.Unlock()
}

// Stream sends args (SUBSCRIBE, PSUBSCRIBE or MONITOR) and then reads
// frames forever, invoking handler for every reply/push frame —
// subscription acknowledgements included. It returns only when the
// connection fails or Close is called (redis-cli's subscribe mode,
// ended by Ctrl-C). While Stream runs, Exec returns an error.
func (c *RESPClient) Stream(handler func(resp.Value), args ...any) error {
	if !c.stream.CompareAndSwap(false, true) {
		return errors.New("ultima: connection already streaming")
	}
	defer c.stream.Store(false)
	if c.closed.Load() {
		return errors.New("ultima: connection closed")
	}
	if err := writeCommand(c.conn, args); err != nil {
		return err
	}
	for {
		v, err := c.readValue()
		if err != nil {
			return err
		}
		handler(v)
	}
}

// writeCommand renders args as one RESP array of bulk strings.
func writeCommand(w io.Writer, args []any) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		s := argString(a)
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(s), s)
	}
	_, err := io.WriteString(w, sb.String())
	return err
}

// argString converts one command argument to its wire string.
func argString(a any) string {
	switch v := a.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", a)
	}
}

// --- RESP2/RESP3 reply parser ------------------------------------------------

// readValue decodes exactly one reply frame (scalar, aggregate or push)
// into resp.Value. The RESP3-first byte set is the full type system
// (lib/resp/value.go); RESP2 replies are a subset and decode identically.
func (c *RESPClient) readValue() (resp.Value, error) {
	line, err := c.readLine()
	if err != nil {
		return resp.Value{}, err
	}
	if line == "" {
		return resp.Value{}, errors.New("ultima: empty reply line")
	}
	typ, arg := line[0], line[1:]
	switch typ {
	case '+':
		return resp.Simple(arg), nil
	case '-':
		return resp.Err(arg), nil
	case ':':
		n, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			return resp.Value{}, protoErr("integer", line)
		}
		return resp.Int(n), nil
	case '$':
		n, err := strconv.Atoi(arg)
		if err != nil {
			return resp.Value{}, protoErr("bulk length", line)
		}
		if n < 0 {
			return resp.Null(), nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return resp.Value{}, err
		}
		return resp.BlobString(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(arg)
		if err != nil {
			return resp.Value{}, protoErr("array length", line)
		}
		if n < 0 {
			return resp.Null(), nil
		}
		elems, err := c.readElems(n)
		return resp.Arr(elems...), err
	case '%':
		n, err := strconv.Atoi(arg)
		if err != nil {
			return resp.Value{}, protoErr("map length", line)
		}
		// resp.Value keeps maps as flattened k,v pairs (lib/resp).
		elems, err := c.readElems(2 * n)
		return resp.Map(elems...), err
	case '~':
		n, err := strconv.Atoi(arg)
		if err != nil {
			return resp.Value{}, protoErr("set length", line)
		}
		elems, err := c.readElems(n)
		return resp.Set(elems...), err
	case '>':
		n, err := strconv.Atoi(arg)
		if err != nil {
			return resp.Value{}, protoErr("push length", line)
		}
		elems, err := c.readElems(n)
		return resp.Push(elems...), err
	case '_':
		return resp.Null(), nil
	case '#':
		switch arg {
		case "t":
			return resp.Bool(true), nil
		case "f":
			return resp.Bool(false), nil
		}
		return resp.Value{}, protoErr("boolean", line)
	case ',':
		f, err := strconv.ParseFloat(arg, 64)
		if err != nil {
			return resp.Value{}, protoErr("double", line)
		}
		return resp.Double(f), nil
	case '(':
		return resp.Value{Kind: resp.KindBigNumber, Str: arg}, nil
	case '=':
		n, err := strconv.Atoi(arg)
		if err != nil || n < 4 {
			return resp.Value{}, protoErr("verbatim length", line)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return resp.Value{}, err
		}
		return resp.Value{Kind: resp.KindVerbatim, Fmt: string(buf[:3]), Blob: buf[4:n]}, nil
	}
	return resp.Value{}, protoErr("type byte", line)
}

func (c *RESPClient) readElems(n int) ([]resp.Value, error) {
	elems := make([]resp.Value, 0, n)
	for i := 0; i < n; i++ {
		v, err := c.readValue()
		if err != nil {
			return nil, err
		}
		elems = append(elems, v)
	}
	return elems, nil
}

// readLine consumes one CRLF-terminated line and returns it without the
// terminator.
func (c *RESPClient) readLine() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func protoErr(what, line string) error {
	return fmt.Errorf("ultima: protocol error: bad %s in reply line %q", what, line)
}
