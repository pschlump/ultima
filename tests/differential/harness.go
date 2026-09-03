// Package differential runs scripted command sequences against Ultima and
// a real redis-server and diffs the decoded replies, including error
// strings (design doc §14.3 #3 — the primary parity gate).
//
// Known deviation: the design doc targets Redis 8.x built from
// note/redis, but that tree is not built in this environment; the harness
// therefore runs against the redis-server on PATH (7.2.7), selected by the
// REDIS_BIN environment variable (default "redis-server"). The P0
// semantics under test are stable across 7.2/8.x; version-reporting
// surfaces (INFO/HELLO content) are compared structurally, not by value.
//
// Ultima runs in-process (shard engine + command engine + lib/resp
// listener on an ephemeral port). Redis runs as a subprocess on an
// ephemeral port with persistence disabled.
//
// Skipped under -short or when DIFFERENTIAL=0.
package differential

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func redisBin() string {
	if b := os.Getenv("REDIS_BIN"); b != "" {
		return b
	}
	return "redis-server"
}

// --- minimal RESP2/RESP3 client -------------------------------------------------

// rconn is a barebones RESP connection that decodes replies into Values.
type rconn struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *rconn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &rconn{conn: c, r: bufio.NewReader(c)}
}

// Value is a decoded RESP reply, comparable across servers.
type Value struct {
	Kind byte // '+', '-', ':', '$', '*', '%', '~', ',', '#', '_', '(', '>'
	Str  string
	Int  int64
	Dbl  string // doubles compared as text (both servers use %.17g)
	Vals []Value
	Null bool
}

func (v Value) String() string {
	switch {
	case v.Null:
		return "(null)"
	case v.Kind == '+' || v.Kind == '-' || v.Kind == '$' || v.Kind == '(':
		return fmt.Sprintf("%c %q", v.Kind, v.Str)
	case v.Kind == ':':
		return fmt.Sprintf(": %d", v.Int)
	case v.Kind == ',':
		return fmt.Sprintf(", %s", v.Dbl)
	case v.Kind == '#':
		return fmt.Sprintf("# %s", v.Str)
	default:
		parts := make([]string, len(v.Vals))
		for i, e := range v.Vals {
			parts[i] = e.String()
		}
		return fmt.Sprintf("%c [%s]", v.Kind, strings.Join(parts, " "))
	}
}

func (c *rconn) do(args ...string) Value {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.conn.Write([]byte(sb.String())); err != nil {
		return Value{Kind: '!', Str: "write: " + err.Error()}
	}
	v, err := c.read()
	if err != nil {
		return Value{Kind: '!', Str: "read: " + err.Error()}
	}
	return v
}

func (c *rconn) readLine() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

func (c *rconn) read() (Value, error) {
	line, err := c.readLine()
	if err != nil {
		return Value{}, err
	}
	if line == "" {
		return Value{}, errors.New("empty reply line")
	}
	kind, body := line[0], line[1:]
	switch kind {
	case '+', '-':
		return Value{Kind: kind, Str: body}, nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		return Value{Kind: kind, Int: n}, err
	case ',':
		return Value{Kind: kind, Dbl: body}, nil
	case '#':
		return Value{Kind: kind, Str: body}, nil
	case '_':
		return Value{Kind: '_', Null: true}, nil
	case '(':
		return Value{Kind: kind, Str: body}, nil
	case '$', '=':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: '$', Null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return Value{}, err
		}
		return Value{Kind: '$', Str: string(buf[:n])}, nil
	case '*', '%', '~', '>':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: '*', Null: true}, nil
		}
		if kind == '%' {
			n *= 2 // map: compare as flattened pairs
			kind = '%'
		}
		vals := make([]Value, 0, n)
		for i := 0; i < n; i++ {
			v, err := c.read()
			if err != nil {
				return Value{}, err
			}
			vals = append(vals, v)
		}
		return Value{Kind: kind, Vals: vals}, nil
	}
	return Value{}, fmt.Errorf("unknown reply type %q in %q", kind, line)
}

// --- servers under test ----------------------------------------------------------

// startRedis spawns redis-server on an ephemeral port with persistence
// off; requirepass is set when non-empty.
func startRedis(t *testing.T, requirepass string) string {
	t.Helper()
	port := freePort(t)
	args := []string{"--port", strconv.Itoa(port), "--save", "", "--appendonly", "no",
		"--enable-debug-command", "yes"}
	if requirepass != "" {
		args = append(args, "--requirepass", requirepass)
	}
	cmd := exec.Command(redisBin(), args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start %s: %v", redisBin(), err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = c.Close()
			return fmt.Sprintf("127.0.0.1:%d", port)
		}
		if time.Now().After(deadline) {
			t.Fatalf("redis-server did not come up on :%d", port)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}
