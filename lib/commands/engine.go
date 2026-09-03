// Package commands is Ultima's front-end-agnostic command engine
// (decision D3): every front-end (RESP today, gRPC/WS later) builds a
// Command out of (ConnState, name, args) and renders the returned
// resp.Value. All P0 commands carry Redis-exact semantics and error
// strings, validated against Redis 7.2.7 by tests/differential.
package commands

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// CompatVersion is the Redis version Ultima reports for drop-in
// compatibility (HELLO version, INFO redis_version).
const CompatVersion = "7.2.7"

// ConnState is the per-connection context shared by all front-ends
// (design doc §6.1 modification #3). Front-ends create one per connection
// with Engine.NewConnState and drop it with Engine.CloseConn.
type ConnState struct {
	Proto   int // negotiated RESP version: 2 or 3
	Authed  bool
	Name    string
	DB      int
	ID      uint64
	Addr    string
	Created time.Time
	Quit    bool // set by QUIT; the front-end closes the connection

	// Room for M3: MULTI/EXEC state, WATCH versions, pub/sub mode.
}

// Engine executes commands against the shard engine, holding the
// server-wide mutable knobs (requirepass, maxmemory, …) that CONFIG SET
// and AUTH touch.
type Engine struct {
	Shards   *shard.Engine
	Version  string // Ultima build version, for INFO
	Started  time.Time
	RunID    string
	RespPort int

	requirePass atomic.Value // string
	maxMemory   atomic.Int64
	save        atomic.Value // string
	appendOnly  atomic.Bool

	clientID   atomic.Uint64
	conns      atomic.Int64
	totalConns atomic.Int64
	totalCmds  atomic.Int64
}

// NewEngine wraps sh with the command layer. respPort feeds INFO.
func NewEngine(sh *shard.Engine, version string, respPort int) *Engine {
	e := &Engine{
		Shards:   sh,
		Version:  version,
		Started:  time.Now(),
		RunID:    genRunID(),
		RespPort: respPort,
	}
	e.requirePass.Store("")
	e.save.Store("3600 1 300 100 60 10000")
	return e
}

// genRunID returns a 40-hex-char run id, like Redis's.
func genRunID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// RequirePass returns the current requirepass ("" = auth disabled).
func (e *Engine) RequirePass() string { return e.requirePass.Load().(string) }

// SetRequirePass updates requirepass (CONFIG SET requirepass).
func (e *Engine) SetRequirePass(pw string) { e.requirePass.Store(pw) }

// MaxMemory returns the configured maxmemory in bytes (0 = unlimited).
func (e *Engine) MaxMemory() int64 { return e.maxMemory.Load() }

// SetMaxMemory updates maxmemory (config file, CONFIG SET).
func (e *Engine) SetMaxMemory(n int64) { e.maxMemory.Store(n) }

// NewConnState registers a connection and returns its state.
func (e *Engine) NewConnState(addr string) *ConnState {
	id := e.clientID.Add(1) + 2 // Redis IDs start at 3 (0-2 are internal)
	e.conns.Add(1)
	e.totalConns.Add(1)
	return &ConnState{Proto: 2, ID: id, Addr: addr, Created: time.Now()}
}

// CloseConn deregisters a connection.
func (e *Engine) CloseConn(_ *ConnState) { e.conns.Add(-1) }

// Execute runs one command. args[0] is the command name (any case).
// It never panics on client input; all errors are reply values.
func (e *Engine) Execute(cs *ConnState, args [][]byte) resp.Value {
	if len(args) == 0 {
		return errUnknownCommand("", nil)
	}
	e.totalCmds.Add(1)
	name := lowerASCII(args[0])
	def, ok := table[name]
	if !ok {
		return errUnknownCommand(string(args[0]), args[1:])
	}

	// NOAUTH gate: with requirepass set, only AUTH/HELLO/QUIT run
	// unauthenticated (Redis semantics).
	if pw := e.RequirePass(); pw != "" && !cs.Authed {
		switch name {
		case "auth", "hello", "quit":
		default:
			return resp.Err("NOAUTH Authentication required.")
		}
	}

	// Arity: positive = exact, negative = at least -arity.
	if (def.Arity > 0 && len(args) != def.Arity) ||
		(def.Arity < 0 && len(args) < -def.Arity) {
		return errArity(def.FullName())
	}
	return def.Handler(e, cs, args)
}

// --- shared reply helpers -------------------------------------------------

func errArity(fullname string) resp.Value {
	return resp.Err(fmt.Sprintf("ERR wrong number of arguments for '%s' command", fullname))
}

var (
	errSyntax    = resp.Err("ERR syntax error")
	errNotInt    = resp.Err("ERR value is not an integer or out of range")
	errIncrOvf   = resp.Err("ERR increment or decrement would overflow")
	errDecrOvf   = resp.Err("ERR decrement would overflow")
	errWrongType = resp.Err("WRONGTYPE Operation against a key holding the wrong kind of value")
	replyOK      = resp.Simple("OK")
)

// errUnknownCommand matches Redis: name as given (≤128 bytes), up to 19
// leading args each quoted and truncated to 128 bytes, trailing space.
func errUnknownCommand(name string, args [][]byte) resp.Value {
	var sb strings.Builder
	sb.WriteString("ERR unknown command '")
	if len(name) > 128 {
		name = name[:128]
	}
	sb.WriteString(name)
	sb.WriteString("', with args beginning with: ")
	for i, a := range args {
		if i >= 19 {
			break
		}
		s := string(a)
		if len(s) > 128 {
			s = s[:128]
		}
		sb.WriteByte('\'')
		sb.WriteString(s)
		sb.WriteString("' ")
	}
	return resp.Err(sb.String())
}

// lowerASCII lowercases b (command names are ASCII) without allocating
// when already lowercase.
func lowerASCII(b []byte) string {
	for _, c := range b {
		if c >= 'A' && c <= 'Z' {
			l := make([]byte, len(b))
			for i, c2 := range b {
				if c2 >= 'A' && c2 <= 'Z' {
					c2 += 'a' - 'A'
				}
				l[i] = c2
			}
			return string(l)
		}
	}
	return string(b)
}

// parseIntStrict mirrors Redis string2ll: optional '-', no '+' or spaces,
// no leading zeros ("0" itself is fine), range-checked int64.
func parseIntStrict(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	i := 0
	neg := false
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		if len(b)-i != 1 {
			return 0, false // leading zeros
		}
		if neg {
			return 0, false // "-0" is rejected by string2ll
		}
		return 0, true
	}
	var v uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + uint64(c-'0')
		if v > 1<<63 { // rejects 2^63 exactly; handled below for negatives
			return 0, false
		}
	}
	if neg {
		if v == 1<<63 {
			return -1 << 63, true
		}
		return -int64(v), true
	}
	if v >= 1<<63 {
		return 0, false
	}
	return int64(v), true
}

// parseCursor mirrors Redis's strtoul cursor parse: decimal, with a
// leading '-' wrapping into uint64 (SCAN -1 completes immediately).
func parseCursor(b []byte) (uint64, bool) {
	if v, ok := parseIntStrict(b); ok {
		return uint64(v), true // negative wraps, as strtoul does
	}
	return 0, false
}

// parseMemBytes parses a Redis memory value: integer with optional
// k/kb/m/mb/g/gb suffix (case-insensitive); plain numbers are bytes.
func parseMemBytes(b []byte) (int64, bool) {
	s := strings.ToLower(string(b))
	mult := int64(1)
	for _, suf := range []struct {
		s string
		m int64
	}{{"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30}, {"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}} {
		if strings.HasSuffix(s, suf.s) {
			mult = suf.m
			s = s[:len(s)-len(suf.s)]
			break
		}
	}
	v, ok := parseIntStrict([]byte(s))
	if !ok || v < 0 {
		return 0, false
	}
	if mult > 1 && v > (1<<62)/mult {
		return 0, false
	}
	return v * mult, true
}
