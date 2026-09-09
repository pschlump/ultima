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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pschlump/ultima/lib/pubsub"
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

	// Multi-mode transaction state (M3, §4.2): Multi marks the connection
	// between MULTI and EXEC/DISCARD; Queue holds the queued commands
	// (deep copies of the client's args); QueueErr records a queue-time
	// error (unknown command, bad arity) that makes EXEC abort with
	// EXECABORT; Watch holds the keys sampled by WATCH for EXEC's dirty
	// check.
	Multi    bool
	Queue    [][][]byte
	QueueErr bool
	Watch    []WatchRef

	// tok is the pause token this connection's shard tasks carry while it
	// executes a transaction under EXEC (§4.2); 0 outside EXEC.
	tok uint64

	// inExec marks the execution phase of EXEC; blocking commands (M3
	// part 5) consult it to take their non-blocking fast path.
	inExec bool

	// Pub/sub state (M3): subs and psubs are this connection's channel
	// and pattern subscription sets; a RESP2 connection with any live
	// subscription is in subscribe mode (see the gate in Execute).
	// StartPush is the front-end hook (lib/respserver) that lazily
	// creates the per-connection push writer on first subscription and
	// returns its enqueue func, cached in deliver; nil in unit tests,
	// in which case broker deliveries to this connection are dropped.
	// outbox holds extra reply frames produced by multi-frame commands
	// (subscribe acks beyond the first, self-addressed publish pushes
	// postponed behind the command reply); the front-end drains it with
	// DrainOutbox after every Execute, after the command reply.
	subs      map[string]struct{}
	psubs     map[string]struct{}
	StartPush func() func(resp.Value)
	deliver   func(resp.Value)
	outbox    []resp.Value
}

// WatchRef is one WATCHed key's dirty-check state: the logical DB, the
// key, and the (epoch, version) sample taken by Shard.WatchVersion.
type WatchRef struct {
	DB         int
	Key        string
	Epoch, Ver uint64
}

// clearTx leaves multi mode: the queue, the queue-error flag, and all
// watched keys are dropped. EXEC (whatever its outcome) and DISCARD both
// end here.
func (cs *ConnState) clearTx() {
	cs.Multi = false
	cs.Queue = nil
	cs.QueueErr = false
	cs.Watch = nil
}

// Engine executes commands against the shard engine, holding the
// server-wide mutable knobs (requirepass, maxmemory, …) that CONFIG SET
// and AUTH touch.
type Engine struct {
	Shards   *shard.Engine
	PubSub   *pubsub.Broker // classic pub/sub registry (M3); channels are global, not per-DB
	Version  string         // Ultima build version, for INFO
	Started  time.Time
	RunID    string
	RespPort int

	requirePass atomic.Value // string
	maxMemory   atomic.Int64
	save        atomic.Value // string
	appendOnly  atomic.Bool

	// Keyspace notifications (M5): notifyFlags is the parsed
	// notify-keyspace-events class bitmask (hot-path gate in
	// notifyKeyspace); CONFIG GET renders it back to the canonical
	// string (notifyFlagsToString), as Redis does.
	notifyFlags atomic.Uint64

	clientID   atomic.Uint64
	conns      atomic.Int64
	totalConns atomic.Int64
	totalCmds  atomic.Int64

	// blockedClients counts connections currently parked in a blocking
	// command (BLPOP…BZMPOP), surfaced as INFO blocked_clients.
	blockedClients atomic.Int64

	// Monitors (§6.2 MonitorEvent feed for the gRPC Monitor stream):
	// monitorN is the hot-path gate (one atomic load per command);
	// monitorMu guards the sink map for register/deregister/fan-out.
	monitorN   atomic.Int64
	monitorSeq atomic.Uint64
	monitorMu  sync.Mutex
	monitors   map[uint64]func(MonitorEvent)
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
	e.PubSub = pubsub.New(func(pattern, s string) bool {
		return GlobMatch([]byte(pattern), []byte(s))
	})
	e.monitors = map[uint64]func(MonitorEvent){}
	e.requirePass.Store("")
	e.save.Store("3600 1 300 100 60 10000")
	// Expiry-driven keyspace notifications (M5): the shard engine reports
	// passive/active expiry deletions; route them to the broker.
	sh.SetOnKeyGone(func(db int, key, reason string) {
		e.notifyKeyspace(db, key, reason)
	})
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

// CloseConn deregisters a connection: its pub/sub subscriptions are
// dropped from the broker, transaction/watch state is cleared, and the
// client counter is decremented.
func (e *Engine) CloseConn(cs *ConnState) {
	if cs == nil {
		return
	}
	e.PubSub.UnsubscribeAll(cs.ID)
	cs.subs, cs.psubs = nil, nil
	cs.clearTx()
	e.conns.Add(-1)
}

// DrainOutbox returns and clears the extra reply frames accumulated by
// the last Execute (multi-frame subscribe acks, postponed self-addressed
// publish pushes). The front-end writes them after the command reply.
func (cs *ConnState) DrainOutbox() []resp.Value {
	out := cs.outbox
	cs.outbox = nil
	return out
}

// DeliverFunc returns the front-end push enqueue hook, non-nil once push
// mode has started (first subscription and the front-end set StartPush).
// The RESP front-end uses it to decide whether command replies must flow
// through the push queue to preserve ordering against async pushes.
func (cs *ConnState) DeliverFunc() func(resp.Value) { return cs.deliver }

// do runs fn inside the goroutine of the shard owning key, carrying the
// connection's transaction pause token (0 outside EXEC, §4.2). All
// command handlers submit per-key work through do/doMulti so EXEC can
// run them inside the paused engine.
func (e *Engine) do(cs *ConnState, key []byte, fn func(s *shard.Shard)) {
	e.Shards.DoTok(cs.tok, e.Shards.ShardIndex(key), fn)
}

// doMulti is the multi-key form of do (see shard.Engine.DoMultiTok).
// fn may run CONCURRENTLY on several shard goroutines: shared writes
// must be synchronized (atomic/mutex) or slot-indexed by key position.
func (e *Engine) doMulti(cs *ConnState, keys [][]byte, fn func(s *shard.Shard, idxs []int)) {
	e.Shards.DoMultiTok(cs.tok, keys, fn)
}

// Execute runs one command. args[0] is the command name (any case).
// It never panics on client input; all errors are reply values.
func (e *Engine) Execute(cs *ConnState, args [][]byte) resp.Value {
	if len(args) == 0 {
		return errUnknownCommand("", nil)
	}
	e.totalCmds.Add(1)
	name := lowerASCII(args[0])
	def, ok := table[name]

	// Queue gate (Redis queueMultiCommand, §4.2): inside MULTI every
	// command except the transaction-control set is queued instead of
	// executed. A queue-time failure (unknown command, bad arity) dirties
	// the transaction so EXEC aborts with EXECABORT. An unknown command
	// dirties even before the NOAUTH gate, as in Redis processCommand.
	queueable := cs.Multi
	switch name {
	case "exec", "discard", "multi", "watch", "quit", "reset":
		queueable = false
	}
	if queueable && !ok {
		cs.QueueErr = true
		return errUnknownCommand(string(args[0]), args[1:])
	}
	if !ok {
		return errUnknownCommand(string(args[0]), args[1:])
	}

	// NOAUTH gate: with requirepass set, only AUTH/HELLO/QUIT/RESET run
	// unauthenticated (Redis semantics).
	if pw := e.RequirePass(); pw != "" && !cs.Authed {
		switch name {
		case "auth", "hello", "quit", "reset":
		default:
			return resp.Err("NOAUTH Authentication required.")
		}
	}

	// Arity: positive = exact, negative = at least -arity.
	if (def.Arity > 0 && len(args) != def.Arity) ||
		(def.Arity < 0 && len(args) < -def.Arity) {
		if queueable {
			cs.QueueErr = true
		}
		return errArity(def.FullName())
	}

	// Subscribe-mode gate (Redis processCommand, verified against
	// 7.2.7): a RESP2 connection with live subscriptions may run only
	// the (P)SUBSCRIBE/(P)UNSUBSCRIBE family, PING, QUIT and RESET;
	// RESP3 connections are not gated at all. Unknown commands and
	// arity errors (above) fire before this gate, as does a container
	// command with an unresolvable subcommand — for those the handler's
	// own "unknown subcommand" error reaches the client (Redis fails
	// command lookup before the gate). EXEC is special: its rejection
	// is wrapped in EXECABORT with the reason embedded.
	if cs.Proto == 2 && len(cs.subs)+len(cs.psubs) > 0 {
		switch name {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit", "reset":
		default:
			if fn := gatedFullname(name, args); fn != "" {
				gateMsg := "Can't execute '" + fn + "': only (P|S)SUBSCRIBE / " +
					"(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
				if name == "exec" {
					return resp.Err("EXECABORT Transaction discarded because of: " + gateMsg)
				}
				return resp.Err("ERR " + gateMsg)
			}
		}
	}

	if queueable {
		cs.Queue = append(cs.Queue, cloneArgs(args))
		return replyQueued
	}
	v := def.Handler(e, cs, args)
	e.notifyMonitors(cs, args)
	return v
}

// MonitorEvent is one executed command as reported to MONITOR streams
// (design doc §6.2). Args is a deep copy safe for the sink to retain;
// credentials never appear in it (AUTH and HELLO-with-AUTH are redacted
// the way Redis redacts them from MONITOR output).
type MonitorEvent struct {
	When time.Time
	DB   int
	Addr string
	Args [][]byte // Args[0] is the command name as the client sent it
}

// AddMonitor registers fn to receive a MonitorEvent for every command
// executed (after execution, on the executing connection's goroutine —
// sinks must be cheap and non-blocking; a full queue should drop or
// disconnect, not stall the engine). Commands queued inside MULTI are not
// reported individually: MONITOR sees the EXEC. The returned func
// deregisters. The hot path costs one atomic load when no monitors are
// registered.
func (e *Engine) AddMonitor(fn func(MonitorEvent)) (cancel func()) {
	id := e.monitorSeq.Add(1)
	e.monitorMu.Lock()
	e.monitors[id] = fn
	e.monitorMu.Unlock()
	e.monitorN.Add(1)
	return func() {
		e.monitorMu.Lock()
		delete(e.monitors, id)
		e.monitorMu.Unlock()
		e.monitorN.Add(-1)
	}
}

// notifyMonitors fans one executed command out to the registered monitors.
func (e *Engine) notifyMonitors(cs *ConnState, args [][]byte) {
	if e.monitorN.Load() == 0 {
		return
	}
	ev := MonitorEvent{When: time.Now(), DB: cs.DB, Addr: cs.Addr, Args: monitorArgs(args)}
	e.monitorMu.Lock()
	defer e.monitorMu.Unlock()
	for _, fn := range e.monitors {
		fn(ev)
	}
}

// monitorArgs copies args for the monitor feed, redacting credentials:
// AUTH collapses to just its name and HELLO keeps only its protocol
// version, matching Redis's MONITOR redaction.
func monitorArgs(args [][]byte) [][]byte {
	name := lowerASCII(args[0])
	switch name {
	case "auth":
		return [][]byte{[]byte("auth")}
	case "hello":
		out := [][]byte{args[0]}
		if len(args) > 1 && (string(args[1]) == "2" || string(args[1]) == "3") {
			out = append(out, args[1])
		}
		return out
	default:
		return cloneArgs(args)
	}
}

// containerSubs maps each container command Ultima implements to its
// known subcommands, for the subscribe-mode gate's fullname rendering
// (Redis reports "config|get"-style canonical names there).
var containerSubs = map[string]map[string]struct{}{
	"config":  {"get": {}, "set": {}},
	"client":  {"setname": {}, "getname": {}, "id": {}, "setinfo": {}},
	"command": {"count": {}, "info": {}},
	"pubsub":  {"channels": {}, "numsub": {}, "numpat": {}},
}

// gatedFullname renders the canonical fullname Redis reports in the
// subscribe-mode gate error: "name|sub" for container commands whose
// subcommand resolves, the bare name otherwise. It returns "" for a
// container command whose subcommand does not resolve: Redis fails
// command lookup before the gate, so the handler's own unknown-
// subcommand error must reach the client instead of the gate error.
func gatedFullname(name string, args [][]byte) string {
	subs, container := containerSubs[name]
	if !container || len(args) < 2 {
		return name
	}
	sub := lowerASCII(args[1])
	if _, ok := subs[sub]; ok {
		return name + "|" + sub
	}
	return ""
}

// cloneArgs deep-copies a command line for the MULTI queue; EXEC must be
// immune to the client (or front-end) mutating or reusing the buffers.
func cloneArgs(args [][]byte) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		b := make([]byte, len(a))
		copy(b, a)
		out[i] = b
	}
	return out
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
