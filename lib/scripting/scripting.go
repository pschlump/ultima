// Package scripting wraps the gopher-lua host engine as Ultima's Lua
// scripting subsystem (M8, design doc §7 P3, D12): the script cache behind
// EVAL/EVALSHA/SCRIPT LOAD, the per-run VM lifecycle, the redis.* host
// function surface (redis.call and friends), the soft BUSY limit
// (lua-time-limit) and the hard watchdog deadline (S5), and the
// Lua⇄RESP conversions (host-side number formatting, decision S7).
//
// Import direction is one-way: lib/commands → lib/scripting →
// gopher-lua/host. The bridge that runs Redis commands from inside a
// script is injected per run as a CallPath closure (S10: this package
// knows nothing about the command engine).
//
// Lifecycle (S4): a fresh host.VM per EVAL execution, created BEFORE the
// shard pause (instantiation is the slow part and touches no keyspace);
// only vm.Run runs under PauseAll. Compiled scripts (wasm bytes) are
// cached process-wide by SHA-1.
package scripting

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pschlump/gopher-lua/host"
	"github.com/pschlump/ultima/lib/resp"
)

// ChunkName is the Lua chunk name scripts are compiled under. Redis
// compiles with "@user_script" and its messages strip the '@'; the
// gopher-lua frontend renders the name verbatim, so we compile under the
// stripped form and add the '@' back only in the error-reply suffix.
const ChunkName = "user_script"

// Config carries the script.* configuration group (§5.1 of the M8
// integration guide) plus the server values the Lua surface mirrors.
type Config struct {
	// LuaTimeLimitMs mirrors Redis's lua-time-limit: the soft limit past
	// which other clients get BUSY while a script runs.
	LuaTimeLimitMs int
	// HardDeadlineMs is the watchdog kill (decision S5 — an Ultima
	// divergence: Redis cannot preempt, we kill at the deadline and keep
	// partial effects). 0 disables the watchdog.
	HardDeadlineMs int
	// MaxMemoryMB is the per-VM Lua allocation budget (rt_set_memlimit);
	// 0 = unlimited (the blob's 256 MiB linear-memory max still applies).
	MaxMemoryMB int
	// RngSeed bases math.random seeding (S6): 0 = derive from RunID.
	RngSeed int64
	// RunID seeds the RNG when RngSeed is 0.
	RunID string
	// CompatVersion is reported as redis.REDIS_VERSION.
	CompatVersion string
	Logger        *slog.Logger
}

// CallPath runs one command line from inside a script (redis.call /
// redis.pcall). argv[0] is the command name (any case). The returned
// resp.Value is the command's reply; wrote reports a successful write
// (SCRIPT KILL's UNKILLABLE gate). Supplied per run by lib/commands.
type CallPath func(argv []string) (reply resp.Value, wrote bool)

// Manager owns the host engine, the script cache, and the running-script
// state behind the BUSY gate and SCRIPT KILL.
type Manager struct {
	eng    *host.Engine
	cfg    Config
	logger *slog.Logger

	mu    sync.RWMutex
	cache map[string]*host.Script

	timeLimitMs    atomic.Int64
	hardDeadlineMs atomic.Int64
	rngSeed        atomic.Int64
	runSeq         atomic.Uint64

	// runs is the set of in-flight script executions, keyed by their VM
	// (the bridge HostFuncs receive the VM and find their run context
	// here). PauseAll serializes scripts globally, so the map holds at
	// most one entry in v1; the shape survives a future lazy-pause.
	runMu sync.Mutex
	runs  map[*host.VM]*run
}

// run is one in-flight script execution.
type run struct {
	vm      *host.VM
	sha     string
	ro      bool // EVAL_RO/EVALSHA_RO: writes are rejected
	call    CallPath
	start   time.Time
	respVer int // redis.setresp (2 or 3)

	wrote  atomic.Bool // a write command succeeded (SCRIPT KILL's UNKILLABLE)
	killed atomic.Bool // SCRIPT KILL requested (drives the client error text)
}

// New builds the scripting engine: the SHA-pinned production blob, the
// per-VM memory budget, and the redis.* function/constant surface
// (registered before the first VM — the host registry then freezes).
func New(cfg Config) (*Manager, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		cfg:    cfg,
		logger: logger,
		cache:  map[string]*host.Script{},
		runs:   map[*host.VM]*run{},
	}
	eng, err := host.NewEngine(
		host.WithMemoryBudgetBytes(int64(cfg.MaxMemoryMB)<<20),
		// Redis 7.2.7 script environment lockdown (script_lua.c +
		// deps/lua readonly patch): undefined-global reads and global
		// writes raise the byte-exact probed errors.
		host.WithGlobalsProtection(true),
	)
	if err != nil {
		return nil, fmt.Errorf("scripting: %w", err)
	}
	m.eng = eng
	m.timeLimitMs.Store(int64(cfg.LuaTimeLimitMs))
	m.hardDeadlineMs.Store(int64(cfg.HardDeadlineMs))
	seed := cfg.RngSeed
	if seed == 0 {
		// S6: derive from the run-id — reproducible per server run,
		// never wall-clock.
		for i := 0; i < len(cfg.RunID) && i < 16; i++ {
			seed = seed*131 + int64(cfg.RunID[i])
		}
		if seed == 0 {
			seed = 42
		}
	}
	m.rngSeed.Store(seed)
	if err := m.registerRedisSurface(); err != nil {
		return nil, err
	}
	return m, nil
}

// Close shuts the engine down; live VMs finish their runs.
func (m *Manager) Close() error { return m.eng.Close() }

// registerRedisSurface installs the redis.* Lua surface (functions +
// constants) on the host engine. Must complete before the first VM.
func (m *Manager) registerRedisSurface() error {
	fns := []struct {
		name string
		fn   host.HostFunc
	}{
		{"call", m.luaCall},
		{"pcall", m.luaPCall},
		{"error_reply", m.luaErrorReply},
		{"status_reply", m.luaStatusReply},
		{"sha1hex", m.luaSHA1Hex},
		{"log", m.luaLog},
		{"setresp", m.luaSetResp},
	}
	for _, f := range fns {
		if err := m.eng.RegisterGlobal("redis", f.name, f.fn); err != nil {
			return err
		}
	}
	vals := []struct {
		name string
		v    host.Value
	}{
		{"LOG_DEBUG", host.Int(0)},
		{"LOG_VERBOSE", host.Int(1)},
		{"LOG_NOTICE", host.Int(2)},
		{"LOG_WARNING", host.Int(3)},
		{"REDIS_VERSION", host.String(m.cfg.CompatVersion)},
	}
	for _, c := range vals {
		if err := m.eng.RegisterValue("redis", c.name, c.v); err != nil {
			return err
		}
	}
	return nil
}

// --- script cache -----------------------------------------------------------

// Compile parses+lowers source (cached by SHA-1) and returns its script
// identity. Compile errors are the frontend's message verbatim; the
// caller wraps them in Redis's "Error compiling script" form.
func (m *Manager) Compile(source []byte) (sha string, s *host.Script, err error) {
	s, err = m.eng.Compile(source, ChunkName)
	if err != nil {
		return "", nil, err
	}
	m.mu.Lock()
	m.cache[s.SHA1] = s
	m.mu.Unlock()
	return s.SHA1, s, nil
}

// Lookup finds a cached script by SHA-1 hex (EVALSHA).
func (m *Manager) Lookup(sha string) (*host.Script, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.cache[sha]
	return s, ok
}

// Exists reports per-sha cache presence (SCRIPT EXISTS).
func (m *Manager) Exists(shas ...string) []bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]bool, len(shas))
	for i, sha := range shas {
		_, out[i] = m.cache[sha]
	}
	return out
}

// Flush drops the cache (SCRIPT FLUSH). A script currently running keeps
// its wasm bytes alive via its *host.Script reference — only future
// lookups miss, matching Redis's tolerance of FLUSH mid-script.
func (m *Manager) Flush() {
	m.mu.Lock()
	m.cache = map[string]*host.Script{}
	m.mu.Unlock()
}

// Cached reports the number of cached scripts (INFO loaded_scripts).
func (m *Manager) Cached() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.cache)
}

// --- knobs ------------------------------------------------------------------

// TimeLimitMs returns the soft BUSY limit (lua-time-limit).
func (m *Manager) TimeLimitMs() int64 { return m.timeLimitMs.Load() }

// SetTimeLimitMs updates the soft limit (CONFIG SET lua-time-limit).
func (m *Manager) SetTimeLimitMs(v int64) { m.timeLimitMs.Store(v) }

// HardDeadlineMs returns the watchdog kill (script-hard-deadline-ms).
func (m *Manager) HardDeadlineMs() int64 { return m.hardDeadlineMs.Load() }

// RngSeedBase returns the RNG seed base (script-rng-seed).
func (m *Manager) RngSeedBase() int64 { return m.rngSeed.Load() }

// SetRngSeedBase updates the seed base for future runs.
func (m *Manager) SetRngSeedBase(v int64) { m.rngSeed.Store(v) }

// NewVM builds a fresh script image. Slow (runtime + Lua state setup) —
// callers create the VM BEFORE taking the shard pause.
func (m *Manager) NewVM() (*host.VM, error) { return m.eng.NewVM() }

// --- execution ---------------------------------------------------------------

// RunOnVM executes s on vm with KEYS/ARGV staged, marking the manager's
// running state (the BUSY gate and SCRIPT KILL) for the duration. call is
// the per-run redis.call bridge (nil in pure-script mode: redis.call then
// fails with a clean error). The caller holds the shard pause (S3) and
// must close vm itself. The returned respVer is the run's final reply
// version (redis.setresp may switch it mid-run).
func (m *Manager) RunOnVM(ctx context.Context, vm *host.VM, s *host.Script, keys, argv []string, ro bool, call CallPath) (host.Result, int, error) {
	r := &run{vm: vm, sha: s.SHA1, ro: ro, call: call, start: time.Now(), respVer: 2}
	m.runMu.Lock()
	m.runs[vm] = r
	m.runMu.Unlock()
	defer func() {
		m.runMu.Lock()
		delete(m.runs, vm)
		m.runMu.Unlock()
	}()

	opt := host.RunOptions{Keys: keys, Argv: argv}
	if d := m.hardDeadlineMs.Load(); d > 0 {
		opt.Deadline = time.Duration(d) * time.Millisecond
	}
	// S6: per-run seed — base ⊕ a scrambled run counter.
	seq := m.runSeq.Add(1)
	opt.Seed = m.rngSeed.Load() + int64(seq*0x9E3779B97F4A7C15)

	res, err := vm.Run(ctx, s, opt)
	if err != nil {
		var se *host.ScriptError
		if errors.As(err, &se) && se.Line == 0 && se.ErrValue.Kind == host.KindString &&
			se.ErrValue.Str == "context deadline exceeded" {
			// The deadline class: the watchdog flag fired (a script
			// erroring with this exact text would carry a position
			// prefix, and the C runtime stages no line for the deadline
			// poll — gopher-lua ledger row 37).
			if r.killed.Load() {
				// SCRIPT KILL landed: Redis's text (decision S5 keeps
				// the partial effects either way).
				se.ErrValue = host.String("ERR Script killed by user with SCRIPT KILL...")
			} else {
				// The hard watchdog deadline (S5 divergence: Redis
				// cannot preempt, so there is no parity text).
				se.ErrValue = host.String("ERR Script killed by the hard execution deadline (script_hard_deadline_ms)")
			}
			se.Line = 1
			return res, r.respVer, se
		}
	}
	return res, r.respVer, err
}

// BusyTimeout reports whether a script is running past the soft limit —
// the BUSY gate in Engine.Execute (Redis's lua_timedout).
func (m *Manager) BusyTimeout() bool {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	for _, r := range m.runs {
		if time.Since(r.start) > time.Duration(m.timeLimitMs.Load())*time.Millisecond {
			return true
		}
	}
	return false
}

// KillOutcome is SCRIPT KILL's three-way result.
// The three outcomes map to Redis's NOTBUSY / UNKILLABLE / +OK replies.
type KillOutcome int

const (
	// KillNotBusy means no script is running (NOTBUSY).
	KillNotBusy KillOutcome = iota
	// KillUnkillable means the running script already wrote (UNKILLABLE).
	KillUnkillable
	// KillOK means the kill was requested (+OK).
	KillOK
)

// Kill requests termination of the running script (SCRIPT KILL): the
// guest's deadline flag is tripped and the run unwinds at the next loop
// back-edge. A script that already wrote is unkillable in Redis terms
// (S5's hard deadline still bounds it).
func (m *Manager) Kill() KillOutcome {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	for _, r := range m.runs {
		if r.wrote.Load() {
			return KillUnkillable
		}
		r.killed.Store(true)
		r.vm.Kill()
		return KillOK
	}
	return KillNotBusy
}

// findRun returns the run context of a VM (bridge HostFuncs).
func (m *Manager) findRun(vm *host.VM) *run {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	return m.runs[vm]
}
