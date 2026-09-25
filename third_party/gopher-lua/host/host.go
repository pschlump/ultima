package host

// Engine: process-wide. Owns the compile cache and the host-function
// registry; VMs own their interpreter states (see vm.go). This is the
// pure-Go port of the fork's wasm-based host package — same API, same
// observable semantics, no wazero.
//
// Safe for concurrent use.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

// HostFunc is a Lua-visible function implemented in Go (the redis.call
// mechanism). It executes on the goroutine that holds the VM's lock — the
// same goroutine running the script — so it may inspect VM state freely,
// but it must NOT call vm.Run (the lock is not reentrant; ErrReentrant is
// returned if it does).
type HostFunc func(vm *VM, args []Value) ([]Value, error)

// Option configures an Engine.
type Option func(*options)

type options struct {
	maxMem            int64 // per-VM allocation budget bytes (0 = unlimited)
	globalsProtection bool  // Redis-classic globals lockdown per VM
}

// WithMemoryBudgetBytes caps each VM's Lua allocation budget; crossing it
// raises a clean pcall-catchable "not enough memory" error inside the
// script — never a host OOM. Accounting is approximate and monotonic
// (frees are not tracked); see LState.accountMem in the vendored core.
func WithMemoryBudgetBytes(n int64) Option {
	return func(o *options) { o.maxMem = n }
}

// WithGlobalsProtection installs the Redis-classic script environment
// lockdown on every VM: reading an undefined global raises "Script
// attempted to access nonexistent global variable '<k>'", and globals
// plus every table reachable from them are readonly — any write
// (assignment, rawset) raises "Attempt to modify a readonly table".
// KEYS/ARGV stage per run inside a readonly-off window, so they stay
// writable. Enable after all RegisterGlobal/RegisterValue calls (VM
// creation order: sandbox → host staging → protection).
func WithGlobalsProtection(on bool) Option {
	return func(o *options) { o.globalsProtection = on }
}

// Engine is the shared, process-wide scripting engine.
type Engine struct {
	opts options

	mu       sync.Mutex
	cache    map[string]*Script
	hostFns  []registeredFn
	hostVals []registeredVal
	vms      int // live VMs (RegisterGlobal freezes once the first exists)
	closed   bool
}

type registeredFn struct {
	table, name string
	fn          HostFunc
}

type registeredVal struct {
	table, name string
	v           Value
}

// luaIdent reports whether s is a plain Lua identifier.
func luaIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && c >= '0' && c <= '9')
		if !ok {
			return false
		}
	}
	return true
}

// RegisterValue installs <table>.<name> as a constant Lua value (nil,
// booleans, numbers, strings) in every VM — the redis.LOG_WARNING /
// redis.REDIS_VERSION mechanism. Same freeze rule as RegisterGlobal.
func (e *Engine) RegisterValue(table, name string, v Value) error {
	if !luaIdent(table) || !luaIdent(name) {
		return errors.New("host: RegisterValue requires Lua-identifier table and name")
	}
	switch v.Kind {
	case KindNil, KindFalse, KindTrue, KindNumber, KindString:
	default:
		return errors.New("host: RegisterValue supports only nil/bool/number/string values")
	}
	if v.Kind == KindNumber && (math.IsInf(v.Num, 0) || math.IsNaN(v.Num)) {
		return errors.New("host: RegisterValue number must be finite")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.vms > 0 {
		return errors.New("host: RegisterValue after NewVM (the registry is frozen once VMs exist)")
	}
	for _, r := range e.hostVals {
		if r.table == table && r.name == name {
			return fmt.Errorf("host: RegisterValue duplicate %s.%s", table, name)
		}
	}
	e.hostVals = append(e.hostVals, registeredVal{table, name, v})
	return nil
}

// NewEngine returns a ready engine.
func NewEngine(opts ...Option) (*Engine, error) {
	o := options{}
	for _, f := range opts {
		f(&o)
	}
	return &Engine{opts: o, cache: map[string]*Script{}}, nil
}

// RegisterGlobal installs <table>.<name> as a Lua function backed by fn
// (e.g. RegisterGlobal("redis", "call", ...)). Must be called before the
// first NewVM — each VM snapshots the registry at creation.
func (e *Engine) RegisterGlobal(table, name string, fn HostFunc) error {
	if table == "" || name == "" {
		return errors.New("host: RegisterGlobal requires non-empty table and name")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.vms > 0 {
		return errors.New("host: RegisterGlobal after NewVM (the registry is frozen once VMs exist)")
	}
	for _, r := range e.hostFns {
		if r.table == table && r.name == name {
			return fmt.Errorf("host: RegisterGlobal duplicate %s.%s", table, name)
		}
	}
	e.hostFns = append(e.hostFns, registeredFn{table, name, fn})
	return nil
}

// Script is one compiled script: the parsed+compiled chunk plus its Redis
// identity. Deterministic for a given (source, name) — cacheable by SHA.
type Script struct {
	// SHA1 is the hex SHA-1 of the source bytes — the SCRIPT LOAD /
	// EVALSHA identity (Redis model).
	SHA1 string

	name  string
	proto *lua.FunctionProto
}

// Name returns the chunk name the script was compiled under (error
// position prefixes use it).
func (s *Script) Name() string { return s.name }

// Compile parses and compiles source, caching by (SHA-1, name). Compile
// errors return the frontend's message verbatim.
func (e *Engine) Compile(source []byte, name string) (*Script, error) {
	if name == "" {
		name = "=script"
	}
	sum := sha1.Sum(source)
	sha := hex.EncodeToString(sum[:])
	key := sha + "\x00" + name
	e.mu.Lock()
	if s, ok := e.cache[key]; ok {
		e.mu.Unlock()
		return s, nil
	}
	e.mu.Unlock()

	chunk, err := parse.Parse(strings.NewReader(string(source)), name)
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, name)
	if err != nil {
		return nil, err
	}
	s := &Script{SHA1: sha, name: name, proto: proto}
	e.mu.Lock()
	// A concurrent Compile of the same source may have won the race:
	// pointer identity per (SHA-1, name) is the invariant the VM
	// one-script law (ErrScriptBound) and consumer-side per-script pools
	// rely on — never hand out two pointers for one source.
	if prev, ok := e.cache[key]; ok {
		e.mu.Unlock()
		return prev, nil
	}
	e.cache[key] = s
	e.mu.Unlock()
	return s, nil
}

// Run compiles nothing: it executes s against a fresh VM.
func (e *Engine) Run(ctx context.Context, s *Script, opt RunOptions) (Result, error) {
	vm, err := e.NewVM()
	if err != nil {
		return Result{}, err
	}
	defer vm.Close()
	return vm.Run(ctx, s, opt)
}

// Close marks the engine closed; live VMs keep working but no new ones
// may be made.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return nil
}
