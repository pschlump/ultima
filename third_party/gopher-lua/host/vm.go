package host

// VM: one Lua memory image — a private *lua.LState (sandboxed, with the
// engine's host functions registered) plus the mutex that makes it
// thread-proof (the fork's design A9). All guest state lives in the
// LState. This is the pure-Go port of the fork's wasm vm.go: no wazero
// runtime, no linear memory — the interpreter IS the runtime.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// RunOptions is one execution of a script against a VM image.
type RunOptions struct {
	// Keys and Argv are staged as the KEYS / ARGV globals (fresh tables
	// per run — a reused VM never leaks staging across runs).
	Keys, Argv []string
	// Deadline bounds the run: the run context is canceled at the
	// deadline and the interpreter's per-instruction context check raises
	// an ordinary pcall-catchable error ("context deadline exceeded" —
	// the fork's M6d D4). Zero disables the watchdog.
	Deadline time.Duration
	// Seed reseals the VM's math.random stream for this run (determinism
	// D5). Zero keeps the VM's existing stream (fresh VMs start at 42 —
	// the harness contract — so set it for reproducible runs).
	Seed int64
}

// Result is a script's return values, converted.
type Result struct {
	Values []Value
}

// ScriptError is a Lua-level error (raised by the script, a library, a
// host function, or the deadline/OOM machinery). ErrValue is the exact
// error value; for string errors Redis semantics pass the text through
// verbatim. Line is the raise line inside the script (0 = unknown) — the
// position Redis reports as "on @user_script:N" in error replies.
type ScriptError struct {
	ErrValue Value
	Line     int
}

func (e *ScriptError) Error() string { return e.ErrValue.String() }

// TrapError is an engine-level failure (a Go panic inside the
// interpreter — by contract always a backend bug, never a script error).
type TrapError struct{ Err error }

func (e *TrapError) Error() string { return "host: trap (backend bug class): " + e.Err.Error() }
func (e *TrapError) Unwrap() error { return e.Err }

// ErrReentrant is returned when a HostFunc tries to re-enter its own VM.
var ErrReentrant = errors.New("host: VM.Run called from inside a host function (the image lock is not reentrant)")

// ErrScriptBound is the one-script law: a VM image executes scripts
// compiled from ONE source. Use a fresh VM (Engine.Run) per distinct
// script.
var ErrScriptBound = errors.New("host: VM is bound to a different script (v1: one script per VM image)")

// VM is one locked Lua memory image.
type VM struct {
	mu     sync.Mutex
	e      *Engine
	closed bool

	L        *lua.LState
	rng      *rand.Rand
	hostFns  []registeredFn  // snapshot of the engine registry
	hostVals []registeredVal // snapshot of the engine value registry

	script *Script // bound at first Run (one-script law)

	// inRun is the same-goroutine reentrancy guard: checked BEFORE the
	// mutex (a HostFunc re-entering its VM would otherwise deadlock on
	// the lock it already holds).
	inRun atomic.Bool

	// killMu guards killCancel: Kill() runs off the Run goroutine (the
	// daemon's SCRIPT KILL) and must not take vm.mu (Run holds it for the
	// whole run). killCancel cancels the in-flight run's context; nil
	// between runs (Kill is then a no-op).
	killMu     sync.Mutex
	killCancel context.CancelFunc
}

// NewVM builds a fresh image: Lua state + stdlib, sandboxed, with the
// engine's host functions registered and (optionally) the globals
// lockdown and memory budget installed.
func (e *Engine) NewVM() (*VM, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, errors.New("host: engine closed")
	}
	vm := &VM{e: e, rng: rand.New(rand.NewSource(42)), hostFns: e.hostFns, hostVals: e.hostVals}
	e.vms++
	e.mu.Unlock()

	L := lua.NewState()
	vm.L = L

	// memory budget first so the whole VM lifetime counts
	if e.opts.maxMem > 0 {
		L.SetMemoryLimit(e.opts.maxMem)
	}

	// the sandbox (the fork's rt_sandbox): nil out the unsafe globals;
	// channel is gopher-lua-only (the wasm runtime never had it); print
	// becomes a no-op (the wasm blob routed it to an event sink the
	// daemon never registers — observable behavior is silence). Keep
	// coroutine.
	for _, name := range []string{
		"io", "os", "package", "require", "module",
		"dofile", "loadfile", "load", "loadstring", "debug", "channel",
	} {
		L.SetGlobal(name, lua.LNil)
	}
	L.SetGlobal("print", L.NewFunction(func(L *lua.LState) int { return 0 }))

	// deterministic RNG (the fork's host random01/randomint/randomseed
	// exports, backed by Go math/rand): each VM owns its stream.
	if mt, ok := L.GetGlobal("math").(*lua.LTable); ok {
		mt.RawSetString("random", L.NewFunction(vm.luaRandom))
		mt.RawSetString("randomseed", L.NewFunction(vm.luaRandomseed))
	}

	// host functions + value constants (redis.call, redis.LOG_WARNING, ...)
	vm.stageHost()

	// globals lockdown (Redis-classic): after ALL host staging, before
	// any script runs — Run stages KEYS/ARGV inside a readonly-off window
	if e.opts.globalsProtection {
		vm.protectGlobals()
	}
	return vm, nil
}

// stageHost installs the registered host functions and value constants.
func (vm *VM) stageHost() {
	L := vm.L
	g := L.G.Global
	tableFor := func(name string) *lua.LTable {
		if t, ok := g.RawGetString(name).(*lua.LTable); ok {
			return t
		}
		t := L.NewTable()
		g.RawSetString(name, t)
		return t
	}
	for _, hf := range vm.hostFns {
		hf := hf
		tableFor(hf.table).RawSetString(hf.name, L.NewFunction(vm.makeTrampoline(hf)))
	}
	for _, rv := range vm.hostVals {
		lv, err := valueToLVal(L, rv.v)
		if err != nil { // unreachable: RegisterValue validates the kinds
			continue
		}
		tableFor(rv.table).RawSetString(rv.name, lv)
	}
}

// makeTrampoline builds the Lua→Go seam for one host function (the fork's
// hostCall): decode the arguments, run the HostFunc synchronously on the
// script's goroutine, push the results — or raise the error value. A
// panicking HostFunc is contained: the script sees a clean Lua error.
func (vm *VM) makeTrampoline(hf registeredFn) lua.LGFunction {
	return func(L *lua.LState) int {
		nargs := L.GetTop()
		args := make([]Value, 0, nargs)
		for i := 1; i <= nargs; i++ {
			args = append(args, toValue(L.Get(i)))
		}
		var vals []Value
		var err error
		func() {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("host function panic: %v", p)
					vals = nil
				}
			}()
			vals, err = hf.fn(vm, args)
		}()
		if err != nil {
			if ve, ok := asValueError(err); ok {
				// Level-0 semantics: the value is raised EXACTLY (no
				// position prefix), so the script's pcall sees it and the
				// daemon renders the text verbatim.
				if lv, cerr := valueToLVal(L, ve.V); cerr == nil {
					L.Error(lv, 0)
				}
			}
			L.Error(lua.LString(err.Error()), 0)
		}
		for _, v := range vals {
			lv, cerr := valueToLVal(L, v)
			if cerr != nil {
				L.Error(lua.LString(cerr.Error()), 0)
			}
			L.Push(lv)
		}
		return len(vals)
	}
}

// luaRandom is math.random backed by the VM's deterministic stream.
func (vm *VM) luaRandom(L *lua.LState) int {
	switch L.GetTop() {
	case 0:
		L.Push(lua.LNumber(vm.rng.Float64()))
	case 1:
		n := L.CheckInt(1)
		if n <= 0 {
			L.ArgError(1, "interval is empty")
		}
		L.Push(lua.LNumber(vm.rng.Intn(n) + 1))
	default:
		m := L.CheckInt(1)
		n := L.CheckInt(2)
		if n < m {
			L.ArgError(2, "interval is empty")
		}
		L.Push(lua.LNumber(vm.rng.Intn(n-m+1) + m))
	}
	return 1
}

// luaRandomseed is math.randomseed backed by the VM's stream.
func (vm *VM) luaRandomseed(L *lua.LState) int {
	vm.rng = rand.New(rand.NewSource(L.CheckInt64(1)))
	return 0
}

// globalsIndexGuard is the __index of the globals table under the
// lockdown (Redis's luaProtectedTableError): reading an absent global is
// a script error naming the key. RaiseError attributes the triggering
// script line (the raise skips this Go frame and lands on the Lua frame
// that performed the read).
func globalsIndexGuard(L *lua.LState) int {
	key := L.Get(2)
	switch k := key.(type) {
	case lua.LString:
		L.RaiseError("Script attempted to access nonexistent global variable '%s'", string(k))
	case lua.LNumber:
		L.RaiseError("Script attempted to access nonexistent global variable '%s'", k.String())
	default:
		L.RaiseError("Second argument to luaProtectedTableError must be a string or number")
	}
	return 0
}

// protectGlobals installs the guard metatable on the globals table and
// recursively marks it and every table reachable from it (metatables
// included) readonly — Redis's enableReadOnlyTables. KEYS/ARGV do not
// exist yet (they stage per run afterwards) and are never marked.
func (vm *VM) protectGlobals() {
	L := vm.L
	g := L.G.Global
	mt := L.NewTable()
	mt.RawSetString("__index", L.NewFunction(globalsIndexGuard))
	g.Metatable = mt
	markReadonly(g, map[*lua.LTable]bool{})
}

// markReadonly recursively freezes tb and every table reachable from it
// (cycle-guarded — _G._G is the obvious loop).
func markReadonly(tb *lua.LTable, seen map[*lua.LTable]bool) {
	if seen[tb] {
		return
	}
	seen[tb] = true
	tb.SetReadonly(true)
	tb.ForEach(func(k, v lua.LValue) {
		if sub, ok := k.(*lua.LTable); ok {
			markReadonly(sub, seen)
		}
		if sub, ok := v.(*lua.LTable); ok {
			markReadonly(sub, seen)
		}
	})
	if mt, ok := tb.Metatable.(*lua.LTable); ok {
		markReadonly(mt, seen)
	}
}

// Run executes s against the image, holding vm.mu end-to-end (A9): one
// execution at a time per image, any number of images in parallel, safe
// from any goroutine.
func (vm *VM) Run(ctx context.Context, s *Script, opt RunOptions) (res Result, err error) {
	if s == nil {
		return Result{}, errors.New("host: nil script")
	}
	if vm.inRun.Load() {
		return Result{}, ErrReentrant
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.closed {
		return Result{}, errors.New("host: VM closed")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	// one-script law: bind at first Run
	if vm.script == nil {
		vm.script = s
	} else if vm.script != s {
		return Result{}, ErrScriptBound
	}

	L := vm.L

	// stage KEYS/ARGV as FRESH tables per run (no cross-run leakage),
	// inside the readonly-off window when the lockdown is on.
	if vm.e.opts.globalsProtection {
		L.SetReadonlyBypass(true)
	}
	kt := L.NewTable()
	for i, k := range opt.Keys {
		kt.RawSetInt(i+1, lua.LString(k))
	}
	at := L.NewTable()
	for i, a := range opt.Argv {
		at.RawSetInt(i+1, lua.LString(a))
	}
	L.SetGlobal("KEYS", kt)
	L.SetGlobal("ARGV", at)
	if vm.e.opts.globalsProtection {
		L.SetReadonlyBypass(false)
	}

	if opt.Seed != 0 {
		vm.rng = rand.New(rand.NewSource(opt.Seed))
	}

	// The deadline watchdog + Kill share one mechanism: a per-run
	// cancelable context. The interpreter's main loop checks it per
	// instruction and raises ctx.Err() as an ordinary (pcall-catchable)
	// Lua error — self-re-raising, since the check never clears.
	runCtx := ctx
	var cancel context.CancelFunc
	if opt.Deadline > 0 {
		runCtx, cancel = context.WithTimeout(ctx, opt.Deadline)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	vm.killMu.Lock()
	vm.killCancel = cancel
	vm.killMu.Unlock()
	defer func() {
		cancel() // retire the run context (also a late Kill's target)
		vm.killMu.Lock()
		vm.killCancel = nil
		vm.killMu.Unlock()
		L.RemoveContext() // a canceled ctx must never poison VM reuse
	}()
	L.SetContext(runCtx)

	// The error handler's ONLY job is capturing the raise line; it
	// returns the error value unchanged so the object survives intact
	// (non-string errors — error(42), error({err=...}) — carry no
	// position prefix, so the line must come from the stack at raise
	// time: the handler runs before the protected call unwinds it).
	raiseLine := 0
	handler := L.NewFunction(func(L *lua.LState) int {
		raiseLine = innermostLuaLine(L)
		L.Push(L.Get(1))
		return 1
	})

	fn := L.NewFunctionFromProto(s.proto)
	vm.inRun.Store(true)
	top := L.GetTop()
	runErr := L.CallByParam(lua.P{Fn: fn, NRet: lua.MultRet, Protect: true, Handler: handler})
	vm.inRun.Store(false)

	if runErr != nil {
		return Result{}, vm.mapRunError(runErr, runCtx, raiseLine, s)
	}

	n := L.GetTop() - top
	vals := make([]Value, 0, max(n, 0))
	for i := 1; i <= n; i++ {
		vals = append(vals, toValue(L.Get(top+i)))
	}
	L.Pop(n)
	return Result{Values: vals}, nil
}

// mapRunError converts a protected-call failure to the host error
// taxonomy: the deadline/kill normalization, real Go panics as
// TrapError, everything else as ScriptError with the exact error value
// and the raise line.
func (vm *VM) mapRunError(err error, runCtx context.Context, raiseLine int, s *Script) error {
	// The deadline/kill class: the run context fired and the interpreter
	// raised its message. Normalize to the exact contract the daemon's
	// RunOnVM keys on (Line 0, "context deadline exceeded" — also for a
	// Kill-cancelled run; the manager distinguishes via its own flag).
	if cerr := runCtx.Err(); cerr != nil {
		if ae, ok := err.(*lua.ApiError); ok {
			if str, ok := ae.Object.(lua.LString); ok {
				msg := string(str)
				if msg == cerr.Error() || strings.HasSuffix(msg, ": "+cerr.Error()) {
					return &ScriptError{ErrValue: String("context deadline exceeded"), Line: 0}
				}
			}
		}
	}
	var ae *lua.ApiError
	if !errors.As(err, &ae) {
		return &TrapError{Err: err}
	}
	if ae.Type == lua.ApiErrorPanic {
		// a Go panic that is not a Lua error is a real bug
		return &TrapError{Err: errors.New(ae.Object.String())}
	}
	v := toValue(ae.Object)
	line := 0
	if v.Kind == KindString {
		// String errors raised at level ≥1 carry the interpreter's
		// position prefix ("user_script:N: ...") — the raise line is in
		// the text itself.
		if n, ok := parsePositionPrefix(v.Str, s.name); ok {
			line = n
		}
	}
	if line == 0 {
		line = raiseLine
	}
	return &ScriptError{ErrValue: v, Line: line}
}

// parsePositionPrefix extracts the line from a "<chunk>:<N>:" position
// prefix the interpreter added to an error string.
func parsePositionPrefix(msg, chunkName string) (int, bool) {
	prefix := chunkName + ":"
	if !strings.HasPrefix(msg, prefix) {
		return 0, false
	}
	rest := msg[len(prefix):]
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	if i == 0 || i >= len(rest) || rest[i] != ':' {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:i])
	if err != nil {
		return 0, false
	}
	return n, true
}

// innermostLuaLine returns the current line of the innermost Lua frame
// (skipping Go frames) — the raise line, when called from the error
// handler before the protected call unwinds the stack.
func innermostLuaLine(L *lua.LState) int {
	for level := 0; level < 1024; level++ {
		dbg, ok := L.GetStack(level)
		if !ok {
			break
		}
		if _, err := L.GetInfo("Sl", dbg, lua.LNil); err != nil {
			break
		}
		if dbg.What == "G" {
			continue
		}
		if dbg.CurrentLine > 0 {
			return dbg.CurrentLine
		}
	}
	return 0
}

// Kill cancels the in-flight run's context: the interpreter's next
// instruction raises the ordinary (pcall-catchable) deadline error,
// exactly as if the watchdog had fired. No-op between runs. This is the
// SCRIPT KILL mechanism: the daemon's scripting manager calls it on the
// VM a killable script is running on.
func (vm *VM) Kill() {
	vm.killMu.Lock()
	defer vm.killMu.Unlock()
	if vm.killCancel != nil && vm.inRun.Load() {
		vm.killCancel()
	}
}

// UsedBytes reports the image's approximate Lua allocation total: the
// same counter the WithMemoryBudgetBytes budget is enforced against.
// The counter is monotonic (frees are not tracked), so on a reused VM
// the figure grows — pooled-VM deployments use it as the recycling
// watermark. Serialized against Run; 0 on a closed VM.
func (vm *VM) UsedBytes(ctx context.Context) int64 {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.closed {
		return 0
	}
	return vm.L.MemoryUsed()
}

// Close tears the image down. The VM is unusable afterwards.
func (vm *VM) Close() error {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.closed {
		return nil
	}
	vm.closed = true
	if vm.L != nil {
		vm.L.Close()
	}
	return nil
}
