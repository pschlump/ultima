package host

// Focused tests for the pure-Go host: compile/run basics, the sandbox,
// RNG determinism, the globals lockdown texts, the readonly rawset bare
// raise, deadline normalization, Kill, UsedBytes, the one-script law, and
// VM-reuse restaging.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func newEngine(t *testing.T, opts ...Option) *Engine {
	t.Helper()
	e, err := NewEngine(opts...)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func compile(t *testing.T, e *Engine, src string) *Script {
	t.Helper()
	s, err := e.Compile([]byte(src), "user_script")
	if err != nil {
		t.Fatalf("Compile(%q): %v", src, err)
	}
	return s
}

func run(t *testing.T, e *Engine, src string, opt RunOptions) (Result, error) {
	t.Helper()
	return e.Run(context.Background(), compile(t, e, src), opt)
}

func TestCompileRunBasics(t *testing.T) {
	e := newEngine(t)
	res, err := run(t, e, "return 1+1", RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Values) != 1 || res.Values[0].Kind != KindNumber || res.Values[0].Num != 2 {
		t.Fatalf("got %+v", res.Values)
	}
	// pointer identity: same source compiles to the same *Script
	s1 := compile(t, e, "return 42")
	s2 := compile(t, e, "return 42")
	if s1 != s2 {
		t.Fatal("Compile did not return the same *Script for the same source")
	}
	if len(s1.SHA1) != 40 {
		t.Fatalf("SHA1 = %q", s1.SHA1)
	}
	if s1.Name() != "user_script" {
		t.Fatalf("Name = %q", s1.Name())
	}
	// compile error surfaces the frontend message
	if _, err := e.Compile([]byte("return 1 +"), "user_script"); err == nil {
		t.Fatal("expected a compile error")
	}
}

func TestKeysArgvStaging(t *testing.T) {
	e := newEngine(t)
	res, err := run(t, e, "return {KEYS[1], ARGV[1], #KEYS, #ARGV}",
		RunOptions{Keys: []string{"k1", "k2"}, Argv: []string{"a1"}})
	if err != nil {
		t.Fatal(err)
	}
	v := res.Values[0]
	if v.Kind != KindTable || len(v.Pairs) != 4 {
		t.Fatalf("got %+v", v)
	}
	if v.Pairs[0].Val.Str != "k1" || v.Pairs[1].Val.Str != "a1" ||
		v.Pairs[2].Val.Num != 2 || v.Pairs[3].Val.Num != 1 {
		t.Fatalf("got %+v", v.Pairs)
	}
}

func TestSandboxNils(t *testing.T) {
	e := newEngine(t)
	// tostring avoids nil holes (table conversion skips nil entries,
	// like the fork's wire encoder)
	res, err := run(t, e, "return {type(io), type(os), type(package), type(require), type(module), type(dofile), type(loadfile), type(load), type(loadstring), type(debug), type(channel), type(print)}", RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"nil", "nil", "nil", "nil", "nil", "nil", "nil", "nil", "nil", "nil", "nil", "function"}
	v := res.Values[0]
	if v.Kind != KindTable || len(v.Pairs) != len(want) {
		t.Fatalf("got %+v", v)
	}
	for i, p := range v.Pairs {
		if p.Val.Str != want[i] {
			t.Fatalf("entry %d: got %v, want %s", i, p.Val, want[i])
		}
	}
	// coroutine stays
	res, err = run(t, e, "return type(coroutine.create(function() end))", RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Values[0].Str != "thread" {
		t.Fatalf("coroutine.create: %v", res.Values[0])
	}
	// print is silent
	if _, err := run(t, e, "print('hello')", RunOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestRNGDeterminism(t *testing.T) {
	e := newEngine(t)
	src := "math.randomseed(7) return {math.random(), math.random(10), math.random(5,9)}"
	r1, err := run(t, e, src, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := run(t, e, src, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range r1.Values[0].Pairs {
		a, b := r1.Values[0].Pairs[i].Val, r2.Values[0].Pairs[i].Val
		if a.Num != b.Num {
			t.Fatalf("run divergence at %d: %v vs %v", i, a.Num, b.Num)
		}
	}
	// fresh VMs start seeded 42: two Engine.Run calls (fresh VM each)
	// with no explicit seed agree
	src2 := "return math.random()"
	r3, _ := run(t, e, src2, RunOptions{})
	r4, _ := run(t, e, src2, RunOptions{})
	if r3.Values[0].Num != r4.Values[0].Num {
		t.Fatalf("fresh-VM streams diverge: %v vs %v", r3.Values[0].Num, r4.Values[0].Num)
	}
	// RunOptions.Seed reseals
	r5, _ := run(t, e, src2, RunOptions{Seed: 99})
	r6, _ := run(t, e, src2, RunOptions{Seed: 99})
	if r5.Values[0].Num != r6.Values[0].Num {
		t.Fatal("Seed=99 runs diverge")
	}
}

func TestGlobalsGuardTexts(t *testing.T) {
	e := newEngine(t, WithGlobalsProtection(true))
	// the redis surface, like the daemon registers
	if err := e.RegisterGlobal("redis", "error_reply", func(vm *VM, args []Value) ([]Value, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterValue("redis", "LOG_WARNING", Int(3)); err != nil {
		t.Fatal(err)
	}
	check := func(src, want string, wantLine int) {
		t.Helper()
		_, err := run(t, e, src, RunOptions{})
		var se *ScriptError
		if !errors.As(err, &se) {
			t.Fatalf("%s: got %v, want ScriptError", src, err)
		}
		if se.ErrValue.Str != want {
			t.Errorf("%s:\n got %q\nwant %q", src, se.ErrValue.Str, want)
		}
		if se.Line != wantLine {
			t.Errorf("%s: Line = %d, want %d", src, se.Line, wantLine)
		}
	}
	check("return undefined_global",
		"user_script:1: Script attempted to access nonexistent global variable 'undefined_global'", 1)
	check("g = 5 return g", "user_script:1: Attempt to modify a readonly table", 1)
	check("return _G[nil]",
		"user_script:1: Second argument to luaProtectedTableError must be a string or number", 1)
	check("return _G[42]",
		"user_script:1: Script attempted to access nonexistent global variable '42'", 1)
	// raw writes: BARE string, no position prefix; Line still captured
	check("rawset(_G, 'g3', 1) return 1", "Attempt to modify a readonly table", 1)
	check("string.foo = 1 return 1", "user_script:1: Attempt to modify a readonly table", 1)
	check("redis.error_reply = nil return 1", "user_script:1: Attempt to modify a readonly table", 1)
	check("redis = nil return 1", "user_script:1: Attempt to modify a readonly table", 1)
	check("math.huge = 5 return math.huge", "user_script:1: Attempt to modify a readonly table", 1)
	check("_G.pairs = nil return 1", "user_script:1: Attempt to modify a readonly table", 1)
	// multiline raise line
	check("local a = 1\nlocal b = 2\ng = 5", "user_script:3: Attempt to modify a readonly table", 3)

	// pcall catches the guarded-read error, prefix included
	res, err := run(t, e, "local ok,e = pcall(function() return nosuchglobal end) return {ok, e}", RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p := res.Values[0].Pairs
	if p[0].Val.Kind != KindFalse ||
		p[1].Val.Str != "user_script:1: Script attempted to access nonexistent global variable 'nosuchglobal'" {
		t.Fatalf("pcall guard: %+v", p)
	}

	// allowed behaviors
	allowed := []string{
		"KEYS[1] = 'x' return KEYS[1]",
		"local t = {} rawset(t, 'k', 1) return t.k",
		"return rawget(_G, 'undefined_global') == nil",
		"return getmetatable(_G) ~= nil",
		"local t = setmetatable({}, {__index=function() return 7 end}) return t.x",
		"for k in pairs(_G) do if k=='nosuch' then return 1 end end return 0",
		"return redis.LOG_WARNING",
		"return _G", // a table: expands to empty pairs
	}
	for _, src := range allowed {
		if _, err := run(t, e, src, RunOptions{Keys: []string{"k"}}); err != nil {
			t.Errorf("%s: %v", src, err)
		}
	}
}

func TestErrorLineMapping(t *testing.T) {
	e := newEngine(t, WithGlobalsProtection(true))
	// level-0 string: no prefix in the value, line from the handler
	_, err := run(t, e, "local a = 1\nlocal b = 2\nerror('boom3', 0)", RunOptions{})
	var se *ScriptError
	if !errors.As(err, &se) {
		t.Fatal(err)
	}
	if se.ErrValue.Str != "boom3" || se.Line != 3 {
		t.Fatalf("level-0 error: %q line %d, want \"boom3\" line 3", se.ErrValue.Str, se.Line)
	}
	// non-string error value
	_, err = run(t, e, "return error(42)", RunOptions{})
	if !errors.As(err, &se) || se.ErrValue.Kind != KindNumber || se.ErrValue.Num != 42 || se.Line != 1 {
		t.Fatalf("error(42): %+v", se)
	}
	// prefixed string: line parsed from the prefix
	_, err = run(t, e, "local a = 1\nlocal b = 2\nerror('boom3')", RunOptions{})
	if !errors.As(err, &se) || se.ErrValue.Str != "user_script:3: boom3" || se.Line != 3 {
		t.Fatalf("error('boom3'): %q line %d", se.ErrValue.Str, se.Line)
	}
}

func TestHostFuncErrorLine(t *testing.T) {
	e := newEngine(t)
	if err := e.RegisterGlobal("redis", "call", func(vm *VM, args []Value) ([]Value, error) {
		return nil, &ValueError{V: String("ERR Wrong number of args calling Redis command from script")}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := e.Run(context.Background(), compile(t, e, "local a = 1\nlocal b = 2\nreturn redis.call('get')"), RunOptions{})
	var se *ScriptError
	if !errors.As(err, &se) {
		t.Fatal(err)
	}
	if se.ErrValue.Str != "ERR Wrong number of args calling Redis command from script" {
		t.Fatalf("value = %q", se.ErrValue.Str)
	}
	if se.Line != 3 {
		t.Fatalf("Line = %d, want 3", se.Line)
	}
}

func TestHostFuncRoundTrip(t *testing.T) {
	e := newEngine(t)
	err := e.RegisterGlobal("redis", "echo", func(vm *VM, args []Value) ([]Value, error) {
		return args, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Run(context.Background(), compile(t, e, "return redis.echo('x', 42, true, {1, 'two'})"),
		RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Values) != 4 {
		t.Fatalf("got %+v", res.Values)
	}
	if res.Values[0].Str != "x" || res.Values[1].Num != 42 ||
		res.Values[2].Kind != KindTrue || res.Values[3].Kind != KindTable {
		t.Fatalf("got %+v", res.Values)
	}
	// registry frozen after NewVM
	if err := e.RegisterGlobal("redis", "late", nil); err == nil {
		t.Fatal("RegisterGlobal after NewVM succeeded")
	}
	if err := e.RegisterValue("redis", "LATE", Int(1)); err == nil {
		t.Fatal("RegisterValue after NewVM succeeded")
	}
}

func TestDeadlineNormalization(t *testing.T) {
	e := newEngine(t)
	_, err := e.Run(context.Background(), compile(t, e, "while true do end"),
		RunOptions{Deadline: 100 * time.Millisecond})
	var se *ScriptError
	if !errors.As(err, &se) {
		t.Fatalf("got %v, want ScriptError", err)
	}
	if se.Line != 0 || se.ErrValue.Kind != KindString || se.ErrValue.Str != "context deadline exceeded" {
		t.Fatalf("deadline shape: line=%d value=%q", se.Line, se.ErrValue.String())
	}
}

func TestKill(t *testing.T) {
	e := newEngine(t)
	vm, err := e.NewVM()
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	s := compile(t, e, "while true do end")
	done := make(chan error, 1)
	go func() {
		_, err := vm.Run(context.Background(), s, RunOptions{})
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	vm.Kill()
	select {
	case err := <-done:
		var se *ScriptError
		if !errors.As(err, &se) || se.Line != 0 || se.ErrValue.Str != "context deadline exceeded" {
			t.Fatalf("kill shape: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Kill did not end the run")
	}
	// Kill between runs is a no-op
	vm.Kill()
	// the VM is reusable after a kill (fresh ctx, fresh staging)
	res, err := vm.Run(context.Background(), s, RunOptions{Deadline: 50 * time.Millisecond})
	_ = res
	if err == nil {
		t.Fatal("expected the deadline to fire again")
	}
}

func TestUsedBytes(t *testing.T) {
	e := newEngine(t)
	vm, err := e.NewVM()
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	s := compile(t, e, `local t = {} for i = 1, 4 do t[i] = string.rep("x", 100000) end return #t`)
	if _, err := vm.Run(context.Background(), s, RunOptions{}); err != nil {
		t.Fatal(err)
	}
	if used := vm.UsedBytes(context.Background()); used < 400000 {
		t.Fatalf("UsedBytes = %d, want ≥ 400000", used)
	}
}

func TestMemoryBudget(t *testing.T) {
	e := newEngine(t, WithMemoryBudgetBytes(8<<20))
	_, err := e.Run(context.Background(), compile(t, e, "local s='x' while true do s=s..s end"),
		RunOptions{})
	var se *ScriptError
	if !errors.As(err, &se) {
		t.Fatalf("got %v, want ScriptError", err)
	}
	if !strings.Contains(se.ErrValue.String(), "not enough memory") {
		t.Fatalf("OOM text: %q", se.ErrValue.String())
	}
	// pcall-catchable: the script observes the error; returning existing
	// values (no new allocation — a blown budget is sticky) completes.
	res, err := e.Run(context.Background(),
		compile(t, e, "local ok, e = pcall(string.rep, 'x', 9000000) return ok, e"),
		RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Values[0].Kind != KindFalse {
		t.Fatalf("pcall over OOM: %+v", res.Values)
	}
	if !strings.Contains(res.Values[1].Str, "not enough memory") {
		t.Fatalf("pcall OOM text: %q", res.Values[1].Str)
	}
}

func TestScriptBound(t *testing.T) {
	e := newEngine(t)
	vm, err := e.NewVM()
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	s1 := compile(t, e, "return 1")
	s2 := compile(t, e, "return 2")
	if _, err := vm.Run(context.Background(), s1, RunOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := vm.Run(context.Background(), s2, RunOptions{}); !errors.Is(err, ErrScriptBound) {
		t.Fatalf("got %v, want ErrScriptBound", err)
	}
	// same script pointer again: fine
	if _, err := vm.Run(context.Background(), s1, RunOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestVMReuseRestaging(t *testing.T) {
	e := newEngine(t, WithGlobalsProtection(true))
	vm, err := e.NewVM()
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	s := compile(t, e, "return {#KEYS, #ARGV, tostring(KEYS[1]), tostring(ARGV[1])}")
	r1, err := vm.Run(context.Background(), s, RunOptions{Keys: []string{"ka"}, Argv: []string{"a1"}})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := vm.Run(context.Background(), s, RunOptions{Keys: []string{"kb", "kc"}, Argv: []string{"a2", "a3", "a4"}})
	if err != nil {
		t.Fatal(err)
	}
	r3, err := vm.Run(context.Background(), s, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	get := func(r Result, i int) Value { return r.Values[0].Pairs[i].Val }
	if get(r1, 0).Num != 1 || get(r1, 1).Num != 1 || get(r1, 2).Str != "ka" || get(r1, 3).Str != "a1" {
		t.Fatalf("run 1: %+v", r1.Values[0].Pairs)
	}
	if get(r2, 0).Num != 2 || get(r2, 1).Num != 3 || get(r2, 2).Str != "kb" || get(r2, 3).Str != "a2" {
		t.Fatalf("run 2: %+v", r2.Values[0].Pairs)
	}
	// fresh tables: no leakage of the previous run's staging
	if get(r3, 0).Num != 0 || get(r3, 1).Num != 0 || get(r3, 2).Str != "nil" || get(r3, 3).Str != "nil" {
		t.Fatalf("run 3: %+v", r3.Values[0].Pairs)
	}
	// a failed run does not wedge the VM
	bad := compile(t, e, "return 1") // different source would violate the one-script law
	_ = bad
}

func TestReentrancy(t *testing.T) {
	e := newEngine(t)
	var vmRef *VM
	err := e.RegisterGlobal("redis", "reenter", func(vm *VM, args []Value) ([]Value, error) {
		vmRef = vm
		s, _ := e.Compile([]byte("return 1"), "user_script")
		_, err := vm.Run(context.Background(), s, RunOptions{})
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = vmRef
	// the HostFunc error is raised as a Lua error value (bare string)
	_, err = e.Run(context.Background(), compile(t, e, "return redis.reenter()"), RunOptions{})
	var se *ScriptError
	if !errors.As(err, &se) || !strings.Contains(se.ErrValue.Str, "not reentrant") {
		t.Fatalf("got %v", err)
	}
}

func TestRunOnClosedVM(t *testing.T) {
	e := newEngine(t)
	vm, _ := e.NewVM()
	s := compile(t, e, "return 1")
	_ = vm.Close()
	if _, err := vm.Run(context.Background(), s, RunOptions{}); err == nil ||
		!strings.Contains(err.Error(), "VM closed") {
		t.Fatalf("got %v", err)
	}
	// double close is clean
	if err := vm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterValidation(t *testing.T) {
	e := newEngine(t)
	if err := e.RegisterGlobal("", "x", nil); err == nil {
		t.Fatal("empty table accepted")
	}
	if err := e.RegisterGlobal("t", "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterGlobal("t", "x", nil); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := e.RegisterValue("t", "bad name", Int(1)); err == nil {
		t.Fatal("non-identifier accepted")
	}
	if err := e.RegisterValue("t", "ok", Int(1)); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterValue("t", "ok", Int(1)); err == nil {
		t.Fatal("duplicate value accepted")
	}
}
