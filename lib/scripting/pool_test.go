package scripting

// VM pool unit tests (M8e R3): reuse, the recycling triggers (run count,
// heap watermark, fatal run errors, stale epoch after SCRIPT FLUSH), the
// global LRU cap, and the pool-disabled fallback. Behavior parity of
// replies is the differential suite's job (tests/differential/scripts_m8.go).

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/yuin/gopher-lua/host"
)

func newPoolManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	if cfg.MaxMemoryMB == 0 {
		cfg.MaxMemoryMB = 64
	}
	if cfg.HardDeadlineMs == 0 {
		cfg.HardDeadlineMs = 2000
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func poolCfg() Config {
	return Config{VMPoolSize: 1, VMPoolMax: 8, VMRecycleRuns: 0, VMRecyclePct: 0}
}

// runOnce checks out a VM for s, runs it, and releases it — the evalImpl
// lifecycle minus the pause and the redis.call bridge.
func runOnce(t *testing.T, m *Manager, s *host.Script) (vm *host.VM, err error) {
	t.Helper()
	vm, err = m.CheckoutVM(s)
	if err != nil {
		t.Fatalf("CheckoutVM: %v", err)
	}
	_, _, rerr := m.RunOnVM(context.Background(), vm, s, nil, nil, false, nil)
	m.ReleaseVM(vm, s.SHA1, rerr != nil && IsVMFatal(rerr))
	return vm, rerr
}

func TestPoolReuseSameSHA(t *testing.T) {
	m := newPoolManager(t, poolCfg())
	_, s, err := m.Compile([]byte("return 42"))
	if err != nil {
		t.Fatal(err)
	}

	vm1, err := runOnce(t, m, s)
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	vm2, err := runOnce(t, m, s)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if vm1 != vm2 {
		t.Fatalf("second run got a different VM — pool not reusing")
	}
	idle, hits, misses, _, _ := m.PoolStats()
	if idle != 1 || hits != 1 || misses != 1 {
		t.Fatalf("stats idle=%d hits=%d misses=%d, want 1/1/1", idle, hits, misses)
	}
}

func TestPoolRecycleRunCount(t *testing.T) {
	cfg := poolCfg()
	cfg.VMRecycleRuns = 3
	m := newPoolManager(t, cfg)
	_, s, err := m.Compile([]byte("return 42"))
	if err != nil {
		t.Fatal(err)
	}

	var prev *host.VM
	for i := 1; i <= 3; i++ {
		vm, err := runOnce(t, m, s)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if i > 1 && vm != prev {
			t.Fatalf("run %d got a fresh VM before the recycle limit", i)
		}
		prev = vm
	}
	// The 3rd release tripped the run-count recycle: the pool is empty.
	vm4, err := runOnce(t, m, s)
	if err != nil {
		t.Fatalf("run 4: %v", err)
	}
	if vm4 == prev {
		t.Fatalf("run 4 reused the recycled VM")
	}
	_, _, _, recycles, _ := m.PoolStats()
	if recycles != 1 {
		t.Fatalf("recycles=%d, want 1", recycles)
	}
}

func TestPoolRecycleHeapWatermark(t *testing.T) {
	cfg := poolCfg()
	cfg.VMRecyclePct = 5 // 5% of 64 MiB ≈ 3.2 MB
	m := newPoolManager(t, cfg)
	// ~8 MB of garbage per run: over the watermark in one run.
	_, s, err := m.Compile([]byte(`local t = {} for i = 1, 8 do t[i] = string.rep("x", 1000000) end return #t`))
	if err != nil {
		t.Fatal(err)
	}

	vm1, err := runOnce(t, m, s)
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	vm2, err := runOnce(t, m, s)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if vm1 == vm2 {
		t.Fatalf("VM past the heap watermark was returned to the pool")
	}
	// Both runs allocated past the watermark, so both releases recycled.
	_, _, _, recycles, _ := m.PoolStats()
	if recycles != 2 {
		t.Fatalf("recycles=%d, want 2", recycles)
	}
}

func TestPoolFlushInvalidation(t *testing.T) {
	m := newPoolManager(t, poolCfg())
	_, s, err := m.Compile([]byte("return 42"))
	if err != nil {
		t.Fatal(err)
	}

	// Idle VMs are dropped by FLUSH.
	vm1, _ := runOnce(t, m, s)
	m.Flush()
	vm2, _ := runOnce(t, m, s)
	if vm1 == vm2 {
		t.Fatalf("checkout after FLUSH reused a pre-FLUSH VM")
	}

	// A VM leased across FLUSH is recycled on release (stale epoch),
	// never returned to the pool.
	vm3, err := m.CheckoutVM(s)
	if err != nil {
		t.Fatal(err)
	}
	m.Flush()
	m.ReleaseVM(vm3, s.SHA1, false)
	if idle, _, _, recycles, _ := m.PoolStats(); idle != 0 || recycles != 1 {
		t.Fatalf("after cross-FLUSH lease: idle=%d recycles=%d, want 0/1 (epoch recycle)", idle, recycles)
	}
	// The VM must not be served again:
	vm4, err := m.CheckoutVM(s)
	if err != nil {
		t.Fatal(err)
	}
	if vm4 == vm3 {
		t.Fatalf("stale-epoch VM was served again")
	}
	m.ReleaseVM(vm4, s.SHA1, false)
}

func TestPoolKillDiscardsVM(t *testing.T) {
	m := newPoolManager(t, poolCfg())
	m.hardDeadlineMs.Store(150) // tight watchdog for a fast test
	_, s, err := m.Compile([]byte("while true do end"))
	if err != nil {
		t.Fatal(err)
	}

	vm1, err := runOnce(t, m, s)
	if err == nil {
		t.Fatal("infinite loop returned no error")
	}
	var se *host.ScriptError
	if !errors.As(err, &se) || !strings.Contains(se.Error(), "hard execution deadline") {
		t.Fatalf("deadline error shape: %v", err)
	}
	if !IsVMFatal(err) {
		t.Fatalf("IsVMFatal(%v) = false, want true", err)
	}
	vm2, _ := runOnce(t, m, s) // killed again by the deadline — a fresh VM
	if vm1 == vm2 {
		t.Fatalf("deadline-killed VM was returned to the pool")
	}
}

func TestPoolLRUEviction(t *testing.T) {
	cfg := poolCfg()
	cfg.VMPoolMax = 2
	m := newPoolManager(t, cfg)
	compile := func(src string) *host.Script {
		_, s, err := m.Compile([]byte(src))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	sa, sb, sc := compile("return 1"), compile("return 2"), compile("return 3")

	vma, _ := runOnce(t, m, sa)
	vmb, _ := runOnce(t, m, sb)
	if idle, _, _, _, _ := m.PoolStats(); idle != 2 {
		t.Fatalf("idle=%d, want 2", idle)
	}
	vmc, _ := runOnce(t, m, sc) // pushes past the cap: A is the LRU victim
	if idle, _, _, _, evictions := m.PoolStats(); idle != 2 || evictions != 1 {
		t.Fatalf("idle=%d evictions=%d, want 2/1", idle, evictions)
	}

	// A was evicted: its next checkout is a miss (fresh VM). B and C hit.
	vm, err := m.CheckoutVM(sa)
	if err != nil {
		t.Fatal(err)
	}
	if vm == vma {
		t.Fatalf("evicted VM served again")
	}
	m.ReleaseVM(vm, sa.SHA1, false) // exceeds cap again: evicts B (now oldest)
	vm, err = m.CheckoutVM(sc)
	if err != nil {
		t.Fatal(err)
	}
	if vm != vmc {
		t.Fatalf("C's VM should have survived; got a different one")
	}
	m.ReleaseVM(vm, sc.SHA1, false)
	_ = vmb
}

func TestPoolDisabled(t *testing.T) {
	m := newPoolManager(t, Config{}) // VMPoolSize 0: the pre-M8e path
	_, s, err := m.Compile([]byte("return 42"))
	if err != nil {
		t.Fatal(err)
	}
	vm1, _ := runOnce(t, m, s)
	vm2, _ := runOnce(t, m, s)
	if vm1 == vm2 {
		t.Fatalf("pool disabled but the VM was reused")
	}
	if idle, hits, misses, recycles, _ := m.PoolStats(); int64(idle)+int64(hits)+int64(misses)+int64(recycles) != 0 {
		t.Fatalf("pool counters moved with pooling disabled: idle=%d hits=%d misses=%d recycles=%d",
			idle, hits, misses, recycles)
	}
}

func TestPoolConcurrentSmoke(t *testing.T) {
	m := newPoolManager(t, poolCfg())
	// Two alternating sources: the Compile pointer-identity race
	// (host.ErrScriptBound wedge) needs two SHAs in flight at once.
	_, sa, err := m.Compile([]byte("return redis.sha1hex('x')"))
	if err != nil {
		t.Fatal(err)
	}
	_, sb, err := m.Compile([]byte("return redis.sha1hex('y')"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				// Interleave Compile with runs: EVAL (source form)
				// recompiles on every call in production.
				src := "return redis.sha1hex('x')"
				s := sa
				if g%2 == 1 {
					src = "return redis.sha1hex('y')"
					s = sb
				}
				_, s2, err := m.Compile([]byte(src))
				if err != nil {
					t.Errorf("Compile: %v", err)
					return
				}
				if s2 != s {
					t.Errorf("Compile returned a different *Script for the same source")
					return
				}
				vm, err := m.CheckoutVM(s)
				if err != nil {
					t.Errorf("CheckoutVM: %v", err)
					return
				}
				if _, _, rerr := m.RunOnVM(context.Background(), vm, s, nil, nil, false, nil); rerr != nil {
					t.Errorf("run: %v", rerr)
				}
				m.ReleaseVM(vm, s.SHA1, false)
			}
		}(g)
	}
	wg.Wait()
}

func TestIsVMFatal(t *testing.T) {
	scriptErr := func(msg string) error {
		return &host.ScriptError{ErrValue: host.String(msg), Line: 3}
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"ordinary script error", scriptErr("user_script:3: attempt to call a nil value"), false},
		{"script kill", scriptErr("ERR Script killed by user with SCRIPT KILL..."), true},
		{"hard deadline", scriptErr("ERR Script killed by the hard execution deadline (script_hard_deadline_ms)"), true},
		{"oom plain", scriptErr("not enough memory"), true},
		{"oom with position", scriptErr("user_script:5: not enough memory"), true},
		{"trap", &host.TrapError{}, true},
		{"error table (pcall-style)", &host.ScriptError{ErrValue: host.Table(host.KV{Key: host.String("err"), Val: host.String("boom")})}, false},
	}
	for _, c := range cases {
		if got := IsVMFatal(c.err); got != c.want {
			t.Errorf("%s: IsVMFatal = %v, want %v", c.name, got, c.want)
		}
	}
}
