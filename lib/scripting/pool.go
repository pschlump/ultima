package scripting

// The M8e R3 per-script VM pool (docs/m8-detailed-plan.md, guide risk R3):
// a fresh host.VM costs ~44 ms of wazero instantiation while a re-run on a
// bound image costs ~16-31 µs, so hot scripts keep a bound VM checked out
// per run instead of building one per EVAL. The host's one-script law
// (host.ErrScriptBound) makes the pool per-SHA by construction.
//
// Byte-exactness is preserved by construction: the M8d globals lockdown
// blocks cross-run global leakage, KEYS/ARGV are restaged per run, the RNG
// is reseeded per run (S6), and setresp state is Go-side per run. The
// differential suite (tests/differential/scripts_m8.go) gates this.
//
// Recycling is mandatory, not optional: guest GC is permanently stopped
// (host/vm.go — "register cells are not GC roots"), so a reused VM's Lua
// heap and wasm linear memory grow monotonically. A pooled VM is recycled
// (closed; the next checkout pays one fresh instantiation) when any of:
//
//  1. its run ended in SCRIPT KILL / the hard deadline / "not enough
//     memory" (partial guest state + consumed heap — IsVMFatal);
//  2. it has served script_vm_recycle_runs runs;
//  3. its heap (VM.UsedBytes) crosses script_vm_recycle_pct % of the
//     script_max_memory_mb budget — the direct measure of the GC-stopped
//     growth;
//  4. a SCRIPT FLUSH landed while it was leased (stale epoch).
//
// Idle capacity is bounded per SHA (script_vm_pool_size; 0 disables
// pooling — the pre-M8e fresh-VM path) and globally (script_vm_pool_max,
// LRU eviction of the oldest idle VM) so ad-hoc EVAL source churn cannot
// grow the pool without bound.

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/yuin/gopher-lua/host"
)

// poolEntry is one pooled VM: bound to entry.sha on its first Run.
type poolEntry struct {
	vm       *host.VM
	sha      string
	runs     int       // runs served (release count)
	epoch    uint64    // pool epoch at first release; stale after Flush
	lastIdle time.Time // LRU eviction basis while idle
}

// IsVMFatal reports whether a run error leaves the VM unfit for reuse:
// the kill/deadline class (partial Lua state mid-execution, heap partially
// consumed), an uncaught "not enough memory" (the VM sits at its budget),
// or a wasm trap (runtime-level failure). Ordinary script errors are
// pcall-caught inside the guest and cleared by the next Run's hygiene
// (rt_err_clear), so they do not recycle the VM.
func IsVMFatal(err error) bool {
	var se *host.ScriptError
	if errors.As(err, &se) {
		if se.ErrValue.Kind != host.KindString {
			return false
		}
		msg := se.ErrValue.Str
		return strings.HasPrefix(msg, "ERR Script killed by") ||
			strings.Contains(msg, "not enough memory")
	}
	var te *host.TrapError
	return errors.As(err, &te)
}

// CheckoutVM leases a VM bound to s — a pooled bound VM on a hit (µs), a
// fresh instantiation on a miss (~44 ms). Call BEFORE taking the shard
// pause: a miss must not instantiate under PauseAll. Pair with ReleaseVM.
func (m *Manager) CheckoutVM(s *host.Script) (*host.VM, error) {
	if m.cfg.VMPoolSize > 0 {
		m.poolMu.Lock()
		if q := m.pool[s.SHA1]; len(q) > 0 {
			e := q[len(q)-1] // LIFO within the SHA: reuses the hottest VM
			m.pool[s.SHA1] = q[:len(q)-1]
			m.poolIdle--
			m.poolMu.Unlock()
			m.poolHits.Add(1)
			return e.vm, nil
		}
		m.poolMisses.Add(1)
		m.poolMu.Unlock()
	}
	return m.eng.NewVM()
}

// ReleaseVM returns a checked-out VM after its run: recycled (closed) when
// discard is set (IsVMFatal), the run-count or heap watermark trips, the
// pool epoch moved (SCRIPT FLUSH mid-run), the per-SHA depth is full, or
// the manager is closing; otherwise the VM goes back to its SHA's idle
// queue and the oldest idle VM is evicted past the global cap.
func (m *Manager) ReleaseVM(vm *host.VM, sha string, discard bool) {
	if m.cfg.VMPoolSize <= 0 {
		_ = vm.Close()
		return
	}

	var closers []*host.VM
	m.poolMu.Lock()
	e := m.vmMeta[vm]
	if e == nil {
		e = &poolEntry{vm: vm, sha: sha, epoch: m.poolEpoch}
		m.vmMeta[vm] = e
	}
	e.runs++

	recycle := discard || m.closed || e.epoch != m.poolEpoch ||
		(m.cfg.VMRecycleRuns > 0 && e.runs >= m.cfg.VMRecycleRuns)
	if !recycle && m.cfg.VMRecyclePct > 0 && m.budgetBytes > 0 {
		used := vm.UsedBytes(context.Background())
		if used*100 >= m.budgetBytes*int64(m.cfg.VMRecyclePct) {
			recycle = true
		}
	}

	if !recycle {
		if q := m.pool[sha]; len(q) >= m.cfg.VMPoolSize {
			recycle = true // depth full (overflow VM from a contended miss)
		} else {
			e.lastIdle = time.Now()
			m.pool[sha] = append(q, e)
			m.poolIdle++
			for m.poolIdle > m.cfg.VMPoolMax {
				if v := m.evictOldestIdleLocked(); v != nil {
					closers = append(closers, v)
				} else {
					break
				}
			}
		}
	}
	if recycle {
		delete(m.vmMeta, vm)
		m.poolRecycles.Add(1)
		closers = append(closers, vm)
	}
	m.poolMu.Unlock()

	for _, v := range closers {
		_ = v.Close()
	}
}

// evictOldestIdleLocked removes the globally oldest idle pool entry.
func (m *Manager) evictOldestIdleLocked() *host.VM {
	var oldest *poolEntry
	for _, q := range m.pool {
		for _, e := range q {
			if oldest == nil || e.lastIdle.Before(oldest.lastIdle) {
				oldest = e
			}
		}
	}
	if oldest == nil {
		return nil
	}
	q := m.pool[oldest.sha]
	for i, e := range q {
		if e == oldest {
			m.pool[oldest.sha] = append(q[:i], q[i+1:]...)
			break
		}
	}
	delete(m.vmMeta, oldest.vm)
	m.poolIdle--
	m.poolEvictions.Add(1)
	return oldest.vm
}

// drainPoolLocked empties every idle queue; the caller closes the VMs
// outside the lock. Leased VMs carry their entry's epoch and are recycled
// on release (Flush) or closed on release (Close).
func (m *Manager) drainPoolLocked() []*host.VM {
	var out []*host.VM
	for sha, q := range m.pool {
		for _, e := range q {
			delete(m.vmMeta, e.vm)
			out = append(out, e.vm)
		}
		delete(m.pool, sha)
	}
	m.poolIdle = 0
	return out
}

// PoolStats reports pool counters for INFO: idle VMs and the
// hit/miss/recycle/eviction totals since startup.
func (m *Manager) PoolStats() (idle int, hits, misses, recycles, evictions uint64) {
	if m.cfg.VMPoolSize <= 0 {
		return 0, 0, 0, 0, 0
	}
	m.poolMu.Lock()
	idle = m.poolIdle
	m.poolMu.Unlock()
	return idle, m.poolHits.Load(), m.poolMisses.Load(), m.poolRecycles.Load(), m.poolEvictions.Load()
}

// poolMu guards pool, vmMeta, poolEpoch, poolIdle, closed.
// (field docs live on Manager in scripting.go)
