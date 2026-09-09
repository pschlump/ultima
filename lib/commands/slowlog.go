package commands

// Slowlog ring and per-command latency stats for the M6c management API
// (design doc §10.1: GET /api/v1/slowlog, GET /api/v1/latency). The ring
// follows Redis semantics: commands taking at least slowlog-log-slower-than
// microseconds are recorded (threshold -1 disables, 0 logs everything),
// the ring holds the newest slowlog-max-len entries, entry args are
// truncated to 32 arguments of 128 bytes. The RESP SLOWLOG command itself
// is not part of M6c; the ring is served over HTTP.

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// slowlogMaxArgs and slowlogMaxArgBytes are Redis's SLOWLOG_ENTRY_MAX_ARGC
// / SLOWLOG_ENTRY_MAX_ARG_LENGTH truncations.
const (
	slowlogMaxArgs     = 32
	slowlogMaxArgBytes = 128
)

// SlowlogEntry is one recorded slow command.
type SlowlogEntry struct {
	ID         int64
	Timestamp  int64 // Unix seconds
	DurationUs int64
	Args       []string
	ClientAddr string
	ClientName string
}

// slowlogRing is a bounded newest-first-free ring: entries are appended
// and trimmed under mu; reads copy out newest-first.
type slowlogRing struct {
	mu      sync.Mutex
	entries []SlowlogEntry
	nextID  int64
}

// noteDuration records one command's wall-clock duration: the latency
// stats always, the slowlog ring when the (enabled) threshold is met.
// Runs at the end of Execute on the connection goroutine.
func (e *Engine) noteDuration(cs *ConnState, name string, args [][]byte, start time.Time) {
	dur := time.Since(start).Microseconds()
	e.latency.note(name, dur)
	thr := e.slowlogSlowerThan.Load()
	if thr < 0 || dur < thr {
		return
	}
	entry := SlowlogEntry{
		Timestamp:  start.Unix(),
		DurationUs: dur,
		ClientAddr: cs.Addr,
		ClientName: cs.Name,
		Args:       make([]string, 0, min(len(args), slowlogMaxArgs)),
	}
	for i, a := range args {
		if i >= slowlogMaxArgs {
			break
		}
		s := string(a)
		if len(s) > slowlogMaxArgBytes {
			s = s[:slowlogMaxArgBytes-3] + "..."
		}
		entry.Args = append(entry.Args, s)
	}
	e.slowlog.add(entry, int(e.slowlogMaxLen.Load()))
}

func (r *slowlogRing) add(entry SlowlogEntry, maxLen int) {
	r.mu.Lock()
	entry.ID = r.nextID
	r.nextID++
	r.entries = append(r.entries, entry)
	if over := len(r.entries) - maxLen; over > 0 {
		// Trim the oldest. maxLen is a runtime knob, so a shrink can
		// drop many entries at once.
		r.entries = append([]SlowlogEntry(nil), r.entries[over:]...)
	}
	r.mu.Unlock()
}

// get returns up to count entries, newest first; count <= 0 means all.
func (r *slowlogRing) get(count int) []SlowlogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.entries)
	if count > 0 && count < n {
		n = count
	}
	out := make([]SlowlogEntry, 0, n)
	for i := len(r.entries) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, r.entries[i])
	}
	return out
}

func (r *slowlogRing) reset() {
	r.mu.Lock()
	r.entries = nil
	r.mu.Unlock()
}

// Slowlog returns up to count slowlog entries, newest first (count <= 0
// returns everything in the ring).
func (e *Engine) Slowlog(count int) []SlowlogEntry { return e.slowlog.get(count) }

// SlowlogReset clears the ring (SLOWLOG RESET).
func (e *Engine) SlowlogReset() { e.slowlog.reset() }

// SlowlogLen is the current ring occupancy (SLOWLOG LEN).
func (e *Engine) SlowlogLen() int {
	e.slowlog.mu.Lock()
	defer e.slowlog.mu.Unlock()
	return len(e.slowlog.entries)
}

// SlowlogSlowerThan / SetSlowlogSlowerThan back the slowlog-log-slower-than
// config param (microseconds; -1 disables the slowlog).
func (e *Engine) SlowlogSlowerThan() int64 { return e.slowlogSlowerThan.Load() }

// SetSlowlogSlowerThan sets the slowlog-log-slower-than threshold.
func (e *Engine) SetSlowlogSlowerThan(v int64) { e.slowlogSlowerThan.Store(v) }

// SlowlogMaxLen / SetSlowlogMaxLen back the slowlog-max-len config param.
func (e *Engine) SlowlogMaxLen() int64 { return e.slowlogMaxLen.Load() }

// SetSlowlogMaxLen sets the slowlog-max-len ring bound.
func (e *Engine) SetSlowlogMaxLen(v int64) { e.slowlogMaxLen.Store(v) }

// --- latency stats -------------------------------------------------------

// CommandLatency is one command's cumulative latency stats.
type CommandLatency struct {
	Command string
	Count   int64
	TotalUs int64
	MaxUs   int64
	AvgUs   int64
}

type latencyStat struct {
	count atomic.Int64
	total atomic.Int64
	max   atomic.Int64
}

// latencyTable maps command name → stats. Updated per command; reads are
// rare (HTTP endpoint), so a mutex-guarded map is fine.
type latencyTable struct {
	mu sync.Mutex
	m  map[string]*latencyStat
}

func (t *latencyTable) note(name string, durUs int64) {
	t.mu.Lock()
	s := t.m[name]
	if s == nil {
		s = &latencyStat{}
		t.m[name] = s
	}
	t.mu.Unlock()
	s.count.Add(1)
	s.total.Add(durUs)
	for {
		cur := s.max.Load()
		if durUs <= cur || s.max.CompareAndSwap(cur, durUs) {
			break
		}
	}
}

// snapshot returns the stats sorted by command name.
func (t *latencyTable) snapshot() []CommandLatency {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]CommandLatency, 0, len(t.m))
	for name, s := range t.m {
		count := s.count.Load()
		total := s.total.Load()
		cl := CommandLatency{Command: name, Count: count, TotalUs: total, MaxUs: s.max.Load()}
		if count > 0 {
			cl.AvgUs = total / count
		}
		out = append(out, cl)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Command < out[j].Command })
	return out
}

// LatencyStats snapshots the per-command latency table (§10.1 /latency).
func (e *Engine) LatencyStats() []CommandLatency { return e.latency.snapshot() }
