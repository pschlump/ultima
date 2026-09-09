package shard

// M5b maxmemory eviction (design doc §5.2, decisions D10/D14): per-shard
// memory accounting, the eviction-policy engine, and the OOM gate's
// eviction attempt. The eviction loop runs inside the shard goroutine
// after each command task: while the shard's estimated bytes exceed its
// quota (maxmemory / shardCount) it picks victims per the configured
// policy and deletes them, emitting the "evicted" keyspace event via
// OnKeyGone (M5a) and counting EvictedKeys.
//
// Victim selection (D14: exact LRU via pluto lru_ts):
//   - allkeys-lru / volatile-lru: the LRU tracker's oldest key
//     (PopOldest), validated against the keyspace — stale tracker
//     entries (deleted or flushed keys, TTL dropped by PERSIST) are
//     discarded and the next-oldest tried.
//   - allkeys-lfu / volatile-lfu: maxmemory-samples random keys from the
//     shard's stripe (SampleStripe), evicting the one with the lowest
//     LFU counter — Redis's approximate-LFU sampling; keys never
//     accessed since tracking began count as coldest.
//   - volatile-ttl: the expiry min-heap's top (shortest TTL first).
//   - allkeys-random / volatile-random: the first live (volatile: TTL'd)
//     key of a random sample.
//   - noeviction: the loop never runs; the OOM gate in
//     lib/commands rejects denyoom commands while over the limit.

import (
	"strings"

	"github.com/pschlump/pluto/lfu_ts"
	"github.com/pschlump/pluto/lru_ts"
)

// EvictPolicy is one of the eight Redis maxmemory policies.
type EvictPolicy int32

// The policies. PolicyNoEviction is the zero value so a fresh engine —
// and an unset maxmemory-policy — defaults to noeviction like Redis.
const (
	PolicyNoEviction EvictPolicy = iota
	PolicyVolatileLRU
	PolicyVolatileLFU
	PolicyVolatileRandom
	PolicyVolatileTTL
	PolicyAllKeysLRU
	PolicyAllKeysLFU
	PolicyAllKeysRandom
)

// EvictPolicyNames lists the policies in the order Redis's CONFIG SET
// error names them (probed against 7.2.7).
var EvictPolicyNames = []string{
	"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl",
	"allkeys-lru", "allkeys-lfu", "allkeys-random", "noeviction",
}

// policyName renders each policy constant, indexed by EvictPolicy.
var policyName = []string{
	PolicyNoEviction:     "noeviction",
	PolicyVolatileLRU:    "volatile-lru",
	PolicyVolatileLFU:    "volatile-lfu",
	PolicyVolatileRandom: "volatile-random",
	PolicyVolatileTTL:    "volatile-ttl",
	PolicyAllKeysLRU:     "allkeys-lru",
	PolicyAllKeysLFU:     "allkeys-lfu",
	PolicyAllKeysRandom:  "allkeys-random",
}

// String returns the canonical Redis name of the policy.
func (p EvictPolicy) String() string {
	if p < 0 || int(p) >= len(policyName) {
		return "noeviction"
	}
	return policyName[p]
}

// ParseEvictPolicy matches a CONFIG value (case-insensitively) to a
// policy, reporting whether it named one.
func ParseEvictPolicy(v string) (EvictPolicy, bool) {
	for p, name := range policyName {
		if strings.EqualFold(v, name) {
			return EvictPolicy(p), true
		}
	}
	return PolicyNoEviction, false
}

// evictTrackerCap is the eviction trackers' soft capacity — deliberately
// huge so the LRU's own capacity eviction never fires (victim selection
// is the eviction loop's job, and victims are validated against the
// keyspace anyway, so stale entries cost memory, not correctness).
const evictTrackerCap = 1 << 30

// DefaultEvictSamples mirrors Redis's maxmemory-samples default: the
// candidate count for the LFU and random policies. Engine.EvictSamples
// carries it; tests shrink or widen it.
const DefaultEvictSamples = 5

// MaxMemory returns the configured maxmemory in bytes (0 = unlimited).
func (e *Engine) MaxMemory() int64 { return e.maxMemory.Load() }

// SetMaxMemory updates maxmemory (config file, CONFIG SET via the
// command layer).
func (e *Engine) SetMaxMemory(n int64) { e.maxMemory.Store(n) }

// Policy returns the configured eviction policy.
func (e *Engine) Policy() EvictPolicy { return EvictPolicy(e.policy.Load()) }

// SetPolicy updates the eviction policy (config file, CONFIG SET via the
// command layer).
func (e *Engine) SetPolicy(p EvictPolicy) { e.policy.Store(int32(p)) }

// UsedBytes returns the whole keyspace's estimated memory in bytes: the
// sum over shards and DBs of the M5b accounting counters.
func (e *Engine) UsedBytes() int64 {
	var total int64
	for _, s := range e.shard {
		total += s.usedTotal.Load()
	}
	return total
}

// OverMemory reports whether the keyspace estimate exceeds maxmemory
// (strictly, like Redis's getMaxmemoryState). Always false when
// maxmemory is 0 (unlimited).
func (e *Engine) OverMemory() bool {
	mm := e.maxMemory.Load()
	return mm > 0 && e.UsedBytes() > mm
}

// EvictNow runs the eviction loop on every shard, synchronously, and
// reports whether the keyspace is still over maxmemory afterwards — the
// OOM gate's performEvictions analogue (§5.2): commands flagged denyoom
// are rejected only when eviction could not get back under the limit.
// tok carries the caller's pause token (EXEC runs under PauseAll).
func (e *Engine) EvictNow(tok uint64) (stillOver bool) {
	if e.maxMemory.Load() <= 0 {
		return false
	}
	for i := range e.shard {
		e.DoTok(tok, i, func(s *Shard) { s.maybeEvict() })
	}
	return e.OverMemory()
}

// used returns the shard's current memory estimate across all DBs.
// Call only from inside the shard goroutine.
func (s *Shard) used() int64 { return s.usedTotal.Load() }

// maybeEvict evicts keys until the shard is back under its per-shard
// quota (maxmemory / shardCount) or no victim is available. It runs
// after every shard task (the run and park loops); with maxmemory unset
// or noeviction configured it costs a few atomic loads and returns.
// Call only from inside the shard goroutine.
func (s *Shard) maybeEvict() {
	mm := s.eng.maxMemory.Load()
	if mm <= 0 {
		return
	}
	pol := EvictPolicy(s.eng.policy.Load())
	if pol == PolicyNoEviction {
		return
	}
	quota := mm / int64(s.eng.n)
	guard := 0
	for s.used() > quota {
		if !s.evictOne(pol) {
			return // no evictable key: the OOM gate reports the failure
		}
		// A pathological victim loop (e.g. every candidate stale) must
		// not stall the shard: bound the iterations per task.
		if guard++; guard > 4*s.eng.EvictSamples+64 {
			return
		}
	}
}

// evictOne picks and deletes one victim per the policy, reporting
// whether a key was evicted. Call only from inside the shard goroutine.
func (s *Shard) evictOne(pol EvictPolicy) bool {
	switch pol {
	case PolicyAllKeysLRU:
		return s.evictLRU(s.lruAll, false)
	case PolicyVolatileLRU:
		return s.evictLRU(s.lruVol, true)
	case PolicyAllKeysLFU:
		return s.evictLFU(s.lfuAll, false)
	case PolicyVolatileLFU:
		return s.evictLFU(s.lfuVol, true)
	case PolicyVolatileTTL:
		return s.evictTTL()
	case PolicyAllKeysRandom:
		return s.evictRandom(false)
	case PolicyVolatileRandom:
		return s.evictRandom(true)
	}
	return false
}

// evictLRU evicts the LRU tracker's oldest live key. volatile restricts
// victims to keys that still carry a TTL; stale tracker entries (key
// gone, TTL dropped) are discarded as they surface.
func (s *Shard) evictLRU(lru *lru_ts.Lru[tombKey, struct{}], volatile bool) bool {
	for range 2 * s.eng.EvictSamples {
		k, _, ok := lru.PopOldest()
		if !ok {
			return false // tracker exhausted
		}
		it, found := s.tab(k.db).Search(item{Key: k.key})
		if !found {
			continue // stale tracker entry
		}
		e := it.E
		if volatile && e.ExpireAtMs == 0 {
			continue // TTL dropped since the key was tracked
		}
		if s.evictEntry(k.db, k.key, e) {
			return true
		}
		// Passively expired: evictEntry expired it; keep looking.
	}
	return false
}

// evictLFU samples evictSamples random keys from each DB's stripe for
// this shard and evicts the coldest by LFU counter (Redis's approximate
// LFU: the sample stands in for a global minimum). Keys never touched
// since tracking began read as counter 0 — coldest, so freshly loaded
// keys evict first.
func (s *Shard) evictLFU(lfu *lfu_ts.Lfu[tombKey], volatile bool) bool {
	best := tombKey{db: -1}
	var bestE *Entry
	var bestCount uint8
	for db := range s.eng.dbs {
		for _, it := range s.tab(db).SampleStripe(s.id, s.eng.EvictSamples, s.rng) {
			e := it.E
			if volatile && e.ExpireAtMs == 0 {
				continue
			}
			var cnt uint8
			if c, ok := lfu.Counter(tombKey{db, it.Key}); ok {
				cnt = c
			} // untracked key: counter 0, coldest
			if best.db < 0 || cnt < bestCount {
				best = tombKey{db, it.Key}
				bestE = e
				bestCount = cnt
			}
		}
	}
	if best.db < 0 {
		return false
	}
	if s.evictEntry(best.db, best.key, bestE) {
		return true
	}
	return false
}

// evictTTL evicts the key with the shortest TTL: the expiry min-heap's
// top, discarding stale heap entries exactly like the sweep.
func (s *Shard) evictTTL() bool {
	now := s.eng.NowMs()
	for range 2 * s.eng.EvictSamples {
		top, ok := s.exp.Peek()
		if !ok {
			return false // no TTL'd key at all
		}
		s.exp.Pop()
		it, found := s.tab(top.DB).Search(item{Key: top.Key})
		if !found {
			continue
		}
		e := it.E
		if e.ExpireAtMs != top.At || e.ExpGen != top.Gen {
			continue // stale heap entry (TTL changed or dropped)
		}
		if e.ExpireAtMs <= now {
			// Already due: expire it (reason "expired"), keep looking.
			s.expireEntry(top.DB, top.Key, e)
			continue
		}
		s.evictEntry(top.DB, top.Key, e)
		return true
	}
	return false
}

// evictRandom evicts the first live key of a random stripe sample
// (volatile: the first that still carries a TTL).
func (s *Shard) evictRandom(volatile bool) bool {
	for db := range s.eng.dbs {
		for _, it := range s.tab(db).SampleStripe(s.id, s.eng.EvictSamples, s.rng) {
			e := it.E
			if volatile && e.ExpireAtMs == 0 {
				continue
			}
			if s.evictEntry(db, it.Key, e) {
				return true
			}
		}
	}
	return false
}

// evictEntry deletes one live victim, maintaining the WATCH tombstone,
// the memory estimate, the trackers, the evicted_keys stat, and the
// "evicted" keyspace event (M5a OnKeyGone sink; M5c will also synthesize
// AOF DEL records from the same sink). It reports false — after expiring
// the entry instead — when the candidate turns out to be already due
// (expiry beats eviction for an overdue key, matching Redis's
// lazy-free-on-access ordering).
func (s *Shard) evictEntry(db int, key string, e *Entry) bool {
	if e.ExpireAtMs > 0 && e.ExpireAtMs <= s.eng.NowMs() {
		s.expireEntry(db, key, e)
		return false
	}
	s.tombstone(db, key, e.Version)
	s.tab(db).Delete(item{Key: key})
	s.addMem(db, -e.memBytes)
	s.untrack(db, key)
	s.eng.EvictedKeys.Add(1)
	if s.OnKeyGone != nil {
		s.OnKeyGone(db, key, "evicted")
	}
	return true
}

// expireEntry deletes one overdue entry as a passive/active expiry:
// tombstone, accounting, trackers, expired_keys stat, "expired" event.
// Call only from inside the shard goroutine.
func (s *Shard) expireEntry(db int, key string, e *Entry) {
	s.tombstone(db, key, e.Version)
	s.tab(db).Delete(item{Key: key})
	s.addMem(db, -e.memBytes)
	s.untrack(db, key)
	s.eng.ExpiredKeys.Add(1)
	if s.OnKeyGone != nil {
		s.OnKeyGone(db, key, "expired")
	}
}
