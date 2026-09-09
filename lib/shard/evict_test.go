package shard

// M5b eviction tests (design doc §5.2, D10/D14): memory accounting at
// the write choke points, victim selection per policy, the quota loop,
// and the evicted stat/event. Engine-level OOM-gate behavior (the
// denyoom/MULTI/EXEC replies) lives in lib/commands/oom_test.go.

import (
	"fmt"
	"testing"
)

// storeStrLen stores a string value of n bytes (memory pressure comes
// from value sizes, so quota math is readable in tests).
func storeStrLen(e *Engine, db int, key string, n int) {
	e.Do(db, []byte(key), func(s *Shard) {
		s.Store(db, key, &Entry{Type: TypeString, Str: make([]byte, n)})
	})
}

func exists(t *testing.T, e *Engine, db int, key string) bool {
	t.Helper()
	var found bool
	e.Do(db, []byte(key), func(s *Shard) {
		_, found = s.Lookup(db, key)
	})
	return found
}

func TestMemAccounting(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()

	if got := e.UsedBytes(); got != 0 {
		t.Fatalf("fresh engine usedBytes = %d, want 0", got)
	}
	storeStrLen(e, 0, "a", 100)
	storeStrLen(e, 1, "b", 200)
	after2 := e.UsedBytes()
	if after2 <= 300 {
		t.Fatalf("usedBytes = %d after two values of 100+200 bytes, want > 300 (overhead counts)", after2)
	}

	// Replace adjusts by the delta, not the total.
	storeStrLen(e, 0, "a", 400)
	if got := e.UsedBytes(); got != after2+300 {
		t.Fatalf("usedBytes after replace = %d, want %d", got, after2+300)
	}

	// In-place mutation via Touch re-accounts.
	e.Do(0, []byte("a"), func(s *Shard) {
		ent, _ := s.Lookup(0, "a")
		ent.Str = make([]byte, 50)
		s.Touch(0, "a", ent)
	})
	if got := e.UsedBytes(); got != after2-50 {
		t.Fatalf("usedBytes after Touch shrink = %d, want %d", got, after2-50)
	}

	// Delete subtracts.
	e.Do(1, []byte("b"), func(s *Shard) { s.Delete(1, "b") })
	if got := e.UsedBytes(); got != after2-50-(200+80+1) {
		t.Fatalf("usedBytes after delete = %d, want %d", got, after2-50-281)
	}

	// FLUSHDB zeroes the shard's whole share of that DB.
	storeStrLen(e, 2, "c", 500)
	e.FlushDB(2)
	e.Do(0, []byte("a"), func(s *Shard) {
		if got := s.usedBytes[2].Load(); got != 0 {
			t.Errorf("db2 usedBytes after FlushDB = %d, want 0", got)
		}
	})
}

// fillEvict builds an engine with one tiny quota shard setup: 1 shard
// (stripe == table, so samples see every key), 1 DB, maxmemory set so
// roughly limitBytes total fits.
func fillEvict(policy EvictPolicy, limit int64) *Engine {
	e := NewEngine(1, 1)
	e.SetMaxMemory(limit)
	e.SetPolicy(policy)
	return e
}

func TestEvictAllKeysLRU(t *testing.T) {
	e := fillEvict(PolicyAllKeysLRU, 500)
	defer e.Close()

	// Four ~112-byte keys (30 value + key + 80 overhead each) stay under
	// the 500-byte quota; the fill must not trip the post-task loop yet.
	for i := range 4 {
		storeStrLen(e, 0, fmt.Sprintf("k%d", i), 30)
	}
	// Refresh k0/k1 so k2 is the LRU end.
	exists(t, e, 0, "k0")
	exists(t, e, 0, "k1")
	// Pushing over the quota evicts the LRU end through the post-task
	// loop — k2 and k3 go, the refreshed and fresh keys survive.
	storeStrLen(e, 0, "k4", 30)
	storeStrLen(e, 0, "k5", 30)

	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow: still over quota with evictable keys")
	}
	for _, k := range []string{"k2", "k3"} {
		if exists(t, e, 0, k) {
			t.Errorf("LRU victim %s survived", k)
		}
	}
	for _, k := range []string{"k0", "k1", "k4", "k5"} {
		if !exists(t, e, 0, k) {
			t.Errorf("recently used key %s was evicted", k)
		}
	}
	if got := e.EvictedKeys.Load(); got < 2 {
		t.Errorf("evicted_keys = %d, want >= 2", got)
	}
}

func TestEvictVolatileLRU(t *testing.T) {
	e := fillEvict(PolicyVolatileLRU, 500)
	defer e.Close()
	now := e.NowMs()

	// Volatile keys only: the persistent keys must survive no matter how
	// old they are.
	storeStrLen(e, 0, "persist-old", 30)
	for i := range 3 {
		k := fmt.Sprintf("vol%d", i)
		e.Do(0, []byte(k), func(s *Shard) {
			ent := &Entry{Type: TypeString, Str: make([]byte, 30), ExpireAtMs: now + 100000}
			s.Store(0, k, ent)
			s.PushExpire(0, k, ent)
		})
	}
	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow: still over with volatile keys available")
	}
	if !exists(t, e, 0, "persist-old") {
		t.Error("volatile-lru evicted a persistent key")
	}

	// Exhaust the volatile set: eviction must report failure (the OOM
	// gate's signal) rather than touch persistent keys.
	e.Do(0, []byte("persist-old"), func(s *Shard) {
		s.Delete(0, "vol1")
		s.Delete(0, "vol2")
	})
	for _, k := range []string{"big1", "big2"} {
		storeStrLen(e, 0, k, 400)
	}
	if stillOver := e.EvictNow(0); !stillOver {
		t.Error("EvictNow with one volatile key and a blown quota = under quota, want still over")
	}
	if !exists(t, e, 0, "big1") || !exists(t, e, 0, "big2") {
		t.Error("volatile-lru evicted persistent keys when volatile set ran dry")
	}
}

func TestEvictVolatileTTL(t *testing.T) {
	e := fillEvict(PolicyVolatileTTL, 500)
	defer e.Close()
	now := e.NowMs()

	// vol-short has the shortest TTL: it is the first victim even though
	// it is the freshest write.
	mk := func(key string, ttlMs int64) {
		e.Do(0, []byte(key), func(s *Shard) {
			ent := &Entry{Type: TypeString, Str: make([]byte, 30), ExpireAtMs: now + ttlMs}
			s.Store(0, key, ent)
			s.PushExpire(0, key, ent)
		})
	}
	mk("vol-long", 90000)
	mk("vol-mid", 50000)
	mk("vol-short", 10000)
	mk("vol-a", 80000)
	mk("vol-b", 70000)
	mk("vol-c", 60000)

	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow: still over with TTL'd keys available")
	}
	if exists(t, e, 0, "vol-short") {
		t.Error("volatile-ttl kept the shortest-TTL key")
	}
	if exists(t, e, 0, "vol-mid") {
		t.Error("volatile-ttl kept the second-shortest-TTL key")
	}
	if !exists(t, e, 0, "vol-long") {
		t.Error("volatile-ttl evicted the longest-TTL key first")
	}
}

func TestEvictAllKeysRandom(t *testing.T) {
	e := fillEvict(PolicyAllKeysRandom, 400)
	defer e.Close()
	for i := range 8 {
		storeStrLen(e, 0, fmt.Sprintf("r%d", i), 30)
	}
	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow: still over with a full table")
	}
	if got := e.EvictedKeys.Load(); got < 1 {
		t.Fatal("allkeys-random evicted nothing")
	}
	if got := e.UsedBytes(); got > 400 {
		t.Errorf("usedBytes = %d after eviction, want <= 400", got)
	}
}

func TestEvictVolatileRandom(t *testing.T) {
	e := fillEvict(PolicyVolatileRandom, 500)
	defer e.Close()
	now := e.NowMs()
	// The persistent keys alone stay under quota, so only the volatile
	// keys are ever eligible victims.
	storeStrLen(e, 0, "stay1", 100)
	storeStrLen(e, 0, "stay2", 100)
	for i := range 2 {
		k := fmt.Sprintf("vr%d", i)
		e.Do(0, []byte(k), func(s *Shard) {
			ent := &Entry{Type: TypeString, Str: make([]byte, 100), ExpireAtMs: now + 60000}
			s.Store(0, k, ent)
			s.PushExpire(0, k, ent)
		})
	}
	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow: still over with volatile keys available")
	}
	if !exists(t, e, 0, "stay1") || !exists(t, e, 0, "stay2") {
		t.Error("volatile-random evicted a persistent key")
	}
	// Quota forces at least one volatile victim.
	if exists(t, e, 0, "vr0") && exists(t, e, 0, "vr1") {
		t.Error("volatile-random evicted nothing")
	}
}

func TestEvictAllKeysLFU(t *testing.T) {
	e := fillEvict(PolicyAllKeysLFU, 500)
	defer e.Close()
	e.EvictSamples = 64 // sample the whole stripe: deterministic victim choice

	// Stay under quota during the fill so the post-task loop does not
	// evict before the heating phase (few enough keys that the 5-sample
	// covers the whole single stripe).
	for i := range 4 {
		storeStrLen(e, 0, fmt.Sprintf("lf%d", i), 30)
	}
	// Heat lf0 and lf1 far above the rest; the cold keys must go first.
	for range 300 {
		exists(t, e, 0, "lf0")
		exists(t, e, 0, "lf1")
	}
	// Push over quota: two cold victims must go, never the hot keys.
	storeStrLen(e, 0, "lf4", 30)
	storeStrLen(e, 0, "lf5", 30)

	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow: still over with a full table")
	}
	if !exists(t, e, 0, "lf0") || !exists(t, e, 0, "lf1") {
		t.Error("allkeys-lfu evicted a frequently accessed key")
	}
	if got := e.EvictedKeys.Load(); got < 1 {
		t.Fatal("allkeys-lfu evicted nothing")
	}
}

func TestEvictEventAndExpiryPrecedence(t *testing.T) {
	e := fillEvict(PolicyAllKeysLRU, 500)
	defer e.Close()

	var gone [][2]string
	e.SetOnKeyGone(func(_ int, key, reason string) {
		gone = append(gone, [2]string{key, reason})
	})

	// An overdue (passively expirable) key that the eviction loop
	// surfaces counts as EXPIRED, not evicted.
	past := e.NowMs() - 1
	e.Do(0, []byte("due"), func(s *Shard) {
		ent := &Entry{Type: TypeString, Str: make([]byte, 30), ExpireAtMs: past}
		s.Store(0, "due", ent)
		s.PushExpire(0, "due", ent)
	})
	for i := range 5 {
		storeStrLen(e, 0, fmt.Sprintf("e%d", i), 30)
	}
	e.EvictNow(0)

	var sawExpired, sawEvicted bool
	for _, g := range gone {
		if g[0] == "due" {
			if g[1] != "expired" {
				t.Errorf("overdue key reported %q, want expired", g[1])
			}
			sawExpired = true
		}
		if g[1] == "evicted" {
			sawEvicted = true
		}
	}
	if !sawExpired {
		t.Error("overdue key never reported gone")
	}
	if !sawEvicted {
		t.Error("no evicted event emitted despite evictions")
	}
	if e.ExpiredKeys.Load() < 1 {
		t.Error("expired_keys not bumped for the overdue key")
	}
}

func TestNoEvictionPolicyNeverEvicts(t *testing.T) {
	e := fillEvict(PolicyNoEviction, 100)
	defer e.Close()
	storeStrLen(e, 0, "x", 500) // way over, but noeviction
	if stillOver := e.EvictNow(0); !stillOver {
		t.Error("EvictNow under noeviction = under quota, want still over (nothing evicted)")
	}
	if !exists(t, e, 0, "x") {
		t.Error("noeviction evicted a key")
	}
	if got := e.EvictedKeys.Load(); got != 0 {
		t.Errorf("evicted_keys = %d under noeviction, want 0", got)
	}
}

func TestEvictLoopBoundsMemory(t *testing.T) {
	// The after-task loop (not just EvictNow) must keep the shard under
	// quota on a sustained write stream — the M5d soak's shape in small.
	e := fillEvict(PolicyAllKeysLRU, 2000)
	defer e.Close()
	for i := range 100 {
		storeStrLen(e, 0, fmt.Sprintf("s%03d", i), 100)
	}
	if got := e.UsedBytes(); got > 2000 {
		t.Errorf("usedBytes = %d after 100 writes over a 2000 quota — the post-task eviction loop fell behind", got)
	}
	if got := e.EvictedKeys.Load(); got < 50 {
		t.Errorf("evicted_keys = %d, want most of the 100 writes evicted", got)
	}
	// The freshest writes survive an LRU soak.
	if !exists(t, e, 0, "s099") || !exists(t, e, 0, "s098") {
		t.Error("freshest keys missing after LRU soak")
	}
}

func TestFlushDBPurgesEvictionTrackers(t *testing.T) {
	// The M5d soak's failure shape: FlushDB swaps the keyspace wholesale,
	// so every tracker entry goes stale at once; without the flushDB purge
	// the eviction budget (2*EvictSamples pops per victim attempt) is spent
	// discarding the backlog, eviction stalls, and the OOM gate wedges.
	e := NewEngine(1, 1)
	defer e.Close()
	e.SetPolicy(PolicyAllKeysLRU)
	now := e.NowMs()
	for i := range 2000 {
		storeStrLen(e, 0, fmt.Sprintf("old%04d", i), 30)
	}
	// Some TTL'd keys too, so the expiry heap carries stale items across
	// the flush (volatile-ttl's victim loop has the same small stale
	// budget as the LRU pops).
	for i := range 50 {
		k := fmt.Sprintf("oldttl%04d", i)
		e.Do(0, []byte(k), func(s *Shard) {
			ent := &Entry{Type: TypeString, Str: make([]byte, 30), ExpireAtMs: now + 600000}
			s.Store(0, k, ent)
			s.PushExpire(0, k, ent)
		})
	}
	e.FlushDB(0)
	e.Do(0, []byte("x"), func(s *Shard) {
		if n := s.lruAll.Len(); n != 0 {
			t.Errorf("lruAll.Len after FlushDB = %d, want 0 (purge)", n)
		}
		if n := s.lfuAll.Len(); n != 0 {
			t.Errorf("lfuAll.Len after FlushDB = %d, want 0 (purge)", n)
		}
		if n := s.exp.Len(); n != 0 {
			t.Errorf("expiry heap Len after FlushDB = %d, want 0 (purge)", n)
		}
	})
	// Refill past the quota: eviction must keep up against live keys.
	e.SetMaxMemory(2000) // ~17 keys of 30+7+80 bytes
	for i := range 40 {
		storeStrLen(e, 0, fmt.Sprintf("new%04d", i), 30)
	}
	if stillOver := e.EvictNow(0); stillOver {
		t.Fatal("EvictNow still over quota after a FlushDB refill — the stale tracker backlog wedged eviction")
	}
	if got := e.UsedBytes(); got > 2000 {
		t.Errorf("usedBytes = %d after refill eviction, want <= 2000", got)
	}
	// The freshest post-flush writes survive (LRU order is over
	// post-flush writes only — no stale duplicates of live keys).
	if !exists(t, e, 0, "new0039") || !exists(t, e, 0, "new0038") {
		t.Error("freshest post-flush keys missing after eviction")
	}
}
