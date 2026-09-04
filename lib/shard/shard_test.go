package shard

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestResolveShardCount(t *testing.T) {
	if n := ResolveShardCount(4); n != 4 {
		t.Errorf("ResolveShardCount(4) = %d", n)
	}
	if n := ResolveShardCount(5); n != 8 {
		t.Errorf("ResolveShardCount(5) = %d", n)
	}
	if n := ResolveShardCount(0); n < 4 || n&(n-1) != 0 {
		t.Errorf("ResolveShardCount(0) = %d, want power of two >= 4", n)
	}
}

func TestStripeAlignment(t *testing.T) {
	// The element hash routes each key to the stripe equal to its shard
	// index, so stripe i is exactly shard i's keyspace view.
	e := NewEngine(8, 16)
	defer e.Close()
	for i := range 500 {
		key := []byte(fmt.Sprintf("key-%d", i))
		e.Do(0, key, func(s *Shard) {
			s.Store(0, string(key), &Entry{Type: TypeString, Str: []byte("v")})
		})
	}
	// every key lives in the stripe matching its shard index
	ks := e.dbs[0].ks.Load()
	perShard := make(map[int]int)
	cursor := uint64(0)
	for {
		items, next := ks.tab.Scan(cursor, 100)
		for _, it := range items {
			perShard[e.ShardIndex([]byte(it.Key))]++
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	seen := 0
	for stripe := 0; stripe < ks.tab.StripeCount(); stripe++ {
		if got, want := ks.tab.StripeLen(stripe), perShard[stripe]; got != want {
			t.Errorf("stripe %d holds %d keys, want %d (keys routing to shard %d)", stripe, got, want, stripe)
		}
		seen += ks.tab.StripeLen(stripe)
	}
	if seen != 500 {
		t.Errorf("total striped keys = %d, want 500", seen)
	}
	if got := e.DBSize(0); got != 500 {
		t.Errorf("DBSize = %d, want 500", got)
	}
}

// TestShardIndexDistribution guards the routing fix: the raw low bits of
// the MSB-first CRC-64 are content-independent for short keys, and the raw
// high bits cluster for structured keys — routing must use the Fibonacci
// product's high bits (see the package doc and note/crc-probe).
func TestShardIndexDistribution(t *testing.T) {
	e := NewEngine(64, 16)
	defer e.Close()
	for _, pattern := range []string{"key-%06d", "user:%d", "a%d", "k%d", "session:%d:tok"} {
		counts := make([]int, e.ShardCount())
		const total = 20000
		for i := range total {
			counts[e.ShardIndex([]byte(fmt.Sprintf(pattern, i)))]++
		}
		lo, hi := total, 0
		for _, c := range counts {
			lo = min(lo, c)
			hi = max(hi, c)
		}
		// perfect would be total/64 ≈ 312 per shard; demand a flat-enough
		// spread (any shard within 4x of another would already beat the old
		// everything-on-shard-0 routing).
		if lo == 0 || hi > 4*lo {
			t.Errorf("pattern %q: shard counts min=%d max=%d, want all non-zero within 4x", pattern, lo, hi)
		}
	}
}

func TestDoAndDoMulti(t *testing.T) {
	e := NewEngine(4, 16)
	defer e.Close()
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	e.DoMulti(0, keys, func(s *Shard, idxs []int) {
		for _, i := range idxs {
			s.Store(0, string(keys[i]), &Entry{Type: TypeString, Str: []byte("v")})
		}
	})
	for _, k := range keys {
		e.Do(0, k, func(s *Shard) {
			if _, ok := s.Lookup(0, string(k)); !ok {
				t.Errorf("key %q missing", k)
			}
		})
	}
	deleted := atomic.Int64{}
	e.DoMulti(0, keys, func(s *Shard, idxs []int) {
		for _, i := range idxs {
			if s.Delete(0, string(keys[i])) {
				deleted.Add(1) // the fan-out runs concurrently across shards
			}
		}
	})
	if deleted.Load() != 4 {
		t.Errorf("deleted = %d, want 4", deleted.Load())
	}
	if got := e.DBSize(0); got != 0 {
		t.Errorf("DBSize after deletes = %d, want 0", got)
	}
}

func TestPassiveAndActiveExpiry(t *testing.T) {
	e := NewEngine(4, 16)
	defer e.Close()
	now := time.Now().UnixMilli()

	// passive: expired entry is invisible via Lookup
	e.Do(0, []byte("p"), func(s *Shard) {
		ent := &Entry{Type: TypeString, Str: []byte("v"), ExpireAtMs: now - 1}
		s.Store(0, "p", ent)
		s.PushExpire(0, "p", ent)
	})
	e.Do(0, []byte("p"), func(s *Shard) {
		if _, ok := s.Lookup(0, "p"); ok {
			t.Error("passive expiry: expired key still visible")
		}
	})

	// active: sweep removes due keys
	e.Do(0, []byte("a"), func(s *Shard) {
		ent := &Entry{Type: TypeString, Str: []byte("v"), ExpireAtMs: now - 1}
		s.Store(0, "a", ent)
		s.PushExpire(0, "a", ent)
	})
	e.Do(0, []byte("a"), func(s *Shard) {
		s.sweep(now, -1)
		if _, ok := s.tab(0).Search(item{Key: "a"}); ok {
			t.Error("active sweep: expired key not removed")
		}
	})

	// stale heap entries do not delete a renewed key
	e.Do(0, []byte("r"), func(s *Shard) {
		ent := &Entry{Type: TypeString, Str: []byte("v"), ExpireAtMs: now - 1}
		s.Store(0, "r", ent)
		s.PushExpire(0, "r", ent)
		ent.ExpireAtMs = now + time.Hour.Milliseconds()
		s.PushExpire(0, "r", ent) // ExpGen bumps; the past-due heap item is stale
	})
	e.Do(0, []byte("r"), func(s *Shard) {
		s.sweep(now, -1)
		if _, ok := s.Lookup(0, "r"); !ok {
			t.Error("stale expiry heap entry deleted a renewed key")
		}
	})
	if e.ExpiredKeys.Load() < 2 {
		t.Errorf("ExpiredKeys = %d, want >= 2", e.ExpiredKeys.Load())
	}
}

func TestFlushAndScan(t *testing.T) {
	e := NewEngine(4, 16)
	defer e.Close()
	for i := range 100 {
		key := fmt.Sprintf("scan-%d", i)
		e.Do(0, []byte(key), func(s *Shard) {
			s.Store(0, key, &Entry{Type: TypeString, Str: []byte("v")})
		})
	}
	// full scan iteration covers all keys
	got := map[string]bool{}
	var cursor uint64
	for {
		keys, next := e.Scan(0, cursor, 10)
		for _, k := range keys {
			got[k] = true
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(got) != 100 {
		t.Errorf("scan covered %d keys, want 100", len(got))
	}
	e.FlushDB(0)
	if got := e.DBSize(0); got != 0 {
		t.Errorf("DBSize after flush = %d, want 0", got)
	}
}

// --- WATCH dirty tracking (§4.2) ---------------------------------------------

func storeStr(e *Engine, db int, key string) {
	e.Do(db, []byte(key), func(s *Shard) {
		s.Store(db, key, &Entry{Type: TypeString, Str: []byte("v")})
	})
}

func watchVersion(e *Engine, db int, key string) (epoch, ver uint64) {
	e.Do(db, []byte(key), func(s *Shard) {
		epoch, ver = s.WatchVersion(db, key)
	})
	return epoch, ver
}

func watchDirty(e *Engine, db int, key string, epoch, ver uint64) (dirty bool) {
	e.Do(db, []byte(key), func(s *Shard) {
		dirty = s.WatchDirty(db, key, epoch, ver)
	})
	return dirty
}

func TestWatchCleanAfterReads(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	storeStr(e, 0, "k")
	epoch, ver := watchVersion(e, 0, "k")
	if ver != 1 {
		t.Fatalf("initial version = %d, want 1", ver)
	}
	// Pure reads (Lookup and WatchVersion itself) never move the version.
	for range 5 {
		e.Do(0, []byte("k"), func(s *Shard) {
			if _, ok := s.Lookup(0, "k"); !ok {
				t.Error("key k missing")
			}
		})
	}
	epoch2, ver2 := watchVersion(e, 0, "k")
	if epoch2 != epoch || ver2 != ver {
		t.Errorf("reads changed watch state: (%d,%d) -> (%d,%d)", epoch, ver, epoch2, ver2)
	}
	if watchDirty(e, 0, "k", epoch, ver) {
		t.Error("dirty right after WatchVersion + reads")
	}
	// A missing key watches as version 0 and stays clean.
	me, mv := watchVersion(e, 0, "absent")
	if mv != 0 {
		t.Errorf("missing key version = %d, want 0", mv)
	}
	if watchDirty(e, 0, "absent", me, mv) {
		t.Error("missing key dirty with no writes")
	}
}

func TestWatchDirtyTouch(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	storeStr(e, 0, "k")
	epoch, ver := watchVersion(e, 0, "k")
	// In-place mutation (no Store) must bump the version via Touch.
	e.Do(0, []byte("k"), func(s *Shard) {
		ent, ok := s.Lookup(0, "k")
		if !ok {
			t.Error("key k missing")
			return
		}
		ent.Str = []byte("v2")
		s.Touch(ent)
		if ent.Version != 2 {
			t.Errorf("version after Touch = %d, want 2", ent.Version)
		}
	})
	if !watchDirty(e, 0, "k", epoch, ver) {
		t.Error("not dirty after in-place Touch")
	}
}

func TestWatchDirtyDelete(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	storeStr(e, 0, "k")
	epoch, ver := watchVersion(e, 0, "k")
	e.Do(0, []byte("k"), func(s *Shard) {
		if !s.Delete(0, "k") {
			t.Error("Delete reported missing")
		}
	})
	if !watchDirty(e, 0, "k", epoch, ver) {
		t.Error("not dirty after delete of watched key")
	}
	// The tombstone reads back as delete-time version + 1; a watcher
	// sampling after the delete is clean.
	epoch2, ver2 := watchVersion(e, 0, "k")
	if ver2 != ver+1 {
		t.Errorf("tombstone version = %d, want %d", ver2, ver+1)
	}
	if watchDirty(e, 0, "k", epoch2, ver2) {
		t.Error("dirty when watching the tombstone itself")
	}
	// Deleting an absent key writes no tombstone and dirties nobody.
	e.Do(0, []byte("k"), func(s *Shard) {
		if s.Delete(0, "k") {
			t.Error("Delete of absent key reported present")
		}
	})
	if watchDirty(e, 0, "k", epoch2, ver2) {
		t.Error("delete of absent key dirtied the watcher")
	}
}

func TestWatchDirtyPassiveExpiry(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	now := time.Now().UnixMilli()
	cur := now
	e.NowMs = func() int64 { return cur }
	e.Do(0, []byte("k"), func(s *Shard) {
		ent := &Entry{Type: TypeString, Str: []byte("v"), ExpireAtMs: cur + 1000}
		s.Store(0, "k", ent)
		s.PushExpire(0, "k", ent)
	})
	epoch, ver := watchVersion(e, 0, "k")
	cur += 2000 // past the expiry; the next access passively deletes
	if !watchDirty(e, 0, "k", epoch, ver) {
		t.Error("not dirty after passive expiry of watched key")
	}
}

func TestWatchDirtySweep(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	now := time.Now().UnixMilli()
	cur := now
	e.NowMs = func() int64 { return cur }
	e.Do(0, []byte("k"), func(s *Shard) {
		ent := &Entry{Type: TypeString, Str: []byte("v"), ExpireAtMs: cur + 1000}
		s.Store(0, "k", ent)
		s.PushExpire(0, "k", ent)
	})
	epoch, ver := watchVersion(e, 0, "k")
	cur += 2000
	e.Do(0, []byte("k"), func(s *Shard) {
		s.sweep(cur, -1)
	})
	if !watchDirty(e, 0, "k", epoch, ver) {
		t.Error("not dirty after sweep-driven expiry of watched key")
	}
}

func TestWatchDirtyRecreate(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	storeStr(e, 0, "k")
	epoch, ver := watchVersion(e, 0, "k") // ver == 1
	e.Do(0, []byte("k"), func(s *Shard) {
		s.Delete(0, "k")
		s.Store(0, "k", &Entry{Type: TypeString, Str: []byte("new")})
	})
	if !watchDirty(e, 0, "k", epoch, ver) {
		t.Error("not dirty after delete+recreate")
	}
	// Store bumps past the tombstone (ver+1 at delete time), so the
	// recreated entry can never alias a version a watcher still holds.
	_, ver2 := watchVersion(e, 0, "k")
	if ver2 != 3 { // tombstone recorded 1+1; recreate stores 2+1
		t.Errorf("recreated version = %d, want 3 (tombstone version + 1)", ver2)
	}
}

func TestWatchTombstoneOverflow(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	// The overflow bumps epochs only on the deleted key's shard, so the
	// watched keys must live on that same shard.
	x := []byte("x")
	si := e.ShardIndex(x)
	var sameShard [][]byte
	for i := 0; len(sameShard) < 2; i++ {
		k := []byte(fmt.Sprintf("w%d", i))
		if e.ShardIndex(k) == si {
			sameShard = append(sameShard, k)
		}
	}
	storeStr(e, 0, string(sameShard[0]))
	storeStr(e, 1, string(sameShard[1]))
	epoch0, ver0 := watchVersion(e, 0, string(sameShard[0]))
	epoch1, ver1 := watchVersion(e, 1, string(sameShard[1]))
	// A zero cap overflows on the first tombstone insert, clearing the
	// map and bumping the epoch of every DB on that shard.
	for i := range e.ShardCount() {
		e.DoShard(i, func(s *Shard) { s.TombCap = 0 })
	}
	storeStr(e, 0, "x")
	e.Do(0, x, func(s *Shard) {
		s.Delete(0, "x")
	})
	if !watchDirty(e, 0, string(sameShard[0]), epoch0, ver0) {
		t.Error("watcher on db 0 not dirty after tombstone overflow")
	}
	if !watchDirty(e, 1, string(sameShard[1]), epoch1, ver1) {
		t.Error("watcher on db 1 not dirty after tombstone overflow")
	}
}

func TestWatchDirtyFlush(t *testing.T) {
	e := NewEngine(4, 4)
	defer e.Close()
	// Same key string in two DBs routes to the same shard, isolating the
	// epoch bump to the flushed DB.
	storeStr(e, 0, "k")
	storeStr(e, 1, "k")
	epoch0, ver0 := watchVersion(e, 0, "k")
	epoch1, ver1 := watchVersion(e, 1, "k")
	e.FlushDB(0)
	if !watchDirty(e, 0, "k", epoch0, ver0) {
		t.Error("watcher on flushed db not dirty")
	}
	if watchDirty(e, 1, "k", epoch1, ver1) {
		t.Error("watcher on other db dirtied by FlushDB")
	}
	e.FlushAll()
	if !watchDirty(e, 1, "k", epoch1, ver1) {
		t.Error("watcher not dirty after FlushAll")
	}
}

// --- Transaction pause (§4.2 strict cross-shard path) ----------------------

// sameShardKeys returns n distinct keys routing to shard 0.
func sameShardKeys(e *Engine, n int) [][]byte {
	var keys [][]byte
	for i := 0; len(keys) < n; i++ {
		k := []byte(fmt.Sprintf("pause-%d", i))
		if e.ShardIndex(k) == 0 {
			keys = append(keys, k)
		}
	}
	return keys
}

// crossShardKeys returns keys covering at least two distinct shards.
func crossShardKeys(e *Engine) [][]byte {
	var keys [][]byte
	seen := map[int]bool{}
	for i := 0; len(seen) < 2; i++ {
		k := []byte(fmt.Sprintf("xpause-%d", i))
		si := e.ShardIndex(k)
		if !seen[si] || len(seen) == 0 {
			seen[si] = true
			keys = append(keys, k)
		}
	}
	return keys
}

func TestPauseBlocksNormalTasks(t *testing.T) {
	e := NewEngine(4, 16)
	defer e.Close()
	_, resume := e.PauseAll()

	// A normal (untokened) task submitted during the pause must block.
	done := make(chan struct{})
	go func() {
		e.Do(0, []byte("blk"), func(s *Shard) {
			s.Store(0, "blk", &Entry{Type: TypeString, Str: []byte("v")})
		})
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("normal task completed while engine was paused")
	case <-time.After(50 * time.Millisecond):
	}
	resume()
	resume() // idempotent: a second call is a no-op
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stashed task did not run after resume")
	}
}

func TestPauseTokenTasksRunAndStashOrder(t *testing.T) {
	e := NewEngine(4, 16)
	defer e.Close()
	keys := sameShardKeys(e, 2)
	tok, resume := e.PauseAll()

	// Tasks carrying the pause token execute while every shard is parked,
	// and see each other's writes.
	e.DoTok(tok, 0, func(s *Shard) {
		s.Store(0, string(keys[0]), &Entry{Type: TypeString, Str: []byte("v")})
	})
	e.DoTok(tok, 0, func(s *Shard) {
		if _, ok := s.Lookup(0, string(keys[0])); !ok {
			t.Error("token task could not see an earlier token task's write")
		}
	})

	// Non-token tasks submitted during the pause are stashed FIFO. Send
	// straight into shard 0's queue (in-package test) so the submission
	// order is deterministic.
	sh := e.shard[0]
	var order []int
	dones := make([]chan struct{}, 3)
	for i := range dones {
		i := i
		dones[i] = make(chan struct{})
		sh.queue <- task{fn: func(_ *Shard) { order = append(order, i) }, done: dones[i]}
	}
	for _, d := range dones {
		select {
		case <-d:
			t.Fatal("stashed task ran during the pause")
		case <-time.After(20 * time.Millisecond):
		}
	}
	resume()
	for _, d := range dones {
		select {
		case <-d:
		case <-time.After(2 * time.Second):
			t.Fatal("stashed task lost on resume")
		}
	}
	if !reflect.DeepEqual(order, []int{0, 1, 2}) {
		t.Errorf("stash replay order = %v, want [0 1 2]", order)
	}
}

func TestDoMultiTokDuringPause(t *testing.T) {
	e := NewEngine(8, 16)
	defer e.Close()
	keys := crossShardKeys(e)
	tok, resume := e.PauseAll()
	defer resume()

	done := make(chan struct{})
	go func() {
		e.DoMultiTok(tok, keys, func(s *Shard, idxs []int) {
			for _, i := range idxs {
				s.Store(0, string(keys[i]), &Entry{Type: TypeString, Str: []byte("v")})
			}
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DoMultiTok across shards did not complete during pause")
	}
	for _, k := range keys {
		e.DoTok(tok, e.ShardIndex(k), func(s *Shard) {
			if _, ok := s.Lookup(0, string(k)); !ok {
				t.Errorf("key %q missing after DoMultiTok", k)
			}
		})
	}
}

func TestPauseAllSerializes(t *testing.T) {
	e := NewEngine(4, 16)
	defer e.Close()
	var wg sync.WaitGroup
	for g := range 2 {
		wg.Add(1)
		go func(shardIdx int) {
			defer wg.Done()
			for range 5 {
				tok, resume := e.PauseAll()
				e.DoTok(tok, shardIdx, func(s *Shard) {
					s.Store(0, "ser", &Entry{Type: TypeString, Str: []byte("v")})
				})
				resume()
			}
		}(g)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent PauseAll deadlocked")
	}
}
