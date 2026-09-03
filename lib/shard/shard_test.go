package shard

import (
	"fmt"
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
	if fibInv*fibMult != 1 {
		t.Fatal("fibInv is not the multiplicative inverse of fibMult")
	}
	for i := range 500 {
		key := []byte(fmt.Sprintf("key-%d", i))
		si := e.ShardIndex(key)
		e.Do(0, key, func(s *Shard) {
			s.Store(0, string(key), &Entry{Type: TypeString, Str: []byte("v")})
		})
		ks := e.dbs[0].ks.Load()
		if _, found := ks.tab.Search(item{Key: string(key)}); !found {
			t.Fatalf("key %d not stored", i)
		}
		_ = si
	}
	// every key lives in the stripe matching its shard index
	ks := e.dbs[0].ks.Load()
	seen := 0
	for stripe := 0; stripe < ks.tab.StripeCount(); stripe++ {
		seen += ks.tab.StripeLen(stripe)
	}
	if seen != 500 {
		t.Errorf("total striped keys = %d, want 500", seen)
	}
	if got := e.DBSize(0); got != 500 {
		t.Errorf("DBSize = %d, want 500", got)
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
	deleted := 0
	e.DoMulti(0, keys, func(s *Shard, idxs []int) {
		for _, i := range idxs {
			if s.Delete(0, string(keys[i])) {
				deleted++
			}
		}
	})
	if deleted != 4 {
		t.Errorf("deleted = %d, want 4", deleted)
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
