// Package shard is Ultima's sharded keyspace engine (design doc §4.1,
// decisions D1/D4/D5/D13).
//
// The keyspace of every logical DB lives in one pluto
// sharded_hash_ts.ShardedHash whose stripe count equals the shard count.
// The element hash is arranged (via the multiplicative inverse of the
// table's Fibonacci routing multiplier) so that stripe i holds exactly the
// keys with crc64(key) & (N-1) == i — i.e. stripe i is shard i, so
// per-shard views (StripeLen for DBSIZE, the stripe cursor inside Scan)
// derive directly from the striping, and each shard goroutine is the only
// writer touching its stripe (the table's internal stripe locks are
// uncontended on the hot path).
//
// Every command that touches data runs inside the owning shard's owner
// goroutine via Engine.Do / Engine.DoMulti: per-key serialization with no
// caller-side locks. Expiry is passive (checked on every access) plus an
// exact per-shard min-heap (heap_ts) on expire-at, swept periodically by
// the shard goroutine (§5.2).
package shard

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pschlump/pluto/crc"
	"github.com/pschlump/pluto/heap_ts"
	"github.com/pschlump/pluto/sharded_hash_ts"
)

// Type tags a keyspace entry's value kind. M1 implements strings only;
// later milestones add hash/list/set/zset/stream.
type Type byte

// Entry types.
const (
	TypeString Type = 's'
)

// Entry is one keyspace value: type tag, value, expiry, and the version
// counter (WATCH in M3; ExpGen invalidates stale expiry-heap entries).
type Entry struct {
	Type       Type
	Str        []byte
	ExpireAtMs int64 // absolute expiry in ms; 0 = no expiry
	ExpGen     uint64
	Version    uint64
}

// item is the sharded_hash_ts element: a two-field struct whose
// equality/hash read only Key (the documented map pattern).
type item struct {
	Key string
	E   *Entry
}

// expItem is one expiry-heap element. Gen matches Entry.ExpGen at push
// time; a popped element whose Gen/At no longer matches the stored entry
// is stale and discarded without deleting the key.
type expItem struct {
	At  int64
	DB  int
	Key string
	Gen uint64
}

func cmpExpItem(a, b expItem) int {
	switch {
	case a.At < b.At:
		return -1
	case a.At > b.At:
		return 1
	case a.DB != b.DB:
		return a.DB - b.DB
	case a.Key < b.Key:
		return -1
	case a.Key > b.Key:
		return 1
	default:
		return 0
	}
}

// fibMult is sharded_hash_ts's stripe-routing multiplier; fibInv is its
// multiplicative inverse mod 2^64 (fibMult is odd, so the inverse exists).
// sharded_hash_ts routes an element to stripe (hash*fibMult) >> (64-k);
// feeding hash = fibInv * rotl64(crc, k) makes the stripe equal the low k
// bits of the CRC — exactly shard routing (crc & (N-1)).
const fibMult = uint64(0x9E3779B97F4A7C15)

var fibInv = modInverse64(fibMult)

func modInverse64(a uint64) uint64 {
	x := a
	for range 6 {
		x *= 2 - a*x
	}
	return x
}

// keyspace is one logical DB's table; FLUSHDB swaps the pointer.
type keyspace struct {
	tab *sharded_hash_ts.ShardedHash[item]
}

// db is one logical DB (SELECT 0..maxDBs-1, §13.3).
type db struct {
	ks atomic.Pointer[keyspace]
}

// Engine owns the shards and the per-DB keyspaces.
type Engine struct {
	n     int // shard count, a power of two
	k     uint
	tab   *crc.Table64
	shard []*Shard
	dbs   []*db

	// NowMs returns the current time in ms; replaceable in tests.
	NowMs func() int64

	// ExpiredKeys counts keys removed by expiry (passive or sweep).
	ExpiredKeys atomic.Int64
}

// NewEngine builds an engine with shardCount shards (rounded up to a
// power of two; 0 selects 4×GOMAXPROCS via ResolveShardCount) and maxDBs
// logical DBs, and starts the shard goroutines.
func NewEngine(shardCount, maxDBs int) *Engine {
	n := ResolveShardCount(shardCount)
	if maxDBs < 1 {
		maxDBs = 16
	}
	e := &Engine{
		n:     n,
		k:     uint(bits.Len(uint(n)) - 1),
		tab:   crc.MakeTable64(crc.ISO), // any stable CRC-64 is fine for routing
		NowMs: func() int64 { return time.Now().UnixMilli() },
	}
	e.dbs = make([]*db, maxDBs)
	for i := range e.dbs {
		d := &db{}
		d.ks.Store(e.newKeyspace())
		e.dbs[i] = d
	}
	e.shard = make([]*Shard, n)
	for i := range e.shard {
		e.shard[i] = newShard(e, i)
	}
	for _, s := range e.shard {
		s.start()
	}
	return e
}

// ResolveShardCount normalizes the configured shard count: 0 (auto) picks
// 4×GOMAXPROCS, and any value rounds up to a power of two (§4.1).
func ResolveShardCount(cfg int) int {
	if cfg <= 0 {
		cfg = 4 * runtime.GOMAXPROCS(0)
	}
	n := 1
	for n < cfg {
		n <<= 1
	}
	return n
}

// ShardCount returns the number of shards.
func (e *Engine) ShardCount() int { return e.n }

// MaxDBs returns the number of logical DBs.
func (e *Engine) MaxDBs() int { return len(e.dbs) }

// ShardIndex routes a key to its shard: crc64(key) & (N-1).
func (e *Engine) ShardIndex(key []byte) int {
	return int(crc.Checksum64(key, e.tab) & uint64(e.n-1))
}

func (e *Engine) newKeyspace() *keyspace {
	k := e.k
	tab := e.tab
	return &keyspace{
		tab: sharded_hash_ts.NewShardedHashFunc(
			func(a, b item) bool { return a.Key == b.Key },
			func(a item) uint64 {
				c := crc.Checksum64([]byte(a.Key), tab)
				return fibInv * bits.RotateLeft64(c, int(k))
			},
			e.n, 0, 0.75,
		),
	}
}

// Close stops every shard goroutine. No tasks may be submitted after Close.
func (e *Engine) Close() {
	for _, s := range e.shard {
		s.stop()
	}
}

// task is one unit of work for a shard goroutine.
type task struct {
	fn   func(s *Shard)
	done chan struct{}
}

// Do runs fn inside the goroutine of the shard owning key, synchronously.
// Callers may not submit on a closed engine.
func (e *Engine) Do(_ int, key []byte, fn func(s *Shard)) {
	e.DoShard(e.ShardIndex(key), fn)
}

// DoShard runs fn inside shard i's goroutine, synchronously.
func (e *Engine) DoShard(i int, fn func(s *Shard)) {
	s := e.shard[i]
	done := make(chan struct{})
	s.queue <- task{fn: fn, done: done}
	<-done
}

// DoMulti runs fn once per shard over the keys hashing to it (§4.2 fast
// path): same-shard keys execute as one task; cross-shard runs as joined
// per-shard tasks (per-shard atomicity only). idxs are the positions of
// the shard's keys within the original keys slice.
func (e *Engine) DoMulti(_ int, keys [][]byte, fn func(s *Shard, idxs []int)) {
	groups := make(map[int][]int, 4)
	for i, k := range keys {
		si := e.ShardIndex(k)
		groups[si] = append(groups[si], i)
	}
	if len(groups) == 1 {
		for si, idxs := range groups {
			e.DoShard(si, func(s *Shard) { fn(s, idxs) })
		}
		return
	}
	var wg sync.WaitGroup
	for si, idxs := range groups {
		wg.Add(1)
		go func(si int, idxs []int) {
			defer wg.Done()
			e.DoShard(si, func(s *Shard) { fn(s, idxs) })
		}(si, idxs)
	}
	wg.Wait()
}

// FlushDB drops every key in one logical DB by swapping in a fresh
// keyspace (§13.3); in-flight sweeps discard their now-stale heap items.
func (e *Engine) FlushDB(db int) {
	e.dbs[db].ks.Store(e.newKeyspace())
}

// FlushAll drops every key in every logical DB.
func (e *Engine) FlushAll() {
	for i := range e.dbs {
		e.FlushDB(i)
	}
}

// Scan returns up to count live keys of db and the next cursor (0 = done),
// via the table's rehash-safe cursor scan. Expired-but-unswept entries are
// filtered out of the result (not deleted — deletion is the owner's job).
func (e *Engine) Scan(db int, cursor uint64, count int) (keys []string, next uint64) {
	ks := e.dbs[db].ks.Load()
	items, next := ks.tab.Scan(cursor, count)
	now := e.NowMs()
	keys = make([]string, 0, len(items))
	for _, it := range items {
		if it.E.ExpireAtMs > 0 && it.E.ExpireAtMs <= now {
			continue
		}
		keys = append(keys, it.Key)
	}
	return keys, next
}

// DBSize returns the number of live keys in db: every shard sweeps its due
// expiries, then per-shard stripe lengths (stripe i == shard i) are summed.
func (e *Engine) DBSize(db int) int {
	var total atomic.Int64
	var wg sync.WaitGroup
	for i := range e.shard {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.DoShard(i, func(s *Shard) {
				s.sweep(e.NowMs(), -1)
				ks := e.dbs[db].ks.Load()
				total.Add(int64(ks.tab.StripeLen(i)))
			})
		}(i)
	}
	wg.Wait()
	return int(total.Load())
}

// DBStats returns (keys, expires) per non-empty DB for INFO keyspace.
func (e *Engine) DBStats() [][3]int {
	out := make([][3]int, 0, len(e.dbs))
	for i := range e.dbs {
		ks := e.dbs[i].ks.Load()
		n := ks.tab.Len()
		if n == 0 {
			continue
		}
		exp := 0
		now := e.NowMs()
		ks.tab.Walk(func(_ int, it item) bool {
			if it.E.ExpireAtMs > 0 && it.E.ExpireAtMs > now {
				exp++
			}
			return true
		})
		out = append(out, [3]int{i, n, exp})
	}
	return out
}

// Shard is one keyspace shard: an owner goroutine draining a command
// queue, plus the shard's expiry min-heap.
type Shard struct {
	eng *Engine
	id  int

	queue chan task
	quit  chan struct{}
	done  chan struct{}

	exp *heap_ts.Heap[expItem]

	// SweepInterval is the active-expiry period; SweepMax bounds pops per
	// periodic sweep (time-boxed active expiry, §5.2).
	SweepInterval time.Duration
	SweepMax      int
}

func newShard(eng *Engine, id int) *Shard {
	return &Shard{
		eng:           eng,
		id:            id,
		queue:         make(chan task, 4096),
		quit:          make(chan struct{}),
		done:          make(chan struct{}),
		exp:           heap_ts.NewHeapFunc(cmpExpItem),
		SweepInterval: 100 * time.Millisecond,
		SweepMax:      1000,
	}
}

func (s *Shard) start() {
	go s.run()
}

func (s *Shard) stop() {
	close(s.quit)
	<-s.done
}

func (s *Shard) run() {
	defer close(s.done)
	tick := time.NewTicker(s.SweepInterval)
	defer tick.Stop()
	for {
		select {
		case t := <-s.queue:
			t.fn(s)
			close(t.done)
		case <-tick.C:
			s.sweep(s.eng.NowMs(), s.SweepMax)
		case <-s.quit:
			return
		}
	}
}

// --- keyspace helpers: call only from inside the shard goroutine ---

func (s *Shard) tab(db int) *sharded_hash_ts.ShardedHash[item] {
	return s.eng.dbs[db].ks.Load().tab
}

// Lookup fetches the entry for key, applying passive expiry: a live
// expired entry is deleted and reported missing (§5.2).
func (s *Shard) Lookup(db int, key string) (*Entry, bool) {
	it, found := s.tab(db).Search(item{Key: key})
	if !found {
		return nil, false
	}
	e := it.E
	if e.ExpireAtMs > 0 && e.ExpireAtMs <= s.eng.NowMs() {
		s.tab(db).Delete(item{Key: key})
		s.eng.ExpiredKeys.Add(1)
		return nil, false
	}
	return e, true
}

// Store inserts or replaces key with e. The caller builds e (including any
// expiry) and calls PushExpire after Store when e has an expiry.
func (s *Shard) Store(db int, key string, e *Entry) {
	e.Version++
	s.tab(db).Insert(item{Key: key, E: e})
}

// Delete removes key, returning whether it existed (passively expired
// keys count as missing).
func (s *Shard) Delete(db int, key string) bool {
	if _, ok := s.Lookup(db, key); !ok {
		return false
	}
	return s.tab(db).Delete(item{Key: key})
}

// PushExpire schedules e's expiry in the shard heap, bumping ExpGen so
// older heap entries for the key become stale.
func (s *Shard) PushExpire(db int, key string, e *Entry) {
	e.ExpGen++
	s.exp.Push(expItem{At: e.ExpireAtMs, DB: db, Key: key, Gen: e.ExpGen})
}

// sweep pops due expiry-heap entries and deletes their keys, discarding
// stale entries. limit bounds pops (negative = unbounded).
func (s *Shard) sweep(now int64, limit int) int {
	popped := 0
	for limit < 0 || popped < limit {
		top, ok := s.exp.Peek()
		if !ok || top.At > now {
			return popped
		}
		s.exp.Pop()
		popped++
		it, found := s.tab(top.DB).Search(item{Key: top.Key})
		if !found {
			continue
		}
		e := it.E
		if e.ExpireAtMs == top.At && e.ExpGen == top.Gen {
			s.tab(top.DB).Delete(item{Key: top.Key})
			s.eng.ExpiredKeys.Add(1)
		}
	}
	return popped
}
