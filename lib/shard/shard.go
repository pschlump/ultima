// Package shard is Ultima's sharded keyspace engine (design doc §4.1,
// decisions D1/D4/D5/D13).
//
// The keyspace of every logical DB lives in one pluto
// sharded_hash_ts.ShardedHash whose stripe count equals the shard count.
// The element hash is the bare crc64(key): the table routes an element to
// stripe (hash*fibMult) >> (64-k) (Fibonacci hashing), which is exactly
// Engine.ShardIndex — so stripe i holds exactly shard i's keys, per-shard
// views (StripeLen for DBSIZE, the stripe cursor inside Scan) derive
// directly from the striping, and each shard goroutine is the only writer
// touching its stripe (the table's internal stripe locks are uncontended
// on the hot path). The multiply is load-bearing: the raw low bits of the
// MSB-first CRC are content-independent for short keys and the raw high
// bits cluster for structured keys ("key-0001"…), while the Fibonacci
// product's high bits avalanche both (probed in note/crc-probe).
//
// Every command that touches data runs inside the owning shard's owner
// goroutine via Engine.Do / Engine.DoMulti: per-key serialization with no
// caller-side locks. Expiry is passive (checked on every access) plus an
// exact per-shard min-heap (heap_ts) on expire-at, swept periodically by
// the shard goroutine (§5.2).
package shard

import (
	"math/bits"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pschlump/pluto/crc"
	"github.com/pschlump/pluto/heap_ts"
	"github.com/pschlump/pluto/lfu_ts"
	"github.com/pschlump/pluto/lru_ts"
	"github.com/pschlump/pluto/sharded_hash_ts"
	"github.com/pschlump/ultima/lib/types"
)

// Type tags a keyspace entry's value kind. M1 implements strings; M2 adds
// the hash/list/set/zset collections (design doc §5.1).
type Type byte

// Entry types.
const (
	TypeString Type = 's'
	TypeHash   Type = 'h'
	TypeList   Type = 'l'
	TypeSet    Type = 'S'
	TypeZSet   Type = 'z'
)

// Entry is one keyspace value: type tag, value, expiry, and the version
// counter (WATCH in M3; ExpGen invalidates stale expiry-heap entries).
// Strings live in Str; collections live in Obj as one of *types.Hash,
// *types.List, *types.Set, *types.ZSet (shard goroutine–owned, no locks).
// memBytes caches the entry's estimated memory cost (key + value +
// overhead, M5b), maintained by Store/Touch/Delete so usedBytes stays
// O(1) per write.
type Entry struct {
	Type       Type
	Str        []byte
	Obj        any
	ExpireAtMs int64 // absolute expiry in ms; 0 = no expiry
	ExpGen     uint64
	Version    uint64
	memBytes   int64
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

// fibMult is sharded_hash_ts's stripe-routing multiplier (Fibonacci
// hashing constant). Feeding the table hash = crc64(key) makes its stripe
// routing (hash*fibMult) >> (64-k) identical to Engine.ShardIndex, so
// stripe i IS shard i by construction.
const fibMult = uint64(0x9E3779B97F4A7C15)

// keyspace is one logical DB's table; FLUSHDB swaps the pointer.
type keyspace struct {
	tab *sharded_hash_ts.ShardedHash[item]
}

// tombKey identifies one deleted key's tombstone slot (WATCH dirty
// tracking, §4.2). The (db, key) pair also indexes the blocking-command
// waiter registry.
type tombKey struct {
	db  int
	key string
}

// Waiter is one parked blocking command (BLPOP…BZMPOP, §4.2): the
// connection goroutine parks selecting on Ch, and a push into the key
// signals it. Ch has capacity 1 and WakeWaiter sends non-blocking, so a
// waiter is signaled at most once per wake and stale signals are
// harmless (waiters always recheck the keyspace after a wake). ID is the
// client/connection ID, for debugging; FIFO slice order in the registry
// is arrival order.
type Waiter struct {
	Ch chan struct{}
	ID uint64
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

	// txMu serializes PauseAll (one EXEC pauses the engine at a time);
	// txTokCounter issues the pause tokens that let the pauser's tasks
	// run while everyone else is stashed (§4.2 strict cross-shard path).
	txMu         sync.Mutex
	txTokCounter atomic.Uint64

	// NowMs returns the current time in ms; replaceable in tests.
	NowMs func() int64

	// ExpiredKeys counts keys removed by expiry (passive or sweep).
	ExpiredKeys atomic.Int64

	// EvictedKeys counts keys removed by maxmemory eviction (M5b).
	EvictedKeys atomic.Int64

	// EvictSamples is the eviction candidate count for the LFU and
	// random policies (Redis maxmemory-samples, DefaultEvictSamples).
	EvictSamples int

	// maxmemory and the eviction policy (M5b): set via CONFIG SET /
	// config file, read by the per-shard eviction loop and the command
	// layer's OOM gate. 0 = unlimited.
	maxMemory atomic.Int64
	policy    atomic.Int32 // EvictPolicy

	// closing is closed at the start of Close, before the shard
	// goroutines stop: parked blocking-command waiters select on it and
	// reply null on shutdown instead of hanging (§4.2).
	closing chan struct{}
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
		n:            n,
		k:            uint(bits.Len(uint(n)) - 1),
		tab:          crc.MakeTable64(crc.ISO), // any stable CRC-64 is fine for routing
		NowMs:        func() int64 { return time.Now().UnixMilli() },
		closing:      make(chan struct{}),
		EvictSamples: DefaultEvictSamples,
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

// ShardIndex routes a key to its shard: (crc64(key)*fibMult) >> (64-k) —
// the same Fibonacci-hash routing sharded_hash_ts applies internally, so
// a key's table stripe always equals its shard (see the package doc).
func (e *Engine) ShardIndex(key []byte) int {
	return int((crc.Checksum64(key, e.tab) * fibMult) >> (64 - e.k))
}

func (e *Engine) newKeyspace() *keyspace {
	tab := e.tab
	return &keyspace{
		tab: sharded_hash_ts.NewShardedHashFunc(
			func(a, b item) bool { return a.Key == b.Key },
			func(a item) uint64 {
				return crc.Checksum64([]byte(a.Key), tab)
			},
			e.n, 0, 0.75,
		),
	}
}

// Close stops every shard goroutine. No tasks may be submitted after Close.
// closing is closed first so parked blocking-command waiters unblock and
// reply null instead of waiting out their timeouts.
func (e *Engine) Close() {
	close(e.closing)
	for _, s := range e.shard {
		s.stop()
	}
}

// Closing returns the channel closed by Close before the shard goroutines
// stop. Parked blocking waiters (BLPOP…BZMPOP) select on it and reply
// null on shutdown; on that path they must NOT submit shard tasks (the
// shards may already be gone), so they skip deregistration.
func (e *Engine) Closing() <-chan struct{} { return e.closing }

// SetOnKeyGone installs fn as every shard's key-gone hook (expiry today,
// eviction in M5b). Call once at engine construction, before serving.
func (e *Engine) SetOnKeyGone(fn func(db int, key, reason string)) {
	for _, s := range e.shard {
		s.OnKeyGone = fn
	}
}

// task is one unit of work for a shard goroutine. tok carries the pause
// token of the transaction the task belongs to (0 = normal task). A nil
// fn is a park/unpark control message from PauseAll, not work: a park
// message (tok != 0, done set) parks the shard after acking done, an
// unpark message (tok == 0, done nil) releases it.
type task struct {
	fn   func(s *Shard)
	done chan struct{}
	tok  uint64
}

// PauseAll parks every shard goroutine so that only tasks carrying the
// returned token execute, and returns a resume function that releases
// them. This is §4.2's strict cross-shard path: a transaction's commands
// run (via DoTok/DoMultiTok) with exclusive access to the whole keyspace
// — the multi-shard equivalent of shard locks taken in shard-id order,
// which is why the park messages go out in ascending shard-id order.
//
// A parked shard keeps serving tasks whose tok matches, and stashes all
// others in a FIFO slice: their done channels are neither closed nor
// dropped, their submitters simply block until resume. resume replays
// each shard's stash in submission order (the shard drains it before
// accepting new queue work), then releases the serialization mutex, so
// resume is the point at which another PauseAll may begin. resume is
// idempotent (safe to defer exactly once; further calls are no-ops).
//
// Deadlock safety rests on two invariants: shard tasks never submit work
// to other shards (only connection goroutines call Do/DoMulti/DoTok), and
// concurrent pauses serialize on txMu. PauseAll itself must therefore
// never be called from inside a shard goroutine, and the pause holder
// must submit its own work only via DoTok/DoMultiTok with tok.
func (e *Engine) PauseAll() (tok uint64, resume func()) {
	e.txMu.Lock()
	tok = e.txTokCounter.Add(1)
	for i := range e.shard {
		ack := make(chan struct{})
		e.shard[i].queue <- task{done: ack, tok: tok} // fn == nil: park
		<-ack                                         // acked after all tasks queued ahead of the park have run
	}
	var once sync.Once
	resume = func() {
		once.Do(func() {
			for i := range e.shard {
				e.shard[i].queue <- task{} // fn == nil, tok == 0: unpark
			}
			e.txMu.Unlock()
		})
	}
	return tok, resume
}

// Do runs fn inside the goroutine of the shard owning key, synchronously.
// Callers may not submit on a closed engine.
func (e *Engine) Do(_ int, key []byte, fn func(s *Shard)) {
	e.DoTok(0, e.ShardIndex(key), fn)
}

// DoShard runs fn inside shard i's goroutine, synchronously.
func (e *Engine) DoShard(i int, fn func(s *Shard)) {
	e.DoTok(0, i, fn)
}

// DoTok is DoShard with an explicit pause token: tok == 0 is exactly
// DoShard, while a token from PauseAll lets the task run inside a parked
// shard. Callers with a nonzero tok must hold that pause.
func (e *Engine) DoTok(tok uint64, i int, fn func(s *Shard)) {
	s := e.shard[i]
	done := make(chan struct{})
	s.queue <- task{fn: fn, done: done, tok: tok}
	<-done
}

// DoMulti runs fn once per shard over the keys hashing to it (§4.2 fast
// path): same-shard keys execute as one task; cross-shard runs as joined
// per-shard tasks (per-shard atomicity only). idxs are the positions of
// the shard's keys within the original keys slice.
//
// Concurrency: with keys on multiple shards, fn runs CONCURRENTLY on
// several shard goroutines. Any shared local written by fn must be
// synchronized (sync/atomic, mutex) — or slot-indexed by original key
// position: every position appears in exactly one group's idxs exactly
// once (duplicate keys hash to the same shard), so out[i] writes never
// alias across goroutines.
func (e *Engine) DoMulti(_ int, keys [][]byte, fn func(s *Shard, idxs []int)) {
	e.DoMultiTok(0, keys, fn)
}

// DoMultiTok is DoMulti with an explicit pause token (see DoTok). The
// concurrency caveat on DoMulti applies.
func (e *Engine) DoMultiTok(tok uint64, keys [][]byte, fn func(s *Shard, idxs []int)) {
	groups := make(map[int][]int, 4)
	for i, k := range keys {
		si := e.ShardIndex(k)
		groups[si] = append(groups[si], i)
	}
	if len(groups) == 1 {
		for si, idxs := range groups {
			e.DoTok(tok, si, func(s *Shard) { fn(s, idxs) })
		}
		return
	}
	var wg sync.WaitGroup
	for si, idxs := range groups {
		wg.Add(1)
		go func(si int, idxs []int) {
			defer wg.Done()
			e.DoTok(tok, si, func(s *Shard) { fn(s, idxs) })
		}(si, idxs)
	}
	wg.Wait()
}

// FlushDB drops every key in one logical DB by swapping in a fresh
// keyspace (§13.3); in-flight sweeps discard their now-stale heap items.
// After the swap every shard is synchronously notified to drop db's
// tombstones and bump db's epoch, dirtying all watchers on db. Callers
// must not be inside a shard goroutine (the fan-out submits shard tasks).
func (e *Engine) FlushDB(db int) {
	e.dbs[db].ks.Store(e.newKeyspace())
	for i := range e.shard {
		e.DoShard(i, func(s *Shard) { s.flushDB(db) })
	}
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
				s.sweep(e.NowMs(), -1, time.Time{})
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

// ShardStat is the per-shard introspection snapshot for the M6c
// management API (design doc §10.1 GET /api/v1/shards).
//
//nolint:revive // the name pairs with Engine.ShardStats; Stat would stutter less but read worse at call sites
type ShardStat struct {
	Index      int
	Keys       int   // keys owned by this shard, all DBs
	Expires    int   // keys with a TTL (future expiry only)
	MemBytes   int64 // this shard's share of the keyspace estimate (M5b)
	QueueDepth int   // pending tasks in the shard goroutine's queue
	ExpiryHeap int   // pending expiry-heap entries (includes stale gens)
}

// ShardStats snapshots every shard by running a report task on each shard
// goroutine (consistent with the rest of the engine: the shard goroutine
// is the only reader of its tables). Keys/Expires walk the stripes, so
// this is O(keys) — a management-endpoint call, not a hot path.
func (e *Engine) ShardStats() []ShardStat {
	out := make([]ShardStat, e.n)
	var wg sync.WaitGroup
	now := e.NowMs()
	for i := range e.shard {
		wg.Add(1)
		e.DoShard(i, func(s *Shard) {
			defer wg.Done()
			st := ShardStat{
				Index:      s.id,
				MemBytes:   s.usedTotal.Load(),
				QueueDepth: len(s.queue),
				ExpiryHeap: s.exp.Len(),
			}
			for d := range e.dbs {
				tab := s.tab(d)
				st.Keys += tab.StripeLen(s.id)
				tab.StripeWalk(s.id, func(_ int, it item) bool {
					if it.E.ExpireAtMs > now {
						st.Expires++
					}
					return true
				})
			}
			out[s.id] = st
		})
	}
	wg.Wait()
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

	// tomb remembers the version a deleted key had at delete time, +1, so
	// a WATCH that sampled the key before its deletion sees a version
	// mismatch (§4.2 WATCH dirty tracking). Store prunes a key's
	// tombstone on recreate. Bounded by TombCap: on overflow the whole
	// map is dropped and every DB's epoch bumps (conservative
	// invalidation — false-positive aborts only, which WATCH clients
	// must already tolerate).
	tomb map[tombKey]uint64

	// epoch holds one invalidation counter per logical DB; a bump
	// dirties every watcher on this shard+DB (FLUSHDB/FLUSHALL,
	// tombstone overflow).
	epoch []uint64

	// waiters holds, per (db, key), the FIFO of blocking commands parked
	// on that key (§4.2: the wait never occupies a shard goroutine — the
	// command registers interest and parks its connection goroutine;
	// pushes into the key signal the first waiter). Shard-goroutine only.
	waiters map[tombKey][]*Waiter

	// SweepInterval is the base active-expiry period (the cadence relaxes
	// up to this when sweeps find nothing due); SweepMax bounds pops per
	// periodic sweep; SweepTimeBox bounds wall-clock time per sweep;
	// SweepFloor is the fastest cadence under expiry pressure (time-boxed
	// adaptive active expiry, §5.2 — the active_expire_effort analogue).
	SweepInterval time.Duration
	SweepMax      int
	SweepTimeBox  time.Duration
	SweepFloor    time.Duration

	// OnKeyGone, when non-nil, is invoked after the shard deletes a key
	// for a non-command reason — passive expiry in Lookup, active expiry
	// in sweep (reason "expired"), maxmemory eviction (reason "evicted",
	// M5b). Runs on the shard goroutine, so the hook must be cheap and
	// non-blocking. Install once via Engine.SetOnKeyGone before serving.
	OnKeyGone func(db int, key, reason string)

	// usedBytes is the shard's share of the keyspace memory estimate
	// (M5b), one counter per logical DB so FLUSHDB can zero its share,
	// plus usedTotal as the shard-wide rollup for the hot paths (the OOM
	// gate and the eviction loop read it per command). Written only by
	// the shard goroutine; read anywhere via the atomics.
	usedBytes []atomic.Int64
	usedTotal atomic.Int64

	// Eviction recency/frequency trackers (M5b, D14): lruAll/lfuAll hold
	// every key, lruVol/lfuVol only keys with a TTL. Maintained by
	// Lookup/Store/Touch/PushExpire/Delete below — always on, like
	// Redis's per-object LRU clock, so CONFIG SET maxmemory-policy never
	// starts cold. Stale entries (deleted/flushed keys) are self-healing:
	// the eviction loop validates every candidate against the keyspace.
	// Keys are tombKey{db,key} — the same (db,key) pair the tombstones
	// and the waiter registry use. Shard-goroutine only, so the _ts
	// locks are uncontended.
	lruAll *lru_ts.Lru[tombKey, struct{}]
	lruVol *lru_ts.Lru[tombKey, struct{}]
	lfuAll *lfu_ts.Lfu[tombKey]
	lfuVol *lfu_ts.Lfu[tombKey]

	// rng feeds victim sampling (random/LFU policies). Shard-goroutine
	// only; seeded per shard so tests are reproducible enough.
	rng *rand.Rand

	// TombCap bounds the tombstone map; shrunk by tests.
	TombCap int
}

func newShard(eng *Engine, id int) *Shard {
	return &Shard{
		eng:           eng,
		id:            id,
		queue:         make(chan task, 4096),
		quit:          make(chan struct{}),
		done:          make(chan struct{}),
		exp:           heap_ts.NewHeapFunc(cmpExpItem),
		tomb:          make(map[tombKey]uint64),
		epoch:         make([]uint64, eng.MaxDBs()),
		waiters:       make(map[tombKey][]*Waiter),
		usedBytes:     make([]atomic.Int64, eng.MaxDBs()),
		lruAll:        lru_ts.NewLru[tombKey, struct{}](evictTrackerCap),
		lruVol:        lru_ts.NewLru[tombKey, struct{}](evictTrackerCap),
		lfuAll:        lfu_ts.NewLfu[tombKey](lfu_ts.DefaultLogFactor, lfu_ts.DefaultDecayTime),
		lfuVol:        lfu_ts.NewLfu[tombKey](lfu_ts.DefaultLogFactor, lfu_ts.DefaultDecayTime),
		rng:           rand.New(rand.NewSource(int64(id)*7919 + 42)),
		SweepInterval: 100 * time.Millisecond,
		SweepMax:      1000,
		SweepTimeBox:  time.Millisecond,
		SweepFloor:    10 * time.Millisecond,
		TombCap:       65536,
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
	interval := s.SweepInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case t := <-s.queue:
			if t.fn == nil {
				// Park control message (PauseAll, §4.2): ack, then
				// serve only the pauser's tasks until unparked.
				close(t.done)
				s.park(t.tok)
			} else {
				t.fn(s)
				close(t.done)
				s.maybeEvict()
			}
		case <-timer.C:
			expired, more := s.sweep(s.eng.NowMs(), s.SweepMax, time.Now().Add(s.SweepTimeBox))
			interval = s.nextSweepInterval(interval, expired, more)
			timer.Reset(interval)
		case <-s.quit:
			return
		}
	}
}

// nextSweepInterval adapts the sweep cadence (§5.2): while a sweep stops
// short with due keys still in the heap (more), halve toward SweepFloor;
// when a sweep finds nothing due, relax back toward SweepInterval.
// Call only from run.
func (s *Shard) nextSweepInterval(cur time.Duration, expired int, more bool) time.Duration {
	switch {
	case more:
		if cur/2 > s.SweepFloor {
			return cur / 2
		}
		return s.SweepFloor
	case expired == 0:
		if cur*2 < s.SweepInterval {
			return cur * 2
		}
		return s.SweepInterval
	default:
		return cur
	}
}

// park is the shard's paused inner loop (§4.2 strict cross-shard path):
// tasks carrying tok execute immediately; every other task is stashed in
// a FIFO slice with its done channel left open — its submitter blocks,
// the work is neither dropped nor reordered. Expiry sweeps are skipped
// for the duration. An unpark message (fn == nil, tok == 0) replays the
// stash in submission order before the normal loop resumes, so no new
// queue work can overtake stashed tasks. Shutdown during a pause drains
// the stash the same way so blocked callers never hang. Call only from
// run.
func (s *Shard) park(tok uint64) {
	var stash []task
	drain := func() {
		for _, st := range stash {
			st.fn(s)
			close(st.done)
			s.maybeEvict()
		}
	}
	for {
		select {
		case t := <-s.queue:
			if t.fn == nil { // unpark
				drain()
				return
			}
			if t.tok == tok {
				t.fn(s)
				close(t.done)
				s.maybeEvict()
			} else {
				stash = append(stash, t)
			}
		case <-s.quit:
			drain()
			return
		}
	}
}

// --- keyspace helpers: call only from inside the shard goroutine ---

func (s *Shard) tab(db int) *sharded_hash_ts.ShardedHash[item] {
	return s.eng.dbs[db].ks.Load().tab
}

// Lookup fetches the entry for key, applying passive expiry: a live
// expired entry is deleted (and tombstoned for WATCH) and reported
// missing (§5.2). A hit counts as an access for the M5b eviction
// trackers (LRU recency / LFU frequency), like Redis's object access.
func (s *Shard) Lookup(db int, key string) (*Entry, bool) {
	it, found := s.tab(db).Search(item{Key: key})
	if !found {
		return nil, false
	}
	e := it.E
	if e.ExpireAtMs > 0 && e.ExpireAtMs <= s.eng.NowMs() {
		s.tombstone(db, key, e.Version)
		s.tab(db).Delete(item{Key: key})
		s.addMem(db, -e.memBytes)
		s.untrack(db, key)
		s.eng.ExpiredKeys.Add(1)
		if s.OnKeyGone != nil {
			s.OnKeyGone(db, key, "expired")
		}
		return nil, false
	}
	s.trackAccess(db, key, e)
	return e, true
}

// addMem adjusts db's memory estimate and the shard-wide rollup by delta
// (negative on removal). Call only from inside the shard goroutine.
func (s *Shard) addMem(db int, delta int64) {
	s.usedBytes[db].Add(delta)
	s.usedTotal.Add(delta)
}

// trackAccess records one keyspace access in the M5b eviction trackers:
// recency in the allkeys/volatile LRU, frequency in the LFU counters
// (Redis's per-object LRU/LFU clock update analogue). Call only from
// inside the shard goroutine.
func (s *Shard) trackAccess(db int, key string, e *Entry) {
	k := tombKey{db, key}
	s.lruAll.Put(k, struct{}{})
	s.lfuAll.Touch(k)
	if e.ExpireAtMs > 0 {
		s.lruVol.Put(k, struct{}{})
		s.lfuVol.Touch(k)
	}
}

// untrack drops key from every eviction tracker (key deleted or expiry
// removed). Call only from inside the shard goroutine.
func (s *Shard) untrack(db int, key string) {
	k := tombKey{db, key}
	s.lruAll.Delete(k)
	s.lruVol.Delete(k)
	s.lfuAll.Delete(k)
	s.lfuVol.Delete(k)
}

// entryMem estimates one entry's memory cost in bytes: the key, the
// entry/overhead share, and the value (string bytes or the collection's
// MemUsage). An estimate, per D10 — monotone and comparable, not exact.
func entryMem(key string, e *Entry) int64 {
	const entryOverhead = 80 // Entry + table node + bookkeeping share
	v := int64(len(key)) + entryOverhead
	switch e.Type {
	case TypeString:
		v += int64(len(e.Str))
	case TypeHash:
		v += e.Obj.(*types.Hash).MemUsage()
	case TypeList:
		v += e.Obj.(*types.List).MemUsage()
	case TypeSet:
		v += e.Obj.(*types.Set).MemUsage()
	case TypeZSet:
		v += e.Obj.(*types.ZSet).MemUsage()
	}
	return v
}

// Store inserts or replaces key with e. The caller builds e (including any
// expiry) and calls PushExpire after Store when e has an expiry. The
// version bump makes the write visible to WATCH; recreating a deleted key
// prunes its tombstone. The new version must exceed anything a watcher
// could have sampled for this key — a plain e.Version++ would resurrect a
// replaced or delete-recreated key at a version a watcher may still hold,
// hiding the write — so the bump is relative to the greater of the
// previous entry's version and the tombstone's.
func (s *Shard) Store(db int, key string, e *Entry) {
	base := s.tomb[tombKey{db, key}]
	old, found := s.tab(db).Search(item{Key: key})
	if found && old.E.Version > base {
		base = old.E.Version
	}
	e.Version = base + 1
	e.memBytes = entryMem(key, e)
	if found {
		s.addMem(db, e.memBytes-old.E.memBytes)
	} else {
		s.addMem(db, e.memBytes)
	}
	delete(s.tomb, tombKey{db, key})
	s.tab(db).Insert(item{Key: key, E: e})
	s.trackAccess(db, key, e)
	if e.ExpireAtMs == 0 {
		// An overwrite drops any TTL (Redis SET semantics): the volatile
		// trackers must not keep the key.
		k := tombKey{db, key}
		s.lruVol.Delete(k)
		s.lfuVol.Delete(k)
	}
}

// Delete removes key, returning whether it existed (passively expired
// keys count as missing). A real removal leaves a tombstone so WATCH
// sees the delete.
func (s *Shard) Delete(db int, key string) bool {
	e, ok := s.Lookup(db, key)
	if !ok {
		return false
	}
	s.tombstone(db, key, e.Version)
	s.addMem(db, -e.memBytes)
	s.untrack(db, key)
	return s.tab(db).Delete(item{Key: key})
}

// Touch bumps e's version after an in-place mutation of a live entry
// (INCR, HSET, list pushes, ZADD… do not go through Store, but §4.2
// WATCH must still observe the write). It also refreshes the cached
// memory estimate (the mutation changed the value's size) and records
// the access in the eviction trackers. Call only from inside the shard
// goroutine.
func (s *Shard) Touch(db int, key string, e *Entry) {
	e.Version++
	mem := entryMem(key, e)
	s.addMem(db, mem-e.memBytes)
	e.memBytes = mem
	s.trackAccess(db, key, e)
}

// tombstone records key's deletion for WATCH dirty tracking: the version
// the key had at delete time, +1. Over TombCap the map is cleared and
// every DB's epoch bumps (conservative: all watchers on this shard go
// dirty). Call only from inside the shard goroutine.
func (s *Shard) tombstone(db int, key string, ver uint64) {
	if len(s.tomb) >= s.TombCap {
		clear(s.tomb)
		for i := range s.epoch {
			s.epoch[i]++
		}
	}
	s.tomb[tombKey{db, key}] = ver + 1
}

// flushDB drops db's tombstones, zeroes db's memory-estimate share (the
// keyspace swap already dropped the entries), purges db's eviction-tracker
// entries, and bumps db's epoch, dirtying every watcher on db. The purge
// matters: the keyspace swap makes every tracked (db,key) stale at once,
// and the eviction loops' stale-entry self-healing is budgeted for a
// handful of stragglers (2*EvictSamples pops per victim attempt) — a whole
// flushed DB's worth of stale entries would exhaust that budget, stall
// eviction, and wedge the OOM gate (the M5d soak's post-FLUSHALL OOM
// storm); a stale duplicate could also evict a re-inserted LIVE key.
// Call only from inside the shard goroutine.
func (s *Shard) flushDB(db int) {
	for k := range s.tomb {
		if k.db == db {
			delete(s.tomb, k)
		}
	}
	s.addMem(db, -s.usedBytes[db].Load())
	s.purgeTrackers(db)
	s.epoch[db]++
}

// purgeTrackers drops db's entries from every eviction tracker. The LRU
// iterator is a snapshot and the LFU Keys list is a copy, so deleting
// while ranging is safe. O(tracked keys) — FlushDB itself is O(1) via the
// keyspace swap, but Redis's FLUSHDB is O(N) too (or lazy with a later
// cost), so the linear purge is in-family.
// Call only from inside the shard goroutine.
func (s *Shard) purgeTrackers(db int) {
	for k := range s.lruAll.All() {
		if k.db == db {
			s.lruAll.Delete(k)
		}
	}
	for k := range s.lruVol.All() {
		if k.db == db {
			s.lruVol.Delete(k)
		}
	}
	for _, k := range s.lfuAll.Keys() {
		if k.db == db {
			s.lfuAll.Delete(k)
		}
	}
	for _, k := range s.lfuVol.Keys() {
		if k.db == db {
			s.lfuVol.Delete(k)
		}
	}
	// The expiry heap goes stale the same way (every db item's key is
	// gone); volatile-ttl's victim loop has the same small stale budget as
	// the LRU pops, so rebuild the heap without db's items rather than
	// leaving it to self-heal.
	var keep []expItem
	for it := range s.exp.All() {
		if it.DB != db {
			keep = append(keep, it)
		}
	}
	s.exp.Truncate()
	for _, it := range keep {
		s.exp.Push(it)
	}
}

// WatchVersion samples key's WATCH state (§4.2): the shard's current
// epoch for db and the key's version — the live entry's Version, else
// its tombstone version, else 0. Applies passive expiry via Lookup.
// Call only from inside the shard goroutine.
func (s *Shard) WatchVersion(db int, key string) (epoch, ver uint64) {
	ent, ok := s.Lookup(db, key)
	epoch = s.epoch[db]
	if ok {
		return epoch, ent.Version
	}
	return epoch, s.tomb[tombKey{db, key}]
}

// WatchDirty reports whether key changed since WatchVersion sampled
// (epoch, ver): an epoch bump (flush, tombstone overflow) dirties every
// watcher on the shard+DB; otherwise a live entry's version or a deleted
// key's tombstone must still equal ver. Call only from inside the shard
// goroutine.
func (s *Shard) WatchDirty(db int, key string, epoch, ver uint64) bool {
	if epoch != s.epoch[db] {
		return true
	}
	if ent, ok := s.Lookup(db, key); ok {
		return ent.Version != ver
	}
	return s.tomb[tombKey{db, key}] != ver
}

// --- blocking-command waiter registry: call only from inside the shard ---
// --- goroutine -----------------------------------------------------------

// AddWaiter appends w to key's waiter FIFO. Callers register one Waiter
// at most once per key per blocking command (duplicate key arguments are
// deduplicated by the caller).
func (s *Shard) AddWaiter(db int, key string, w *Waiter) {
	k := tombKey{db, key}
	s.waiters[k] = append(s.waiters[k], w)
}

// RemoveWaiter drops every occurrence of w from key's waiter FIFO,
// deleting the slot when it empties.
func (s *Shard) RemoveWaiter(db int, key string, w *Waiter) {
	k := tombKey{db, key}
	ws := s.waiters[k]
	out := ws[:0]
	for _, x := range ws {
		if x != w {
			out = append(out, x)
		}
	}
	if len(out) == 0 {
		delete(s.waiters, k)
		return
	}
	s.waiters[k] = out
}

// WakeWaiter signals the first waiter parked on key (non-blocking; the
// waiter's channel caps at one pending signal). Spurious wakes are fine:
// waiters always recheck the keyspace, and a woken waiter that loses the
// race re-registers.
func (s *Shard) WakeWaiter(db int, key string) {
	ws := s.waiters[tombKey{db, key}]
	if len(ws) == 0 {
		return
	}
	select {
	case ws[0].Ch <- struct{}{}:
	default:
	}
}

// PushExpire schedules e's expiry in the shard heap, bumping ExpGen so
// older heap entries for the key become stale.
func (s *Shard) PushExpire(db int, key string, e *Entry) {
	e.ExpGen++
	s.exp.Push(expItem{At: e.ExpireAtMs, DB: db, Key: key, Gen: e.ExpGen})
}

// sweep pops due expiry-heap entries and deletes their keys, discarding
// stale entries. limit bounds pops (negative = unbounded); deadline
// time-boxes the loop (zero value = no deadline) so a shard with a large
// due backlog cannot stall its command queue (§5.2). It returns how many
// keys actually expired and whether due work remained when the loop
// stopped (cap or deadline hit with the heap top still due) — the
// pressure signal for the adaptive cadence in run.
func (s *Shard) sweep(now int64, limit int, deadline time.Time) (expired int, more bool) {
	popped := 0
	for limit < 0 || popped < limit {
		top, ok := s.exp.Peek()
		if !ok || top.At > now {
			return expired, false
		}
		if !deadline.IsZero() && popped > 0 && time.Now().After(deadline) {
			return expired, true // top is due; only the clock stopped us
		}
		s.exp.Pop()
		popped++
		it, found := s.tab(top.DB).Search(item{Key: top.Key})
		if !found {
			continue
		}
		e := it.E
		if e.ExpireAtMs == top.At && e.ExpGen == top.Gen {
			s.tombstone(top.DB, top.Key, e.Version)
			s.tab(top.DB).Delete(item{Key: top.Key})
			s.addMem(top.DB, -e.memBytes)
			s.untrack(top.DB, top.Key)
			s.eng.ExpiredKeys.Add(1)
			expired++
			if s.OnKeyGone != nil {
				s.OnKeyGone(top.DB, top.Key, "expired")
			}
		}
	}
	// Stopped on the pop cap: report pressure if the next entry is due.
	top, ok := s.exp.Peek()
	return expired, ok && top.At <= now
}
