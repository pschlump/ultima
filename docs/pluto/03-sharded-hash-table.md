# Pluto Request 03 — Sharded (striped) concurrent hash table

**Type:** New package `sharded_hash_ts/`
**Priority:** P2 (optional but strongly desired)
**Blocks:** Simpler keyspace implementation; unified SCAN cursors

## Context

Ultima shards its keyspace into N owner-goroutine shards. Today that means N
independent `cuckoo_ts` tables plus glue code in Ultima for routing and unified
iteration. A native striped table would replace that glue: one logical table,
internal striping, per-stripe locking, and a single SCAN cursor space.

Pluto conventions: generics with `New...[T comparable]` (stdlib `hash/maphash`,
per-table random seed) and `New...Func[T any]` variants; range-over-func
iterators; stdlib only. This package is inherently concurrent, so it is `_ts`-
suffixed by nature (there is no plain twin).

## Requirements

### 1. API

```go
type ShardedHash[T comparable] struct{ /* ... */ }

func NewShardedHash[T comparable](opts ...Option) *ShardedHash[T]
// Options: WithStripes(n int) (default 256, rounded to power of two),
//          WithInitialCapacity(n int), WithLoadFactor(f float64)

func (h *ShardedHash[T]) Insert(key T, value V) (replaced bool)   // see note on shape below
func (h *ShardedHash[T]) Search(key T) (V, bool)
func (h *ShardedHash[T]) Delete(key T) bool
func (h *ShardedHash[T]) Len() int          // exact total (atomic counters ok)
func (h *ShardedHash[T]) Truncate()

// LockKey returns the stripe lock for key held in the requested mode plus
// no-lock accessors, enabling atomic read-modify-write on one key.
func (h *ShardedHash[T]) LockKey(key T) (nl NlShardedHash[T], unlock func())

// Scan returns up to `count` entries and the next cursor (0 = done).
// One cursor space across all stripes (e.g. cursor = stripe:slot).
func (h *ShardedHash[T]) Scan(cursor uint64, count int) (items []Pair[T, V], next uint64)

func (h *ShardedHash[T]) StripeCount() int
func (h *ShardedHash[T]) StripeLen(i int) int   // per-stripe load, for metrics
```

(Adjust the exact KV shape — separate `key, value` params vs. a single `T`
holding both — to match how `hash_grow`/`cuckoo` are shaped; consistency with
the existing `HashTab[T]` API matters more than this sketch.)

### 2. Semantics

- Insert replaces on equal key and reports whether it replaced.
- Per-stripe independent growth (open addressing or cuckoo — reuse existing
  pluto internals if practical; document which).
- `Scan` guarantees, matching Redis SCAN: a full iteration (cursor 0 → cursor 0)
  returns every element present for the entire scan, at least once; elements
  added/removed mid-scan may or may not appear.
- `Scan` must never hold more than one stripe lock at a time, and never across
  a call boundary.

### 3. Concurrency requirements

- Concurrent Insert/Search/Delete on disjoint keys must scale near-linearly
  with stripe count (demonstrate with a benchmark at 1/4/16 stripes ×
  GOMAXPROCS goroutines).
- No global lock anywhere on the hot path (Len may use `atomic.Int64`).
- `LockKey` ordering rule: clients locking multiple keys must lock in
  stripe-index order — document this.

### 4. Edge cases

- Truncate concurrent with Scan: Scan returns error or restarts cleanly —
  pick one, document it.
- Stripe growth during Scan of that stripe: cursor must remain valid
  (rehash-safe cursor design; Redis's reverse-binary iteration is an acceptable
  model — see `note/redis/src/dict.c` `dictScan`).

### 5. Tests and benchmarks

- Oracle test vs `map[T]V` under `-race` with random concurrent op mix.
- Scan full-coverage property test: freeze table, scan to completion, assert
  exact set equality; then repeat with concurrent mutations asserting
  no-panic/no-deadlock.
- Throughput benchmark vs `cuckoo_ts` single-lock and vs N × `cuckoo_ts`,
  read-heavy and 50/50 mixes; report ns/op and allocs/op.

## References

- Pluto: `cuckoo_ts/hash_tab.go`, `hash_grow_ts/hash_tab.go`
- Redis: `note/redis/src/dict.c` (`dictScan`, reverse-binary cursor)
