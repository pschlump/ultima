# Pluto Request 06 — Thread-safe LRU twin (`lru_ts`)

**Type:** New package `lru_ts/` (twin of existing `lru/`)
**Priority:** P3 — needed for `allkeys-lru` / `volatile-lru` eviction
**Blocks:** maxmemory LRU policies in Ultima

## Context

Pluto's `lru` (capacity-bounded LRU with eviction-veto callback, soft cap,
MRU→LRU iterators) is the only major eviction-related structure without a
goroutine-safe twin. Ultima will run one LRU per shard — mostly uncontended —
but the `_ts` wrapper is required to match the library's safety contract and to
allow safe cross-shard inspection (metrics, DEBUG commands).

## Requirements

### 1. Follow the established `_ts` twin pattern exactly

- Identical public API to `lru`: `NewLru`, `NewLruFunc` (with eviction-veto
  callback), `Get`, `Peek`, `Put`, `Delete`, `Clear`, `Len`, MRU→LRU /
  LRU→MRU iterators.
- Internal `sync.RWMutex`: read lock for `Get`-shaped reads where possible —
  **but note** `Get` mutates recency, so it needs the write lock; `Peek` is a
  true read. Document which methods take which lock.
- `Lock()`/`Unlock()` + `Nl*` no-lock variants (matching the ten pluto packages
  that already expose this pattern) so callers can compose atomic compound ops,
  e.g. "evict until under cap" loops.
- Snapshot-based iterators: materialize the key order under lock, then iterate.

### 2. Callback safety

- The eviction-veto callback in plain `lru` runs inside the mutation path. In
  `lru_ts` it will run **while holding the write lock** — document this
  prominently: callbacks must not call back into the same `lru_ts` (deadlock).
- Provide a `Nl*`-documented escape hatch: veto callback receives a no-lock
  handle? (Optional — only if it can be done without weakening safety;
  otherwise document the constraint and move on.)

### 3. Plain `lru` must remain unchanged

No behavioral or API changes to `lru/` — `lru_ts` wraps or mirrors it. If
implementation sharing requires small exported hooks on `lru`, keep them
additive only.

### 4. Tests

- Port all existing `lru` tests to `lru_ts` (behavioral identity).
- Race-detector test: N goroutines doing Get/Put/Delete/iterate concurrently.
- Compound-op test using `Lock()` + `Nl*`: concurrent "Put then evict-to-cap"
  from multiple goroutines must never exceed cap and never deadlock.
- Benchmark: uncontended Get/Put vs plain `lru` (report lock overhead);
  contended 8-goroutine Get/Put.

## References

- Pluto: `lru/lru.go`; twin pattern examples: `hash_tab_ts/`, `heap_ts/`,
  `dqueue_ts/`
