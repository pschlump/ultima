# Pluto Request 01 — Skip-list range, rank, and index operations

**Type:** Modification to existing packages `skip_list/` and `skip_list_ts/`
**Priority:** P1 — blocks the entire Redis sorted-set (ZSET) command family
**Blocks:** ZRANGEBYSCORE, ZRANGE, ZRANK, ZREVRANK, ZCOUNT, ZREMRANGEBYSCORE,
ZREMRANGEBYRANK, ZLEXRANGE-style queries, GEO* (via geohash scores)

## Context
Ultima (see `~/go/src/github.com/pschlump/ultima/docs/ULTIMA-DESIGN.md`)
is a high-throughput Redis superset clone in Go. Its sorted-set
type is planned as a skiplist + hash-table pair (the same shape as
Redis's large zset encoding: `note/redis/src/t_zset.c`). Pluto's
`skip_list` currently supports
Insert/Search/Delete/FindMin/FindMax/Truncate and full iterators
(`All`/`Backward`), but has **no** positional or bounded-range
operations — everything below O(n) scans.

Pluto conventions to follow:

- Generics: `NewSkipList[T cmp.Ordered]()` and `NewSkipListFunc[T any](cmp func(a, b T) int)`.
- `skip_list_ts` is the goroutine-safe twin (internal `sync.RWMutex`, identical
  public API, snapshot-based iterators, `Lock()`/`Unlock()` + `Nl*` no-lock methods).
- Iterators are Go 1.23+ range-over-func (`iter.Seq[T]`).
- Stdlib only, no external dependencies.

## Requirements

### 1. Span-annotated links

Add a `span` (width) counter to each forward pointer, exactly as Redis's
`zskiplistNode` does (`note/redis/src/t_zset.c`). This makes rank arithmetic
O(log n). Insert/Delete must maintain spans.

### 2. New API (both `skip_list` and `skip_list_ts`)

```go
// Rank returns the 0-based position of key in sorted order.
func (s *SkipList[T]) Rank(key T) (rank int, found bool)

// AtIndex returns the element at 0-based rank i.
func (s *SkipList[T]) AtIndex(i int) (T, bool)

// Ceil returns the smallest element >= key.
func (s *SkipList[T]) Ceil(key T) (T, bool)

// Floor returns the largest element <= key.
func (s *SkipList[T]) Floor(key T) (T, bool)

// CountRange returns the number of elements with lo <= x <= hi.
func (s *SkipList[T]) CountRange(lo, hi T) int

// Range returns an iterator over elements with lo <= x <= hi, ascending.
func (s *SkipList[T]) Range(lo, hi T) iter.Seq[T]

// RangeBackward is Range in descending order.
func (s *SkipList[T]) RangeBackward(lo, hi T) iter.Seq[T]

// DeleteRange removes all elements with lo <= x <= hi, returning the count.
func (s *SkipList[T]) DeleteRange(lo, hi T) int

// DeleteByRank removes elements with ranks [start, stop] inclusive, returning count.
func (s *SkipList[T]) DeleteByRank(start, stop int) int
```

### 3. Complexity requirements

| Operation                           | Required complexity                                 |
|-------------------------------------|-----------------------------------------------------|
| Rank, AtIndex, Ceil, Floor          | O(log n) expected                                   |
| CountRange                          | O(log n) expected                                   |
| Range / RangeBackward               | O(log n + m), m = returned elements                 |
| DeleteRange / DeleteByRank          | O(log n + m)                                        |
| Insert / Search / Delete (existing) | must not regress more than 5% vs current benchmarks |

### 4. Concurrency

`skip_list_ts` mirrors everything; `Range*` iterators follow the existing
snapshot convention (materialize the range under RLock, then iterate the copy).
`Nl*` no-lock variants for each new method, callable under a client-held
`Lock()` for atomic compound ops (e.g. rank-then-delete).

### 5. Edge cases

- `lo > hi` → empty results, no error/panic.
- Rank on missing key → `found=false`; Rank of elements between existing values
  is not required (exact match only).
- AtIndex out of bounds → `false`.
- Duplicate inserts retain current replace/ignore semantics; spans stay correct.
- After `Truncate`, all structures reset cleanly.

### 6. Tests and benchmarks

- Randomized oracle test: N=10k random inserts/deletes, compare `Rank`,
  `AtIndex`, `Range`, `CountRange` against a sorted-slice oracle after every
  batch.
- Iterator tests: early `break` out of `Range` must not leak or corrupt.
- Benchmarks: Insert/Search/Delete before/after spans (prove ≤5% regression);
  Rank/Range at 1e6 elements.
- `skip_list_ts`: race-detector clean under concurrent readers + one writer.

## References

- Pluto: `skip_list/skip_list.go`, `skip_list_ts/`
- Redis: `note/redis/src/t_zset.c` (zskiplist, `zslRank`, `zslFirstInRange`,
  `zslDeleteRangeByScore`, `zslDeleteRangeByRank`)
