# Pluto Request 11 — Cursor-based incremental Scan for hash tables

**Type:** Modification (additive) to `hash_grow/`, `cuckoo/`, and their `_ts` twins
**Priority:** P2 — blocks a correct, non-blocking SCAN implementation
**Blocks:** SCAN, HSCAN, SSCAN, ZSCAN in Ultima

## Context

Redis's SCAN family iterates incrementally with a cursor so a huge keyspace
never stalls the server (`note/redis/src/dict.c` `dictScan` — reverse-binary
bit counting over bucket indexes, which stays meaningful across rehashing).
Pluto's hash tables currently offer only whole-table iterators (`Walk`, `All`,
`Values`). Ultima *could* snapshot-iterate an entire shard per SCAN step, but
that defeats incremental iteration for million-key shards. Each hash table
needs a real cursor API.

Pluto conventions: generics, `NewHashTab[T comparable]` (maphash-seeded) and
`NewHashTabFunc`, range-over-func iterators, stdlib only.

## Requirements

### 1. API (add to `hash_grow`, `cuckoo`, and both `_ts` twins)

```go
// Scan returns up to `count` entries starting at `cursor`, and the next
// cursor. next == 0 means the iteration is complete. Start with cursor == 0.
func (h *HashTab[T]) Scan(cursor uint64, count int) (items []T, next uint64)
```

### 2. Semantics (Redis SCAN contract — match these exactly)

- A **full iteration** (0 → … → 0) returns every element that was present in
  the table for the *entire* iteration, at least once.
- Elements added or deleted mid-iteration may or may not be returned;
  duplicates are allowed.
- `count` is a hint, not a limit — implementations may return more or fewer
  (a whole bucket chain per step is fine).
- Cursor values are opaque to the caller; only 0 is meaningful (start/done).
- Calling Scan with a cursor not produced by a prior Scan of the same table is
  allowed to return arbitrary results (document it), but must not corrupt the
  table or deadlock.

### 3. Resize-safety (the hard part)

- `hash_grow` rehashes on growth; `cuckoo` has a **background resize
  goroutine**. Cursors must stay valid across a resize of the table being
  scanned. Options (pick one, document why):
  - Reverse-binary cursor like Redis (bucket index enumerated in bit-reversed
    order; after a doubling, the enumeration covers the new table correctly).
  - Generation-stamped cursor: embed a table-generation in high cursor bits;
    on generation change, restart or map the position.
- The `_ts` twins must not hold any lock across Scan calls — each call locks,
  reads its batch, unlocks. Concurrent resizes from another goroutine must not
  cause lost-batch duplication beyond what the contract allows.

### 4. Interaction with existing iterators

`Walk`/`All`/`Values` stay as-is. Scan is additive; shared internals
(bucket walking) may be refactored but existing iterator behavior must not
change.

### 5. Edge cases

- Scan on empty table → `(nil, 0)`.
- `count <= 0` → treat as default (e.g. 10).
- Scan concurrent with `Truncate` → next cursor 0 or restart; no panic.
- Full scan of a table under continuous insert/delete churn must terminate
  (bounded steps per cursor — prove in a stress test).

### 6. Tests

- Frozen-table coverage: build table with N random entries, scan to completion
  with count ∈ {1, 7, 1000}; assert the union equals the table exactly
  (duplicates allowed — compare as sets).
- Resize-during-scan: insert during iteration (forcing growth), assert every
  pre-existing element is still returned by completion.
- Cuckoo background-resize case: scan while forcing saturation to trigger the
  resize goroutine; assert coverage + no deadlock under `-race`.
- Termination stress: adversarial insert/delete loop with concurrent scanning
  goroutine, assert it completes within a step bound.

## References

- Redis: `note/redis/src/dict.c` (`dictScan`, reverse-binary increment),
  `note/redis/src/db.c` (`scanGenericCommand`)
- Pluto: `hash_grow/hash_tab.go`, `cuckoo/hash_tab.go`, `_ts` twins
