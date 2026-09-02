# Pluto Request 05 — LFU (least-frequently-used) frequency counters

**Type:** New packages `lfu/` + `lfu_ts/`
**Priority:** P4 — blocks `maxmemory-policy allkeys-lfu` / `volatile-lfu`
**Blocks:** eviction policies, OBJECT FREQ

## Context

Ultima implements Redis's maxmemory eviction. LRU is covered by pluto's `lru`,
but LFU has no equivalent. Redis's LFU (`note/redis/src/evict.c`,
`LFULogIncr`/`LFUDecrAndReturn` in `note/redis/src/object.c` and
`note/redis/src/lfu.c`) uses an 8-bit **Morris logarithmic counter** plus a
16-bit "last access time" (in minutes) for time-based decay. This request is a
generic, reusable version of that mechanism. Stdlib only, `_ts` twin per pluto
convention.

## Requirements

### 1. Core counter primitive

```go
// MorrisCounter is an 8-bit logarithmic probabilistic counter.
type MorrisCounter struct{ v uint8 }

// Incr increments with probability r / (baseValue*logFactor + 1).
// Saturates at 255. Returns the new value.
func (c *MorrisCounter) Incr(logFactor int) uint8

// Decay decrements by the number of elapsed decay periods.
func (c *MorrisCounter) Decay(periods int) uint8

func (c *MorrisCounter) Value() uint8
```

Increment math must follow Redis exactly (so eviction behavior matches):
given random `r` in `[0,1)`, increment iff `r < 1.0/(baseValue*logFactor+1)`
where `baseValue` is the current counter value (initial 5 — `LFU_INIT_VAL`).

### 2. Keyed LFU table (generic)

```go
type Lfu[K comparable] struct{ /* ... */ }

func NewLfu[K comparable](logFactor int, decayTimeMinutes int) *Lfu[K]

// Touch records an access: applies time-based decay, then probabilistic incr.
func (l *Lfu[K]) Touch(key K) uint8
func (l *Lfu[K]) Counter(key K) (uint8, bool)   // OBJECT FREQ support; no side effects
func (l *Lfu[K]) Add(key K)                      // insert with LFU_INIT_VAL
func (l *Lfu[K]) Delete(key K) bool
func (l *Lfu[K]) Len() int
func (l *Lfu[K]) Truncate()

// IdleMinutes returns minutes since last Touch (from the stored minute clock).
func (l *Lfu[K]) IdleMinutes(key K) (int, bool)
```

- Internal clock: minutes since epoch (16-bit wraparound-aware, as Redis's
  `LFUTimeElapsed`), injectable clock func for testing.
- Backing storage: pluto hash table (`hash_grow`) keyed by K → `{counter uint8,
  lastMin uint16}`.

### 3. Concurrency

`lfu_ts` twin with RWMutex; `Touch` takes the write lock. Ultima will call these
inside shard owner-goroutines, so contention is low — favor simplicity.

### 4. Edge cases

- Touch on unknown key → treat as Add then Touch (document) or no-op — pick and
  document; Ultima's eviction path needs the "Add then Touch" behavior.
- `decayTimeMinutes == 0` → no decay (Redis semantics).
- Clock wraparound at 65535 minutes must not corrupt counters.

### 5. Tests

- Determinism test with seeded RNG: two counters receiving the same access
  pattern diverge statistically but stay within expected distribution (chi-square
  or mean/±3σ bounds over 10k trials).
- Decay test with injected clock: counter decays exactly `periods` steps.
- Hot/cold separation test: after mixed access, hottest keys have measurably
  higher counters (needed for eviction quality).

## References

- Redis: `note/redis/src/lfu.c`, `object.c` (`LFULogIncr`, `LFUDecrAndReturn`),
  `evict.c`
- Morris, "Counting large numbers of events in small registers" (1978)
