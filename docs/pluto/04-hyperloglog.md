# Pluto Request 04 — HyperLogLog cardinality estimator

**Type:** New packages `hyperloglog/` + `hyperloglog_ts/`
**Priority:** P4 — blocks the Redis PF* command family
**Blocks:** PFADD, PFCOUNT, PFMERGE

## Context

Ultima needs a HyperLogLog (HLL) implementation compatible in *behavior* with
Redis's PF* commands (`note/redis/src/hyperloglog.c`): ~0.81% standard error,
dense 6-bit registers, 2^14 = 16384 registers. Bit-level compatibility with
Redis's serialized format is **not** required (Ultima has its own persistence
format), but the accuracy profile and API semantics must match. Stdlib only,
`_ts` twin per pluto convention.

## Requirements

### 1. API

```go
const Precision = 14          // m = 1<<14 registers, 6 bits each (12 KiB dense)
const RegisterBits = 6

type Hll struct{ /* dense [12288]byte register array (16384 x 6 bits) */ }

func NewHll() *Hll

// Add hashes v internally and updates the registers. Returns true if any
// register changed (needed for PFADD's reply and for cache invalidation).
func (h *Hll) Add(v []byte) bool

// Count returns the estimated cardinality.
func (h *Hll) Count() uint64

// Merge folds others into h (register-wise max). others are unchanged.
func (h *Hll) Merge(others ...*Hll)

func (h *Hll) Reset()
func (h *Hll) Bytes() []byte            // dense serialized form
func HllFromBytes(b []byte) (*Hll, error)
```

### 2. Hashing

- Internal deterministic 64-bit hash with good avalanche behavior (implement
  xxhash64 or MurmurHash64A inline — no external deps). Redis uses MurmurHash64A
  with seed 0xadc83b19; matching it exactly is optional, but the hash must be
  fixed and documented so serialized HLLs remain valid across versions.

### 3. Estimator requirements

- Standard HLL with:
  - **LinearCounting** correction for small cardinalities (raw estimate below
    ~2.5m threshold).
  - Bias correction for the mid-range: either the HLL++ bias tables approach or
    the LogLogBeta estimator — your choice, document which and why.
- Accuracy acceptance criteria (must be demonstrated in tests):
  - |relative error| < 1.5% at cardinalities 1k, 10k, 100k, 1M, 10M
    (mean over ≥ 20 random trials), typical error ≈ 0.8%.
  - Near-exact (< 0.5% error) below ~1000 (LinearCounting range).

### 4. Edge cases

- Count on empty → 0.
- Merge with empty HLLs → no-op.
- `HllFromBytes` validates length and register values (max 64); corrupt input →
  error.
- Add of the same value repeatedly must not change Count materially.

### 5. Concurrency

`hyperloglog_ts` twin with RWMutex; `Add` takes write lock (register update),
`Count` may take read lock (or atomically cached count invalidated by Add —
your choice, document).

### 6. Tests and benchmarks

- Cardinality sweep test (exact-set oracle) at the points above.
- Merge correctness: split N elements across k HLLs, merge, compare vs full.
- Serialization round-trip test.
- Benchmark: Add throughput (target ≥ 10M ops/s single goroutine), Count cost
  (and cached-Count cost if cached).

## References

- Redis: `note/redis/src/hyperloglog.c`
- Paper: Flajolet et al., "HyperLogLog: the analysis of a near-optimal
  cardinality estimation algorithm"; Heule et al., "HyperLogLog in Practice"
  (HLL++ bias correction)
