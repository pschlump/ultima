# Pluto Request 09 — Bitmap and bitfield operations

**Type:** New package `bitmap/` (with `bitmap_ts/` only if you judge it useful —
Ultima will call it under shard locks, so plain is acceptable)
**Priority:** P4 — blocks the Redis bit operation family
**Blocks:** SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO

## Context

Redis bitmaps are plain strings with bit-level operations
(`note/redis/src/t_string.c` — `setbitCommand` etc., and
`note/redis/src/bitfield.c`). Ultima stores strings as `[]byte`; what's needed
is a well-tested, fast package of bit operations over byte slices with Redis-
exact semantics (growth rules, range units, overflow policies). Stdlib only.

## Requirements

### 1. API

```go
// GETBIT / SETBIT. SetBit grows buf (zero-filled) as needed and returns the
// previous bit. Offset is a bit offset from the MSB of byte 0 (Redis order).
func GetBit(buf []byte, offset uint64) int
func SetBit(buf []byte, offset uint64, bit int) (newBuf []byte, oldBit int)

// BITCOUNT [start end [BYTE|BIT]] — byte or bit indexed ranges, negative
// indexes from the end, Redis semantics.
func BitCount(buf []byte, start, end int64, unit Unit) int64

// BITPOS bit [start [end [BYTE|BIT]]] — incl. the "not found" and
// empty-string edge cases Redis defines.
func BitPos(buf []byte, bit int, start, end int64, unit Unit) (int64, bool)

// BITOP AND|OR|XOR dest, src... and BITOP NOT dest src.
// Result length = longest source (shortest for NOT = its only source);
// missing bytes read as zero. Returns a new slice.
func BitOpAnd(srcs ...[]byte) []byte
func BitOpOr(srcs ...[]byte) []byte
func BitOpXor(srcs ...[]byte) []byte
func BitOpNot(src []byte) []byte

type Unit int
const ( ByteUnit Unit = iota; BitUnit )

// BITFIELD subcommands.
type FieldOp struct {
    Kind     FieldKind // Get, Set, IncrBy
    Signed   bool
    Bits     int       // 1..63 signed, 1..64 unsigned
    Offset   int64     // in bits; "#n" multiplier syntax resolved by caller
    Value    int64     // Set/IncrBy value
    Overflow OverflowPolicy
}

type OverflowPolicy int
const ( OverflowWrap OverflowPolicy = iota; OverflowSat; OverflowFail )

// Execute applies ops left-to-right, growing buf as needed. Results per op:
// Get -> value; Set -> old value; IncrBy -> new value (nil entry on OverflowFail
// overflow — represent with *int64 or (int64, ok)).
func ExecuteFieldOps(buf []byte, ops []FieldOp) (newBuf []byte, results []int64, failed []bool)
```

### 2. Correctness requirements (Redis-exact)

- Bit order: MSB-first within each byte (`GETBIT key 7` reads the last bit of
  byte 0).
- Growth: SETBIT/BITFIELD grow with zero-fill; max size 512 MiB like Redis
  (return error past it).
- BITCOUNT/BITPOS negative indexes measured from string end in the range's
  unit; BITPOS on empty string looking for 0 → -1, looking for 1 → 0... match
  `bitposCommand` exactly (`t_string.c`).
- BITOP NOT requires exactly one source; result length = that source's length.
- Overflow: WRAP (mod 2^n, two's complement), SAT (clamp to min/max — signed
  and unsigned limits differ), FAIL (no change, null result).

### 3. Performance

- BitCount should use `math/bits.OnesCount64` over 8-byte words (Redis uses
  popcount tables + SIMD; word-wise popcount is the Go equivalent).
- BitOp* word-wise (8 or 16 bytes at a time), not bit-wise.
- Benchmarks: BitCount and BitOpOr over 1 MB / 100 MB inputs.

### 4. Tests

- Differential vectors: every tricky case from
  `note/redis/tests/unit/bitops.tcl` and `bitfield.tcl` ported as table tests
  (these TCL files encode the edge-case corpus — mine them).
- Fuzz: random buffers and offsets; invariants (GetBit after SetBit, BitCount
  == popcount of range, BitOp associativity).
- Overflow policy table tests at sign boundaries (i8/i63/u64 edges).

## References

- Redis: `note/redis/src/t_string.c` (bitops), `note/redis/src/bitfield.c`,
  `note/redis/tests/unit/bitops.tcl`, `note/redis/tests/unit/bitfield.tcl`
