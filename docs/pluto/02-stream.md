# Pluto Request 02 — Stream data structure

**Type:** New packages `stream/` + `stream_ts/`
**Priority:** P3 — blocks the Redis stream command family
**Blocks:** XADD, XRANGE, XREVRANGE, XLEN, XREAD, XTRIM, XDEL, XGROUP,
XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO, XSETID

## Context

Ultima needs a Redis-compatible stream: an append-oriented log of entries, each
identified by an `<ms>-<seq>` ID, with consumer groups and pending-entry
tracking. Redis implements this as a radix tree (rax) of listpack blocks
(`note/redis/src/t_stream.c`); the Go version should favor simplicity and
concurrency over byte-level compactness. Stdlib only, generics, range-over-func
iterators, `_ts` twin per pluto convention.

## Requirements

### 1. ID type

```go
type ID struct{ Ms, Seq uint64 }

func ParseID(s string) (ID, error)            // "1234-56", also "1234" -> Seq=0
func (id ID) String() string                   // canonical "1234-56"
func CompareID(a, b ID) int
```

- `0-0` is never a valid entry ID (it is the "start" sentinel for ranges).
- Last-ID sentinel semantics: `+` = end, `-` = start, `$` = last entry,
  `>` = never-delivered (consumer-group read) — sentinels may be handled by the
  caller (Ultima), but document which helpers the package provides.

### 2. Entry and core stream API

```go
type Entry struct {
    ID     ID
    Fields [][2]string // ordered field/value pairs, duplicates allowed (Redis allows them)
}

type Stream struct{ /* ... */ }

func NewStream() *Stream
// Add appends an entry. id.Seq == AutoSeq means "next sequence for this ms".
// Returns the assigned ID. Error if id <= last ID (IDs strictly increase).
func (s *Stream) Add(id ID, fields [][2]string) (ID, error)
func (s *Stream) Len() int
func (s *Stream) LastID() ID
func (s *Stream) FirstID() (ID, bool)
// Range returns entries with start <= id <= end, ascending; count<=0 = no limit.
func (s *Stream) Range(start, end ID, count int) iter.Seq[Entry]
func (s *Stream) RevRange(end, start ID, count int) iter.Seq[Entry]
func (s *Stream) Delete(id ID) bool            // removes entry; consumer-group PELs unaffected
func (s *Stream) TrimMaxLen(maxLen int) int    // evict oldest; returns evicted count
func (s *Stream) TrimMinID(min ID) int
func (s *Stream) SetLastID(id ID)              // XSETID support
```

### 3. Consumer groups

```go
type PendingEntry struct {
    ID            ID
    Consumer      string
    DeliveryTime  time.Time
    DeliveryCount int
}

func (s *Stream) CreateGroup(name string, startID ID) error // error if group exists
func (s *Stream) DestroyGroup(name string) int              // returns pending count dropped
func (s *Stream) GroupNames() []string

// ReadGroup delivers entries with id > group's last-delivered-id (">" semantics),
// moving them onto the consumer's pending list.
func (s *Stream) ReadGroup(group, consumer string, after ID, count int) []Entry
func (s *Stream) Ack(group string, ids ...ID) int
func (s *Stream) Pending(group string) (count int, min, max ID, perConsumer map[string]int)
func (s *Stream) PendingRange(group, consumer string, start, end ID, count int) []PendingEntry

// Claim transfers ownership of pending entries idle >= minIdle to consumer.
func (s *Stream) Claim(group, consumer string, minIdle time.Duration, ids ...ID) []Entry
func (s *Stream) AutoClaim(group, consumer string, minIdle time.Duration, start ID, count int) (entries []Entry, nextStart ID, deletedIDs []ID)

// Group management
func (s *Stream) GroupCreateConsumer(group, consumer string) bool
func (s *Stream) GroupDeleteConsumer(group, consumer string) int // returns pending count dropped
func (s *Stream) GroupSetID(group string, id ID)
```

### 4. Internal layout requirement

- Ordered storage keyed by ID with O(log n) add/seek: use a skip list or B-tree
  keyed by `ID` (pluto `skip_list` once request 01 lands, or `b_tree`), with
  entries packed in blocks for memory locality. Document the choice.
- The PEL per group must support O(log n) min-idle lookup for XAUTOCLAIM
  (a heap on delivery time, or ordered by ID with a time index — your choice,
  document it).

### 5. Concurrency

`stream_ts` twin: RWMutex, snapshot iterators, `Lock()`/`Unlock()` + `Nl*`
methods so Ultima can do atomic read-then-ack compounds.

### 6. Edge cases

- Add with `id == lastID` or smaller → error (except equal-ms with bumped Seq
  via AutoSeq).
- Add to empty stream with explicit `0-1` allowed; `0-0` rejected.
- TrimMaxLen(0) empties the stream; group PEL entries for evicted/deleted IDs
  persist (XAUTOCLAIM must report them as deleted, per Redis).
- Ranges with start > end → empty.

### 7. Tests and benchmarks

- Behavioral tests mirrored from `note/redis/tests/unit/type/stream*.tcl`
  expectations (ID monotonicity, auto-seq, trim, group delivery/ack/claim).
- Benchmark: 1M XADD-equivalent adds; Range over 1M-entry stream; XAUTOCLAIM
  over 100k pending.

## References

- Redis: `note/redis/src/t_stream.c`, `note/redis/tests/unit/type/stream*.tcl`
- Pluto building blocks: `skip_list` (request 01), `heap_ts`, `hash_grow_ts`
