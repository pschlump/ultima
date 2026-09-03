# Pluto Request 10 — Segmented deque ("quicklist" equivalent)

**Type:** New packages `quicklist/` + `quicklist_ts/`
**Priority:** P5 (memory-efficiency phase — not v1)
**Blocks:** memory-competitive large Redis lists; LINDEX/LSET/LRANGE/LINSERT/
LPOS on large lists without O(n) node-hopping pain

## Context

Redis lists are quicklists: a doubly-linked list of nodes, each node a packed
listpack holding up to `list-max-listpack-size` entries (default 128, or size
cap ~8 KiB), with optional LZF compression of interior nodes
(`note/redis/src/quicklist.c`). Pluto's `dqueue`/`dll` are one-object-per-node
linked structures — fine for correctness, but pointer-heavy: a 1M-element list
of small strings costs several times Redis's memory. This request is a Go
segmented deque: linked list of slice-backed segments. Stdlib only; optional
compression hook may use pluto's `lzw` package.

## Requirements

### 1. API

```go
type QuickList[T any] struct{ /* dll of segments; segment = packed []T or packed bytes */ }

func NewQuickList[T any](opts ...Option) *QuickList[T]
// Options: WithSegmentFill(n int)   // target entries per segment, default 128
//          WithSegmentBytes(n int)  // size-based cap alternative
//          WithCompression(codec Codec, depth int) // compress segments deeper
//                                                   // than `depth` from each end

// End operations — amortized O(1):
func (q *QuickList[T]) PushHead(v T)
func (q *QuickList[T]) PushTail(v T)
func (q *QuickList[T]) PopHead() (T, bool)
func (q *QuickList[T]) PopTail() (T, bool)
func (q *QuickList[T]) PeekHead() (T, bool)
func (q *QuickList[T]) PeekTail() (T, bool)

// Positional operations (needed for LINDEX/LSET/LRANGE/LINSERT/LPOS/LREM):
func (q *QuickList[T]) Len() int
func (q *QuickList[T]) At(i int) (T, bool)              // negative i from tail
func (q *QuickList[T]) Set(i int, v T) bool
func (q *QuickList[T]) InsertBefore(i int, v T) bool
func (q *QuickList[T]) InsertAfter(i int, v T) bool
func (q *QuickList[T]) Delete(i int) bool
func (q *QuickList[T]) DeleteRange(start, stop int) int // LTRIM's complement
func (q *QuickList[T]) Trim(start, stop int)            // keep [start, stop]
func (q *QuickList[T]) Range(start, stop int) iter.Seq2[int,T]

// Iteration:
func (q *QuickList[T]) All() iter.Seq2[int,T]
func (q *QuickList[T]) Backward() iter.Seq2[int,T]

// Rotation (RPOPLPUSH / LMOVE): move head of src to tail of dst, O(1) amortized.
func MoveHeadToTail[T any](src, dst *QuickList[T]) (T, bool)
```

### 2. Segment behavior

- Segments split on overflow and merge/rebalance when under-full (e.g. merge
  when two adjacent segments together fit the fill target) so repeated
  insert/delete at one position doesn't degenerate into many tiny segments.
- `At`/`Set`/`Range` must be seek-based: walk segments, not elements —
  O(#segments) to locate, then O(1) within the segment.

### 3. Compression hook (optional layer)

- `Codec` interface `Compress([]byte) []byte / Decompress([]byte) []byte`;
  interior segments beyond `depth` from both ends stored compressed
  (Redis `list-compress-depth` semantics). Provide an adapter to pluto `lzw`.
- Compression must be transparent to every API above (decompress on access).

### 4. Complexity table (acceptance criteria)

| Operation | Required |
|---|---|
| Push/Pop/Peek at ends | amortized O(1) |
| At/Set/Delete by index | O(#segments + seg size), i.e. ~O(√n) with default fill |
| Range over m elements | O(#segments + m) |
| Trim | O(#segments removed) |

### 5. `_ts` twin

Standard pattern: RWMutex, snapshot iterators, `Lock()`/`Unlock()` + `Nl*`.

### 6. Edge cases

- Ops on empty list; single-segment list; delete-last-element-of-segment.
- Negative indexes everywhere Redis allows them; out-of-range → false/empty.
- Trim ranges that clip segment boundaries.
- Insert at head/tail boundary positions must land in the correct segment
  (no phantom empty segments after merges).
- With compression enabled: first/last `depth` segments always plain.

### 7. Tests and benchmarks

- Oracle test vs `[]T` slice: random op mix (push/pop/insert/delete/trim/set)
  for 100k ops, full comparison after each batch, plus segment-invariant checks
  (no empty segments, fill bounds respected, no unary merges).
- Memory benchmark vs `dll`/`dqueue`: 1M small entries, report bytes/element
  (goal: within 1.5× of a plain packed slice, several × better than dll).
- Throughput: end-ops and At benchmarks vs dll/dqueue.

## References

- Redis: `note/redis/src/quicklist.c`, `t_list.c`
- Pluto: `dll/`, `dqueue/`, `lzw/`
