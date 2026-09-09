// Package types holds the four Redis collection value types that live in
// shard.Entry.Obj (design doc §5.1): Hash, List, Set and ZSet. Every type
// is owned by its shard goroutine — no locking inside these types beyond
// what the underlying pluto _ts structures do internally.
//
// Encoding tiers follow Redis: small hashes are insertion-ordered slices
// promoted to a slice+map past hash-max-listpack-entries/value, small
// all-integer sets are sorted int64 slices (intset equivalent) promoted
// to a map past set-max-intset-entries. Like Redis, promotion is
// one-way (no demotion on shrink). Lists use pluto quicklist_ts (§5.3
// #10); sorted sets use pluto skip_list_ts + a member→score map, the
// §5.3 #1 rank/range pairing.
package types

import (
	"iter"

	"github.com/pschlump/pluto/quicklist_ts"
)

// Encoding thresholds, matching the Redis defaults
// (hash-max-listpack-entries/value, set-max-intset-entries).
const (
	HashMaxListpackEntries = 128
	HashMaxListpackValue   = 64
	SetMaxIntsetEntries    = 512
)

// List is the Redis list value: a pluto segmented deque (quicklist
// equivalent, §5.3 #10) plus a running total of element bytes for
// MemUsage (M5b memory accounting). The zero value is ready to use.
type List struct {
	ql    quicklist_ts.QuickList[[]byte]
	bytes int64
}

// NewList returns an empty list.
func NewList() *List { return &List{} }

// Len returns the number of elements.
func (l *List) Len() int { return l.ql.Len() }

// PushHead prepends v.
func (l *List) PushHead(v []byte) { l.bytes += int64(len(v)); l.ql.PushHead(v) }

// PushTail appends v.
func (l *List) PushTail(v []byte) { l.bytes += int64(len(v)); l.ql.PushTail(v) }

// PopHead removes and returns the first element.
func (l *List) PopHead() ([]byte, bool) {
	v, ok := l.ql.PopHead()
	if ok {
		l.bytes -= int64(len(v))
	}
	return v, ok
}

// PopTail removes and returns the last element.
func (l *List) PopTail() ([]byte, bool) {
	v, ok := l.ql.PopTail()
	if ok {
		l.bytes -= int64(len(v))
	}
	return v, ok
}

// PeekHead returns the first element without removing it.
func (l *List) PeekHead() ([]byte, bool) { return l.ql.PeekHead() }

// PeekTail returns the last element without removing it.
func (l *List) PeekTail() ([]byte, bool) { return l.ql.PeekTail() }

// At returns the element at index i.
func (l *List) At(i int) ([]byte, bool) { return l.ql.At(i) }

// Set replaces the element at index i.
func (l *List) Set(i int, v []byte) bool {
	old, ok := l.ql.At(i)
	if !ok {
		return false
	}
	l.bytes += int64(len(v)) - int64(len(old))
	return l.ql.Set(i, v)
}

// InsertBefore inserts v ahead of index i.
func (l *List) InsertBefore(i int, v []byte) bool {
	if !l.ql.InsertBefore(i, v) {
		return false
	}
	l.bytes += int64(len(v))
	return true
}

// InsertAfter inserts v behind index i.
func (l *List) InsertAfter(i int, v []byte) bool {
	if !l.ql.InsertAfter(i, v) {
		return false
	}
	l.bytes += int64(len(v))
	return true
}

// Delete removes the element at index i.
func (l *List) Delete(i int) bool {
	v, ok := l.ql.At(i)
	if !ok {
		return false
	}
	l.bytes -= int64(len(v))
	return l.ql.Delete(i)
}

// Trim keeps only the elements in [start, stop], recomputing the byte
// total from the survivors (O(n), like Redis LTRIM).
func (l *List) Trim(start, stop int) {
	l.ql.Trim(start, stop)
	var keep int64
	for _, v := range l.ql.All() {
		keep += int64(len(v))
	}
	l.bytes = keep
}

// All iterates the elements head to tail.
func (l *List) All() iter.Seq2[int, []byte] { return l.ql.All() }

// Range iterates the elements in [start, stop].
func (l *List) Range(start, stop int) iter.Seq2[int, []byte] { return l.ql.Range(start, stop) }

// Lock takes the list's lock for compound operations (see the Nl*
// methods); shard-goroutine ownership makes it uncontended in practice.
func (l *List) Lock() { l.ql.Lock() }

// Unlock releases the lock taken by Lock.
func (l *List) Unlock() { l.ql.Unlock() }

// NlLen is Len for use while holding Lock.
func (l *List) NlLen() int { return l.ql.NlLen() }

// NlAt is At for use while holding Lock.
func (l *List) NlAt(i int) ([]byte, bool) { return l.ql.NlAt(i) }

// ParseInt64 mirrors Redis string2ll: optional '-', no '+' or spaces, no
// leading zeros ("0" itself is fine), range-checked int64. Used for the
// set intset encoding decision.
func ParseInt64(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	i := 0
	neg := false
	if s[0] == '-' {
		neg = true
		i = 1
		if len(s) == 1 {
			return 0, false
		}
	}
	if s[i] == '0' {
		if len(s)-i != 1 || neg {
			return 0, false
		}
		return 0, true
	}
	var v uint64
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + uint64(c-'0')
		if v > 1<<63 {
			return 0, false
		}
	}
	if neg {
		if v == 1<<63 {
			return -1 << 63, true
		}
		return -int64(v), true
	}
	if v >= 1<<63 {
		return 0, false
	}
	return int64(v), true
}
