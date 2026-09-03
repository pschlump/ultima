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
// equivalent, §5.3 #10). The zero value is ready to use.
type List = quicklist_ts.QuickList[[]byte]

// NewList returns an empty list.
func NewList() *List { return &quicklist_ts.QuickList[[]byte]{} }

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
