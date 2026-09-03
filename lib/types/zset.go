package types

import (
	"bytes"

	"github.com/pschlump/pluto/skip_list_ts"
)

// ZElem is one sorted-set element. Ordering is Redis's: score first,
// then member bytes — so members with equal scores sort lexically, which
// is what the BYLEX commands assume.
type ZElem struct {
	Score  float64
	Member string
}

func cmpZElem(a, b ZElem) int {
	switch {
	case a.Score < b.Score:
		return -1
	case a.Score > b.Score:
		return 1
	}
	return bytes.Compare([]byte(a.Member), []byte(b.Member))
}

// ZSet is the Redis sorted-set value: a pluto skip_list_ts ordered by
// (score, member) plus a member→score map (§5.1, §5.3 #1). The skiplist
// carries span counters, so rank and by-index access are O(log n).
type ZSet struct {
	sl *skip_list_ts.SkipList[ZElem]
	m  map[string]float64
}

// NewZSet returns an empty sorted set.
func NewZSet() *ZSet {
	return &ZSet{
		sl: skip_list_ts.NewSkipListFunc(cmpZElem),
		m:  make(map[string]float64),
	}
}

// Len returns the cardinality.
func (z *ZSet) Len() int { return len(z.m) }

// Score returns the member's score.
func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.m[member]
	return s, ok
}

// Add inserts member with score, or updates the score when the member
// exists. Reports whether the member is new.
func (z *ZSet) Add(member string, score float64) bool {
	if cur, ok := z.m[member]; ok {
		if cur == score {
			return false
		}
		z.sl.Delete(ZElem{Score: cur, Member: member})
		z.sl.Insert(ZElem{Score: score, Member: member})
		z.m[member] = score
		return false
	}
	z.sl.Insert(ZElem{Score: score, Member: member})
	z.m[member] = score
	return true
}

// Remove deletes member, reporting whether it existed.
func (z *ZSet) Remove(member string) bool {
	s, ok := z.m[member]
	if !ok {
		return false
	}
	delete(z.m, member)
	return z.sl.Delete(ZElem{Score: s, Member: member})
}

// Rank returns the member's 0-based ascending rank.
func (z *ZSet) Rank(member string) (int, bool) {
	s, ok := z.m[member]
	if !ok {
		return 0, false
	}
	return z.sl.Rank(ZElem{Score: s, Member: member})
}

// At returns the element at 0-based ascending rank i.
func (z *ZSet) At(i int) (ZElem, bool) {
	return z.sl.AtIndex(i)
}

// RemoveRankRange deletes the elements at 0-based ranks [start, stop]
// (inclusive, already clamped by the caller) and returns how many.
func (z *ZSet) RemoveRankRange(start, stop int) int {
	n := 0
	for i := stop; i >= start; i-- {
		el, ok := z.sl.AtIndex(i)
		if !ok {
			continue
		}
		z.sl.Delete(el)
		delete(z.m, el.Member)
		n++
	}
	return n
}

// ScoreRangeLoc returns the half-open ascending rank window [lo, hi) of
// elements inside the score range, via binary search over the span-indexed
// skiplist. Exclusive bounds are honoured exactly.
func (z *ZSet) ScoreRangeLoc(minS, maxS float64, minExcl, maxExcl bool) (lo, hi int) {
	n := z.sl.Len()
	lo = sortSearch(n, func(i int) bool {
		el, _ := z.sl.AtIndex(i)
		if minExcl {
			return el.Score > minS
		}
		return el.Score >= minS
	})
	hi = sortSearch(n, func(i int) bool {
		el, _ := z.sl.AtIndex(i)
		if maxExcl {
			return el.Score >= maxS
		}
		return el.Score > maxS
	})
	return lo, hi
}

// LexRangeLoc is ScoreRangeLoc over member bytes: the window of elements
// whose member lies in the lexicographic range. minKind/maxKind are '<'
// for "-", '>' for "+", '[' for inclusive and '(' for exclusive bounds.
// Like Redis, this assumes equal scores (members then sort lexically).
func (z *ZSet) LexRangeLoc(minKind byte, minM string, maxKind byte, maxM string) (lo, hi int) {
	n := z.sl.Len()
	switch minKind {
	case '<':
		lo = 0
	case '>':
		lo = n
	default:
		lo = sortSearch(n, func(i int) bool {
			el, _ := z.sl.AtIndex(i)
			c := bytes.Compare([]byte(el.Member), []byte(minM))
			if minKind == '(' {
				return c > 0
			}
			return c >= 0
		})
	}
	switch maxKind {
	case '<':
		hi = 0
	case '>':
		hi = n
	default:
		hi = sortSearch(n, func(i int) bool {
			el, _ := z.sl.AtIndex(i)
			c := bytes.Compare([]byte(el.Member), []byte(maxM))
			if maxKind == '(' {
				return c >= 0
			}
			return c > 0
		})
	}
	return lo, hi
}

// sortSearch is sort.Search without the import ceremony at call sites.
func sortSearch(n int, f func(int) bool) int {
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		if f(mid) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}
