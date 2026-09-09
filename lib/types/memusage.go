package types

// Memory-usage estimators for M5b maxmemory eviction (docs/pluto design
// doc §5.2, decision D10): O(1) rough byte counts per value, fed by the
// content-byte counters each type maintains on its mutation paths. These
// are ESTIMATES — Go's encodings share nothing with Redis's
// robj/listpack/intset layout, so byte-exact parity with Redis
// used_memory is impossible and not a goal (the differential gate never
// asserts absolute memory numbers). What matters is that the numbers are
// monotone in real content size and roughly proportional across types,
// so per-shard quotas evict the right amount.

// Structural-overhead estimates, in bytes.
const (
	// hashBase is the Hash struct plus slice header; hashPair is one
	// slice element (two string headers); hashMapSlot is the per-entry
	// cost of the promotion index.
	hashBase     int64 = 64
	hashPairCost int64 = 32
	hashMapSlot  int64 = 16

	// setBase is the Set struct plus slice header; setIntsetElem is one
	// int64; setMapSlot is one map bucket share.
	setBase       int64 = 64
	setIntsetElem int64 = 8
	setMapSlot    int64 = 16

	// zsetBase is the ZSet struct plus skiplist header; zsetElem is one
	// skiplist node (element + ~1.33 forward pointers on average) plus
	// one map entry (string header + float64 + bucket share).
	zsetBase int64 = 96
	zsetElem int64 = 72

	// listBase is the List wrapper plus the quicklist header; listElem
	// is one element's share of a segment (slice slot + bookkeeping).
	listBase int64 = 96
	listElem int64 = 24
)

// MemUsage estimates the hash's memory in bytes: fixed overhead, per-pair
// structural cost, tracked content bytes, and the promotion index when
// the large encoding is active. O(1).
func (h *Hash) MemUsage() int64 {
	n := int64(len(h.pairs))
	total := hashBase + n*hashPairCost + h.bytes
	if h.idx != nil {
		total += n * hashMapSlot
	}
	return total
}

// MemUsage estimates the set's memory in bytes: fixed overhead plus
// either the intset's flat 8 bytes per member or the map encoding's
// per-member slot and tracked content bytes. O(1).
func (s *Set) MemUsage() int64 {
	if s.isInt {
		return setBase + int64(len(s.ints))*setIntsetElem
	}
	return setBase + int64(len(s.member))*setMapSlot + s.bytes
}

// MemUsage estimates the sorted set's memory in bytes: fixed overhead,
// per-element skiplist node + map entry cost, and tracked member bytes.
// O(1).
func (z *ZSet) MemUsage() int64 {
	return zsetBase + int64(len(z.m))*zsetElem + z.bytes
}

// MemUsage estimates the list's memory in bytes: fixed overhead, a
// per-element segment share, and the tracked element bytes. O(1).
func (l *List) MemUsage() int64 {
	return listBase + int64(l.ql.Len())*listElem + l.bytes
}
