package types

// Hash is the Redis hash value. Small hashes (≤ HashMaxListpackEntries
// fields, all fields and values ≤ HashMaxListpackValue bytes) are an
// insertion-ordered slice, matching the wire-visible iteration order of
// Redis's listpack encoding. Larger hashes add a field→index map for
// O(1) lookup. Like Redis, promotion is one-way.
type Hash struct {
	pairs []hashPair
	idx   map[string]int // nil while small
	bytes int64          // sum of len(field)+len(value), for MemUsage
}

type hashPair struct {
	field string
	value string
}

// NewHash returns an empty hash.
func NewHash() *Hash { return &Hash{} }

// Len returns the number of fields.
func (h *Hash) Len() int { return len(h.pairs) }

// Big reports whether the hash uses the large (indexed) encoding.
func (h *Hash) Big() bool { return h.idx != nil }

// Get returns the value for field.
func (h *Hash) Get(field string) (string, bool) {
	if h.idx != nil {
		if i, ok := h.idx[field]; ok {
			return h.pairs[i].value, true
		}
		return "", false
	}
	for i := range h.pairs {
		if h.pairs[i].field == field {
			return h.pairs[i].value, true
		}
	}
	return "", false
}

// Set inserts or updates field, reporting whether it is new. May promote
// the encoding past the listpack thresholds.
func (h *Hash) Set(field, value string) (added bool) {
	if h.idx != nil {
		if i, ok := h.idx[field]; ok {
			h.bytes += int64(len(value)) - int64(len(h.pairs[i].value))
			h.pairs[i].value = value
			return false
		}
		h.idx[field] = len(h.pairs)
		h.pairs = append(h.pairs, hashPair{field, value})
		h.bytes += int64(len(field)) + int64(len(value))
		return true
	}
	for i := range h.pairs {
		if h.pairs[i].field == field {
			h.bytes += int64(len(value)) - int64(len(h.pairs[i].value))
			h.pairs[i].value = value
			return false
		}
	}
	h.pairs = append(h.pairs, hashPair{field, value})
	h.bytes += int64(len(field)) + int64(len(value))
	if len(h.pairs) > HashMaxListpackEntries ||
		len(field) > HashMaxListpackValue || len(value) > HashMaxListpackValue {
		h.promote()
	}
	return true
}

// promote builds the field index (listpack → hashtable equivalent).
func (h *Hash) promote() {
	h.idx = make(map[string]int, len(h.pairs))
	for i := range h.pairs {
		h.idx[h.pairs[i].field] = i
	}
}

// Del removes field, reporting whether it existed. Insertion order of
// the survivors is preserved (listpack delete semantics).
func (h *Hash) Del(field string) bool {
	if h.idx != nil {
		i, ok := h.idx[field]
		if !ok {
			return false
		}
		h.bytes -= int64(len(field)) + int64(len(h.pairs[i].value))
		delete(h.idx, field)
		h.pairs = append(h.pairs[:i], h.pairs[i+1:]...)
		for j := i; j < len(h.pairs); j++ {
			h.idx[h.pairs[j].field] = j
		}
		return true
	}
	for i := range h.pairs {
		if h.pairs[i].field == field {
			h.bytes -= int64(len(field)) + int64(len(h.pairs[i].value))
			h.pairs = append(h.pairs[:i], h.pairs[i+1:]...)
			return true
		}
	}
	return false
}

// Fields returns every field in insertion order.
func (h *Hash) Fields() []string {
	out := make([]string, len(h.pairs))
	for i, p := range h.pairs {
		out[i] = p.field
	}
	return out
}

// Each calls fn(field, value) for every pair in insertion order.
func (h *Hash) Each(fn func(field, value string)) {
	for _, p := range h.pairs {
		fn(p.field, p.value)
	}
}
