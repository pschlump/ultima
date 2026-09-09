package types

import "strconv"

// Set is the Redis set value. While every member is an int64 (strict
// string2ll parse) and the cardinality stays ≤ SetMaxIntsetEntries it is
// a sorted int64 slice — the intset equivalent, whose ascending iteration
// order is wire-visible in SMEMBERS/SINTER/…. A non-integer member or
// overflow past the threshold promotes it to a map, one-way like Redis.
type Set struct {
	ints   []int64             // sorted; valid while isInt
	isInt  bool                // intset encoding active
	member map[string]struct{} // valid when !isInt
	bytes  int64               // sum of member lengths, map encoding only; for MemUsage
}

// NewSet returns an empty set (intset encoding).
func NewSet() *Set { return &Set{isInt: true} }

// Len returns the cardinality.
func (s *Set) Len() int {
	if s.isInt {
		return len(s.ints)
	}
	return len(s.member)
}

// IsIntset reports whether the set uses the small int encoding.
func (s *Set) IsIntset() bool { return s.isInt }

// Contains reports membership.
func (s *Set) Contains(m string) bool {
	if s.isInt {
		v, ok := ParseInt64(m)
		if !ok {
			return false
		}
		_, found := s.find(v)
		return found
	}
	_, ok := s.member[m]
	return ok
}

// find returns the position of v (or its insertion point) and whether it
// is present; intset encoding only.
func (s *Set) find(v int64) (int, bool) {
	lo, hi := 0, len(s.ints)
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case s.ints[mid] < v:
			lo = mid + 1
		case s.ints[mid] > v:
			hi = mid
		default:
			return mid, true
		}
	}
	return lo, false
}

// Add inserts m, reporting whether it is new. Adding a non-integer
// member, or growing past SetMaxIntsetEntries, promotes the encoding.
func (s *Set) Add(m string) bool {
	v, isInt := ParseInt64(m)
	if s.isInt && isInt {
		if i, found := s.find(v); found {
			return false
		} else if len(s.ints) < SetMaxIntsetEntries {
			s.ints = append(s.ints, 0)
			copy(s.ints[i+1:], s.ints[i:])
			s.ints[i] = v
			return true
		}
	}
	if s.isInt {
		s.promote()
	}
	if _, ok := s.member[m]; ok {
		return false
	}
	s.member[m] = struct{}{}
	s.bytes += int64(len(m))
	return true
}

// promote converts the intset slice into the map encoding.
func (s *Set) promote() {
	s.member = make(map[string]struct{}, len(s.ints))
	for _, v := range s.ints {
		m := strconv.FormatInt(v, 10)
		s.member[m] = struct{}{}
		s.bytes += int64(len(m))
	}
	s.ints = nil
	s.isInt = false
}

// Remove deletes m, reporting whether it was present.
func (s *Set) Remove(m string) bool {
	if s.isInt {
		v, ok := ParseInt64(m)
		if !ok {
			return false
		}
		i, found := s.find(v)
		if !found {
			return false
		}
		s.ints = append(s.ints[:i], s.ints[i+1:]...)
		return true
	}
	if _, ok := s.member[m]; !ok {
		return false
	}
	delete(s.member, m)
	s.bytes -= int64(len(m))
	return true
}

// Members returns every member: ascending numeric order for the intset
// encoding (matching Redis), map iteration order otherwise.
func (s *Set) Members() []string {
	if s.isInt {
		out := make([]string, len(s.ints))
		for i, v := range s.ints {
			out[i] = strconv.FormatInt(v, 10)
		}
		return out
	}
	out := make([]string, 0, len(s.member))
	for m := range s.member {
		out = append(out, m)
	}
	return out
}
