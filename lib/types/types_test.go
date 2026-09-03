package types

import (
	"strconv"
	"testing"
)

func TestHashEncodingPromotion(t *testing.T) {
	h := NewHash()
	for i := 0; i < HashMaxListpackEntries; i++ {
		h.Set("f"+strconv.Itoa(i), "v")
	}
	if h.Big() {
		t.Fatal("hash promoted at 128 fields")
	}
	h.Set("f128", "v")
	if !h.Big() {
		t.Fatal("hash not promoted at 129 fields")
	}
	if got, ok := h.Get("f0"); !ok || got != "v" {
		t.Fatal("f0 lost in promotion")
	}
	// One-way: shrinking does not demote (Redis behavior).
	for i := 0; i < HashMaxListpackEntries; i++ {
		h.Del("f" + strconv.Itoa(i))
	}
	if !h.Big() {
		t.Fatal("hash demoted on shrink")
	}
	if h.Len() != 1 {
		t.Fatalf("Len = %d", h.Len())
	}
}

func TestHashValueSizePromotion(t *testing.T) {
	h := NewHash()
	big := make([]byte, HashMaxListpackValue+1)
	h.Set("f", string(big))
	if !h.Big() {
		t.Fatal("oversize value did not promote")
	}
}

func TestHashInsertionOrder(t *testing.T) {
	h := NewHash()
	h.Set("z", "1")
	h.Set("a", "2")
	h.Set("m", "3")
	h.Set("a", "9") // update keeps position
	h.Del("z")
	h.Set("z", "5") // re-add goes to the end
	var got []string
	h.Each(func(f, v string) { got = append(got, f+v) })
	want := []string{"a9", "m3", "z5"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestSetIntset(t *testing.T) {
	s := NewSet()
	if !s.Add("3") || !s.Add("1") || !s.Add("2") {
		t.Fatal("adds failed")
	}
	if s.Add("2") {
		t.Fatal("duplicate add reported new")
	}
	if !s.IsIntset() {
		t.Fatal("int set not in intset encoding")
	}
	m := s.Members()
	if m[0] != "1" || m[1] != "2" || m[2] != "3" {
		t.Fatalf("intset not sorted: %v", m)
	}
	if !s.Contains("2") || s.Contains("9") || s.Contains("x") {
		t.Fatal("Contains wrong")
	}
	// "01" is not an integer (string2ll): promotes.
	if s.Add("01") && s.IsIntset() {
		t.Fatal("non-canonical int kept intset encoding")
	}
	if !s.Contains("01") || !s.Contains("1") {
		t.Fatal("members lost in promotion")
	}
	if s.Remove("9") || !s.Remove("1") || s.Remove("1") {
		t.Fatal("Remove wrong")
	}
}

func TestSetIntsetOverflowPromotes(t *testing.T) {
	s := NewSet()
	for i := 0; i < SetMaxIntsetEntries; i++ {
		s.Add(strconv.Itoa(i * 2))
	}
	if !s.IsIntset() {
		t.Fatal("promoted at exactly 512")
	}
	s.Add(strconv.Itoa(2 * SetMaxIntsetEntries))
	if s.IsIntset() {
		t.Fatal("not promoted at 513")
	}
	if s.Len() != SetMaxIntsetEntries+1 {
		t.Fatalf("Len = %d", s.Len())
	}
}

func TestZSetRankRanges(t *testing.T) {
	z := NewZSet()
	for i, m := range []string{"a", "b", "c", "d", "e"} {
		z.Add(m, float64(i+1))
	}
	if r, ok := z.Rank("c"); !ok || r != 2 {
		t.Fatalf("Rank(c) = %d,%v", r, ok)
	}
	lo, hi := z.ScoreRangeLoc(2, 4, false, false)
	if lo != 1 || hi != 4 {
		t.Fatalf("ScoreRangeLoc [2,4] = [%d,%d)", lo, hi)
	}
	lo, hi = z.ScoreRangeLoc(2, 4, true, true)
	if lo != 2 || hi != 3 {
		t.Fatalf("ScoreRangeLoc (2,4) = [%d,%d)", lo, hi)
	}
	lo, hi = z.LexRangeLoc('[', "b", '(', "e")
	if lo != 1 || hi != 4 {
		t.Fatalf("LexRangeLoc [b,(e = [%d,%d)", lo, hi)
	}
	// score update reorders
	z.Add("a", 10)
	if r, _ := z.Rank("a"); r != 4 {
		t.Fatalf("Rank(a) after update = %d", r)
	}
	if el, _ := z.At(4); el.Member != "a" || el.Score != 10 {
		t.Fatalf("At(4) = %+v", el)
	}
	if n := z.RemoveRankRange(0, 1); n != 2 || z.Len() != 3 {
		t.Fatalf("RemoveRankRange = %d, len %d", n, z.Len())
	}
	if _, ok := z.Score("b"); ok {
		t.Fatal("b still in map")
	}
	// equal scores order by member bytes
	z2 := NewZSet()
	z2.Add("banana", 1)
	z2.Add("apple", 1)
	z2.Add("cherry", 1)
	if el, _ := z2.At(0); el.Member != "apple" {
		t.Fatalf("At(0) = %q", el.Member)
	}
}

func TestParseInt64(t *testing.T) {
	if v, ok := ParseInt64("-42"); !ok || v != -42 {
		t.Fatal("-42")
	}
	for _, bad := range []string{"", "+1", "01", "-0", " 1", "1 ", "1.0", "9223372036854775808"} {
		if _, ok := ParseInt64(bad); ok {
			t.Errorf("ParseInt64(%q) succeeded", bad)
		}
	}
}
