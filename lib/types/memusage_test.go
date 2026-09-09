package types

// MemUsage accounting tests (M5b, D10): the tracked content bytes must
// follow every mutation path exactly — eviction quotas are only as good
// as these counters. Absolute overheads are estimates and not asserted;
// the deltas are exact.

import (
	"strconv"
	"testing"
)

func TestHashMemUsage(t *testing.T) {
	h := NewHash()
	base := h.MemUsage()
	h.Set("aa", "bb") // +4 content bytes, +1 pair
	one := h.MemUsage()
	if one <= base {
		t.Fatalf("MemUsage after Set = %d, want > %d", one, base)
	}
	h.Set("aa", "bbbbbb") // replace: +4 content bytes, no new pair
	if got := h.MemUsage(); got != one+4 {
		t.Fatalf("MemUsage after replace = %d, want %d", got, one+4)
	}
	// Promotion raises the estimate (map slots) without losing content.
	for i := 0; i < HashMaxListpackEntries+1; i++ {
		h.Set("f"+strconv.Itoa(i), "v")
	}
	if !h.Big() {
		t.Fatal("hash did not promote")
	}
	big := h.MemUsage()
	h.Del("aa") // field "aa" (2) + replaced value "bbbbbb" (6) = 8 content bytes
	if got := h.MemUsage(); got != big-8-hashPairCost-hashMapSlot {
		t.Errorf("MemUsage after Del = %d, want %d", got, big-8-hashPairCost-hashMapSlot)
	}
}

func TestSetMemUsage(t *testing.T) {
	s := NewSet()
	base := s.MemUsage()
	s.Add("12345") // intset: +8, no content tracking
	if got := s.MemUsage(); got != base+setIntsetElem {
		t.Fatalf("intset MemUsage = %d, want %d", got, base+setIntsetElem)
	}
	s.Remove("12345")
	if got := s.MemUsage(); got != base {
		t.Fatalf("intset MemUsage after remove = %d, want %d", got, base)
	}
	s.Add("not-an-int") // promotes to the map encoding
	mapBase := s.MemUsage()
	s.Add("xyz")
	if got := s.MemUsage(); got != mapBase+setMapSlot+3 {
		t.Fatalf("map MemUsage after Add = %d, want %d", got, mapBase+setMapSlot+3)
	}
	s.Remove("xyz")
	if got := s.MemUsage(); got != mapBase {
		t.Fatalf("map MemUsage after Remove = %d, want %d", got, mapBase)
	}
}

func TestZSetMemUsage(t *testing.T) {
	z := NewZSet()
	base := z.MemUsage()
	z.Add("member1", 1.5)
	one := z.MemUsage()
	if got := one - base; got != zsetElem+7 {
		t.Fatalf("MemUsage delta after Add = %d, want %d", got, zsetElem+7)
	}
	z.Add("member1", 2.5) // score update: no size change
	if got := z.MemUsage(); got != one {
		t.Fatalf("MemUsage after score update = %d, want %d", got, one)
	}
	z.Remove("member1")
	if got := z.MemUsage(); got != base {
		t.Fatalf("MemUsage after Remove = %d, want %d", got, base)
	}
	// RemoveRankRange accounts every removed member.
	for _, m := range []string{"aa", "bbbb", "c"} {
		z.Add(m, 1)
	}
	full := z.MemUsage()
	if n := z.RemoveRankRange(0, 2); n != 3 {
		t.Fatalf("RemoveRankRange removed %d, want 3", n)
	}
	if got := z.MemUsage(); got != full-3*zsetElem-7 {
		t.Errorf("MemUsage after RemoveRankRange = %d, want %d", got, full-3*zsetElem-7)
	}
}

func TestListMemUsage(t *testing.T) {
	l := NewList()
	base := l.MemUsage()
	l.PushHead([]byte("abc"))
	l.PushTail([]byte("12345"))
	two := l.MemUsage()
	if got := two - base; got != 2*listElem+8 {
		t.Fatalf("MemUsage delta after 2 pushes = %d, want %d", got, 2*listElem+8)
	}
	l.Set(0, []byte("abcdef")) // +3 content bytes
	if got := l.MemUsage(); got != two+3 {
		t.Fatalf("MemUsage after Set = %d, want %d", got, two+3)
	}
	if _, ok := l.PopHead(); !ok {
		t.Fatal("PopHead failed")
	}
	if got := l.MemUsage(); got != two+3-listElem-6 {
		t.Fatalf("MemUsage after PopHead = %d, want %d", got, two+3-listElem-6)
	}
	l.InsertAfter(0, []byte("xx"))
	mid := l.MemUsage()
	l.Delete(1) // removes "xx"
	if got := l.MemUsage(); got != mid-listElem-2 {
		t.Fatalf("MemUsage after Delete = %d, want %d", got, mid-listElem-2)
	}
	// Trim recomputes from the survivors.
	l.PushTail([]byte("tail-bytes"))
	l.Trim(0, 0) // keep one 5-byte element
	if got := l.MemUsage(); got != base+listElem+5 {
		t.Fatalf("MemUsage after Trim = %d, want %d", got, base+listElem+5)
	}
}
