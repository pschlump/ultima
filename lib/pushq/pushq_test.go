package pushq

import (
	"sync"
	"testing"
	"time"
)

// TestFIFOOrder: items come out in enqueue order across ring wraparound
// and growth.
func TestFIFOOrder(t *testing.T) {
	q := New[int](1 << 20)
	const n = 5000 // well past initItems, forces several growth doublings
	for i := 0; i < n; i++ {
		if !q.Enqueue(i, 1) {
			t.Fatalf("enqueue %d failed", i)
		}
	}
	// Pop some, push more, so the ring head is mid-buffer.
	stop := make(chan struct{})
	for i := 0; i < n/2; i++ {
		got, ok := q.Dequeue(stop)
		if !ok || got != i {
			t.Fatalf("dequeue = %d, %v; want %d, true", got, ok, i)
		}
	}
	for i := n; i < n+n/2; i++ {
		q.Enqueue(i, 1)
	}
	for i := n / 2; i < n+n/2; i++ {
		got, ok := q.Dequeue(stop)
		if !ok || got != i {
			t.Fatalf("dequeue = %d, %v; want %d, true", got, ok, i)
		}
	}
	if q.Len() != 0 || q.Bytes() != 0 {
		t.Fatalf("drained queue has len %d bytes %d, want 0/0", q.Len(), q.Bytes())
	}
}

// TestByteAccountingExact: enqueues succeed up to exactly the byte cap;
// one more byte overflows and latches.
func TestByteAccountingExact(t *testing.T) {
	q := New[int](100)
	if !q.Enqueue(1, 60) || !q.Enqueue(2, 40) {
		t.Fatal("enqueue to exactly cap failed")
	}
	if q.Bytes() != 100 {
		t.Fatalf("bytes = %d, want 100", q.Bytes())
	}
	if q.Enqueue(3, 1) {
		t.Fatal("enqueue one byte past cap succeeded")
	}
	if !q.Overflowed() {
		t.Fatal("Overflowed = false after tripping the cap")
	}
	// Latched: even a zero-cost item now fails.
	if q.Enqueue(4, 0) {
		t.Fatal("enqueue after overflow succeeded")
	}
	// Already-queued items are still delivered.
	stop := make(chan struct{})
	for _, want := range []int{1, 2} {
		got, ok := q.Dequeue(stop)
		if !ok || got != want {
			t.Fatalf("dequeue = %d, %v; want %d, true", got, ok, want)
		}
	}
}

// TestGrowthBeyondOldCap: the queue grows well past the old fixed
// 4096-message cap when the byte ceiling allows it.
func TestGrowthBeyondOldCap(t *testing.T) {
	const n = 3 * 4096
	q := New[int](n) // 1 byte per item
	for i := 0; i < n; i++ {
		if !q.Enqueue(i, 1) {
			t.Fatalf("enqueue %d of %d failed", i, n)
		}
	}
	if q.Len() != n || q.Bytes() != n {
		t.Fatalf("len/bytes = %d/%d, want %d/%d", q.Len(), q.Bytes(), n, n)
	}
}

// TestCloseWakesDequeue: Close unblocks a consumer waiting on an empty
// queue, and the closed queue still drains queued items first.
func TestCloseWakesDequeue(t *testing.T) {
	q := New[int](0)
	stop := make(chan struct{})
	q.Enqueue(7, 1)
	q.Close()

	// Drain-after-close: the queued item comes back first.
	if got, ok := q.Dequeue(stop); !ok || got != 7 {
		t.Fatalf("dequeue after close = %d, %v; want 7, true", got, ok)
	}
	// Drained and closed: immediate false, no block.
	if _, ok := q.Dequeue(stop); ok {
		t.Fatal("dequeue on drained closed queue returned ok")
	}
	// Enqueue after close fails fast.
	if q.Enqueue(8, 1) {
		t.Fatal("enqueue after close succeeded")
	}
	// Close is idempotent.
	q.Close()

	// A blocked Dequeue wakes on Close.
	q2 := New[int](0)
	done := make(chan bool, 1)
	go func() {
		_, ok := q2.Dequeue(stop)
		done <- ok
	}()
	time.Sleep(20 * time.Millisecond) // let the goroutine park
	q2.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("blocked dequeue on closed queue returned ok")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake a blocked Dequeue")
	}
}

// TestStopAbortsDequeue: a fired stop channel ends a blocked Dequeue even
// though the queue is open.
func TestStopAbortsDequeue(t *testing.T) {
	q := New[int](0)
	stop := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		_, ok := q.Dequeue(stop)
		done <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("stopped dequeue returned ok")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not wake a blocked Dequeue")
	}
}

// TestConcurrentProducers: many goroutines enqueue while one consumer
// drains; every byte is accounted and nothing is lost. Run with -race.
func TestConcurrentProducers(t *testing.T) {
	const producers = 8
	const perProducer = 2000
	q := New[int](producers * perProducer) // 1 byte per item: exact fit
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if !q.Enqueue(base+i, 1) {
					t.Errorf("enqueue failed at %d", base+i)
					return
				}
			}
		}(p * perProducer)
	}
	stop := make(chan struct{})
	sum := 0
	for got := 0; got < producers*perProducer; got++ {
		v, ok := q.Dequeue(stop)
		if !ok {
			t.Fatalf("dequeue %d returned not-ok on an open queue", got)
		}
		sum += v
	}
	wg.Wait()
	want := producers * perProducer * (producers*perProducer - 1) / 2
	if sum != want {
		t.Fatalf("sum = %d, want %d (items lost or duplicated)", sum, want)
	}
	if q.Bytes() != 0 {
		t.Fatalf("bytes = %d after full drain, want 0", q.Bytes())
	}
}
