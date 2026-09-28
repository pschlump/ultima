// Package pushq provides the growable, byte-capped outbound queue behind
// the pub/sub slow-consumer policy of all three front-ends (M9c) — the
// analogue of Redis's client-output-buffer-limit pubsub hard limit: a
// subscribed connection's queued-but-unwritten frames may grow to a byte
// ceiling (server.pubsub_client_buffer_mb, default 32 MB, Redis's own
// default), and a client that stays past the ceiling is disconnected as a
// slow consumer. It replaces the fixed 4096-message channels M0–M9b used,
// so many small messages no longer trip the limit while a few huge ones
// still do.
//
// Byte accounting is caller-supplied per item: the front-ends estimate
// frame bytes (monotone, not exact — the same philosophy as the lib/types
// memory estimators, D10) and the shared resp.Value message the broker
// builds once is never copied. One goroutine consumes each queue in every
// front-end; producers (broker Publish snapshots, connection read loops)
// are many, and may outlive the connection, so Enqueue after overflow or
// Close fails fast forever.
package pushq

import "sync"

// DefaultCapBytes is the byte ceiling used when a caller passes 0: Redis's
// client-output-buffer-limit pubsub hard-limit default of 32 MB.
const DefaultCapBytes = 32 << 20

// initItems is the initial ring capacity; the ring doubles as it grows, so
// queues that stay small never pay for the byte ceiling.
const initItems = 64

// Q is a growable FIFO with a byte ceiling. Safe for multiple producers
// and a single consumer (all three front-ends drain their queue from one
// writer goroutine).
type Q[T any] struct {
	mu       sync.Mutex
	notify   chan struct{} // cap-1 wake signal for the consumer
	items    []T           // ring buffer, head/count indexed
	sizes    []int         // per-item byte cost, parallel to items
	head     int
	count    int
	bytes    int
	cap      int
	closed   bool
	overflow bool
}

// New returns a queue with the given byte ceiling; capBytes <= 0 resolves
// to DefaultCapBytes.
func New[T any](capBytes int) *Q[T] {
	if capBytes <= 0 {
		capBytes = DefaultCapBytes
	}
	return &Q[T]{cap: capBytes, notify: make(chan struct{}, 1)}
}

// Enqueue appends item with byte cost nbytes. It never blocks. It returns
// false — now and for every subsequent call — once the queue has overflowed
// (bytes+nbytes would exceed the ceiling, which latches) or been Closed;
// the caller treats false as "close this connection". Zero-cost control
// items (close markers) never trip the ceiling, but still fail after an
// overflow or Close.
func (q *Q[T]) Enqueue(item T, nbytes int) bool {
	q.mu.Lock()
	if q.closed || q.overflow {
		q.mu.Unlock()
		return false
	}
	if q.bytes+nbytes > q.cap {
		q.overflow = true
		q.mu.Unlock()
		return false
	}
	if q.count == len(q.items) {
		q.growLocked()
	}
	idx := q.head + q.count
	if idx >= len(q.items) {
		idx -= len(q.items)
	}
	q.items[idx] = item
	q.sizes[idx] = nbytes
	q.count++
	q.bytes += nbytes
	q.mu.Unlock()
	q.signal()
	return true
}

// Dequeue removes the oldest item, blocking while the queue is empty. ok
// is false when stop fires or when the queue is Closed and drained — a
// closed queue still returns every item queued ahead of the close first
// (QUIT close-after-pending semantics depend on this).
func (q *Q[T]) Dequeue(stop <-chan struct{}) (item T, ok bool) {
	for {
		q.mu.Lock()
		if q.count > 0 {
			it, _ := q.popLocked()
			q.mu.Unlock()
			return it, true
		}
		if q.closed {
			q.mu.Unlock()
			var zero T
			return zero, false
		}
		notify := q.notify
		q.mu.Unlock()
		select {
		case <-notify:
		case <-stop:
			var zero T
			return zero, false
		}
	}
}

// TryDequeue removes the oldest item without blocking; ok is false when
// the queue is empty. It exists for teardown paths that collect (rather
// than write) the frames a dying writer left behind.
func (q *Q[T]) TryDequeue() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		var zero T
		return zero, false
	}
	it, _ := q.popLocked()
	return it, true
}

// Close wakes the consumer and fails all future Enqueues; idempotent.
// Items already queued remain dequeueable.
func (q *Q[T]) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.mu.Unlock()
	q.signal()
}

// Overflowed reports whether the byte ceiling has been tripped (latched).
func (q *Q[T]) Overflowed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.overflow
}

// Len reports the number of queued items.
func (q *Q[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// Bytes reports the queued byte total.
func (q *Q[T]) Bytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

// signal wakes a blocked Dequeue, if any; the cap-1 channel coalesces
// repeated signals, and Dequeue re-checks state under the lock on every
// wake, so no wakeup is ever missed.
func (q *Q[T]) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// growLocked doubles the ring, linearizing head..tail to index 0.
func (q *Q[T]) growLocked() {
	n := 2 * len(q.items)
	if n < initItems {
		n = initItems
	}
	items := make([]T, n)
	sizes := make([]int, n)
	for i := 0; i < q.count; i++ {
		idx := q.head + i
		if idx >= len(q.items) {
			idx -= len(q.items)
		}
		items[i] = q.items[idx]
		sizes[i] = q.sizes[idx]
	}
	q.items, q.sizes, q.head = items, sizes, 0
}

func (q *Q[T]) popLocked() (item T, nbytes int) {
	it, n := q.items[q.head], q.sizes[q.head]
	var zero T
	q.items[q.head] = zero // drop the reference so popped items can be GC'd
	q.head++
	if q.head == len(q.items) {
		q.head = 0
	}
	q.count--
	q.bytes -= n
	return it, n
}
