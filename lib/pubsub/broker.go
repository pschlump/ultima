// Package pubsub implements Ultima's classic (non-sharded) pub/sub broker
// (design doc §7 P2, M3). Channels are global, not per-DB, matching
// Redis. Sharded pub/sub (SSUBSCRIBE/SPUBLISH) is deferred to M8 and
// keyspace notifications to M5.
//
// The broker spawns no goroutines: Publish runs on the publisher's
// connection goroutine and hands each message to the recipient's Sub.
// Deliver function, which by contract is a non-blocking enqueue onto the
// recipient connection's push queue (lib/respserver).
package pubsub

import (
	"sort"
	"sync"

	"github.com/pschlump/ultima/lib/resp"
)

// Sub is one client's registration on a channel or pattern. Deliver must
// be a non-blocking enqueue that stays safe to call after the client is
// gone (the broker may hold a snapshot across a concurrent close); a nil
// Deliver drops messages (unit tests without a push front-end).
type Sub struct {
	ID      uint64
	Deliver func(resp.Value)
}

// Broker is the mutex-guarded registry of channel and pattern
// subscriptions.
type Broker struct {
	// Match is Redis stringmatchlen glob matching ('*', '?', classes);
	// injected by the caller (lib/commands.GlobMatch) to keep this
	// package dependency-free of the command layer.
	Match func(pattern, s string) bool

	mu       sync.Mutex
	channels map[string]map[uint64]*Sub
	patterns map[string]map[uint64]*Sub
}

// New returns an empty Broker using match for glob pattern matching.
func New(match func(pattern, s string) bool) *Broker {
	return &Broker{
		Match:    match,
		channels: map[string]map[uint64]*Sub{},
		patterns: map[string]map[uint64]*Sub{},
	}
}

// Subscribe registers s on channel (idempotent per ID).
func (b *Broker) Subscribe(channel string, s *Sub) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.channels[channel]
	if m == nil {
		m = map[uint64]*Sub{}
		b.channels[channel] = m
	}
	m[s.ID] = s
}

// Unsubscribe removes id from channel.
func (b *Broker) Unsubscribe(channel string, id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if m := b.channels[channel]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(b.channels, channel)
		}
	}
}

// PSubscribe registers s on pattern (idempotent per ID).
func (b *Broker) PSubscribe(pattern string, s *Sub) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.patterns[pattern]
	if m == nil {
		m = map[uint64]*Sub{}
		b.patterns[pattern] = m
	}
	m[s.ID] = s
}

// PUnsubscribe removes id from pattern.
func (b *Broker) PUnsubscribe(pattern string, id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if m := b.patterns[pattern]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(b.patterns, pattern)
		}
	}
}

// UnsubscribeAll removes every channel and pattern registration of id
// (connection close, RESET) and returns what was removed, sorted, for
// callers that generate acks.
func (b *Broker) UnsubscribeAll(id uint64) (channels, patterns []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch, m := range b.channels {
		if _, ok := m[id]; ok {
			delete(m, id)
			channels = append(channels, ch)
			if len(m) == 0 {
				delete(b.channels, ch)
			}
		}
	}
	for pat, m := range b.patterns {
		if _, ok := m[id]; ok {
			delete(m, id)
			patterns = append(patterns, pat)
			if len(m) == 0 {
				delete(b.patterns, pat)
			}
		}
	}
	sort.Strings(channels)
	sort.Strings(patterns)
	return channels, patterns
}

// Publish delivers payload to every subscriber of channel (a "message"
// push) and to every subscriber of a matching pattern (a "pmessage"
// push), returning the number of deliveries. Redis counts deliveries,
// not unique clients: a client subscribed to both the channel and a
// matching pattern (or to two matching patterns) receives — and counts —
// twice.
//
// Recipients are snapshotted under the lock; delivery happens outside
// it. Deliveries to the subscriber whose ID is selfID go to self instead
// of Sub.Deliver, so the publishing connection can append them after its
// own command reply (Redis pending_push_messages semantics: the PUBLISH
// reply precedes self-addressed pushes); self may be nil.
func (b *Broker) Publish(channel string, payload []byte, selfID uint64, self func(resp.Value)) int {
	msg := resp.Push(resp.BlobStr("message"), resp.BlobStr(channel), resp.BlobString(payload))

	b.mu.Lock()
	direct := make([]*Sub, 0, len(b.channels[channel]))
	for _, s := range b.channels[channel] {
		direct = append(direct, s)
	}
	type patHit struct {
		pattern string
		subs    []*Sub
	}
	var pats []patHit
	for pat, m := range b.patterns {
		if !b.Match(pat, channel) {
			continue
		}
		hit := patHit{pattern: pat, subs: make([]*Sub, 0, len(m))}
		for _, s := range m {
			hit.subs = append(hit.subs, s)
		}
		pats = append(pats, hit)
	}
	sort.Slice(pats, func(i, j int) bool { return pats[i].pattern < pats[j].pattern })
	b.mu.Unlock()

	deliver := func(s *Sub, v resp.Value) {
		switch {
		case s.ID == selfID:
			if self != nil {
				self(v)
			}
		case s.Deliver != nil:
			s.Deliver(v)
		}
	}
	n := 0
	for _, s := range direct {
		deliver(s, msg)
		n++
	}
	for _, hit := range pats {
		pmsg := resp.Push(resp.BlobStr("pmessage"), resp.BlobStr(hit.pattern),
			resp.BlobStr(channel), resp.BlobString(payload))
		for _, s := range hit.subs {
			deliver(s, pmsg)
			n++
		}
	}
	return n
}

// Channels returns the channels that currently have at least one
// subscriber, sorted; pattern filters by glob ("" = all).
func (b *Broker) Channels(pattern string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.channels))
	for ch := range b.channels {
		if pattern == "" || b.Match(pattern, ch) {
			out = append(out, ch)
		}
	}
	sort.Strings(out)
	return out
}

// NumSub reports the subscriber count of each channel (0 when none);
// pattern subscriptions are not counted.
func (b *Broker) NumSub(channels ...string) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int64, len(channels))
	for i, ch := range channels {
		out[i] = int64(len(b.channels[ch]))
	}
	return out
}

// NumPat reports the number of patterns with at least one subscriber.
func (b *Broker) NumPat() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(len(b.patterns))
}

// NumChannels reports the number of channels with at least one
// subscriber (INFO pubsub_channels).
func (b *Broker) NumChannels() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(len(b.channels))
}
