package commands

// Pipelined-burst coalescing (M9b, design doc §4.1). redis-benchmark-style
// clients pipeline N commands per read burst; executing each through its
// own shard handoff costs two goroutine switches per command and, when
// every key lands on ONE shard (redis-benchmark's default fixed key),
// serializes the whole burst on a ping-pong with that shard's goroutine.
//
// ExecuteBurst runs the same per-command Execute logic — every gate
// (NOAUTH, arity, subscribe mode, BUSY, OOM, MULTI queueing) and every
// side effect (AOF capture, keyspace notifications, MONITOR, slowlog) is
// untouched — but executes maximal segments of whitelisted single-key
// commands as one fan-out: the segment's commands are grouped by shard
// (preserving per-shard order), each group is one shard task submitted
// asynchronously, and inside a task do() runs inline (cs.inls slot). A
// 16-command burst costs one parallel round of handoffs instead of sixteen
// serial ones; a same-shard burst is a single task. Per-shard groups are
// atomic per shard against other connections, and per-key order is
// identical to sequential execution — a later command in a burst can only
// observe an earlier command's effect through a shared key, and shared
// keys share a shard, where order is preserved.
//
// Whitelist discipline: a command is eligible only when its handler is
// verified to touch exactly one key (args[1]) through e.do — no doMulti,
// no PauseAll, no blocking, no ConnState mutation, no outbox frames.
// Anything else breaks the segment and executes on the normal path, so
// ordering and state transitions are preserved exactly.

import (
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// burstInlineable names the audited single-key handlers (see the package
// comment): the strings family plus the single-key keyspace expiry/probe
// commands. Extend only after verifying the handler uses e.do exclusively.
var burstInlineable = map[string]struct{}{
	"get": {}, "set": {}, "getset": {}, "getdel": {}, "getex": {},
	"incr": {}, "decr": {}, "incrby": {}, "decrby": {}, "incrbyfloat": {},
	"append": {}, "strlen": {},
	"expire": {}, "pexpire": {}, "expireat": {}, "pexpireat": {},
	"persist": {}, "ttl": {}, "pttl": {}, "type": {},
}

// ExecuteBurst runs firstArgs plus every command in rest, coalescing
// whitelisted segments into per-shard group tasks, and returns every reply
// (and any outbox frame) in issue order. The int result is the count of
// rest commands consumed — always len(rest); the redcon loop drops them
// from its pipeline slice.
//
// Coalescing is disabled when it could change semantics or deadlock:
//   - maxmemory configured: the OOM gate inside Execute can call EvictNow,
//     which submits a task to EVERY shard and waits — a self-deadlock
//     inside a shard goroutine. (Eviction configurations keep the
//     per-command path; making EvictNow shard-local is future work.)
//   - cs.Multi: queued commands append to cs.Queue; concurrent group
//     tasks would race on it. (MULTI/EXEC/DISCARD themselves are not
//     whitelisted and break segments, so this only matters for bursts
//     that begin inside a transaction.)
func (e *Engine) ExecuteBurst(cs *ConnState, firstArgs [][]byte, rest []resp.Command) ([]resp.Value, int) {
	if len(rest) == 0 || e.MaxMemory() > 0 {
		return []resp.Value{e.Execute(cs, firstArgs)}, 0
	}

	m := len(rest) + 1
	argsAt := func(k int) [][]byte {
		if k == 0 {
			return firstArgs
		}
		return rest[k-1].Args
	}

	vals := make([]resp.Value, 0, m)
	for k := 0; k < m; {
		// A segment starts at k: the longest run of whitelisted commands.
		// cs.Multi is re-checked per segment — an unqueued MULTI earlier
		// in the burst flips it for everything after.
		end := k
		if !cs.Multi {
			for end < m && e.burstEligible(argsAt(end)) {
				end++
			}
		}
		switch segLen := end - k; segLen {
		case 0, 1: // not eligible, or alone: normal per-command path
			vals = append(vals, e.Execute(cs, argsAt(k)))
			vals = append(vals, cs.DrainOutbox()...)
			k++
		default:
			e.execSegment(cs, argsAt, k, end, &vals)
			k = end
		}
	}
	return vals, len(rest)
}

// execSegment runs the whitelisted segment [start, end) as per-shard
// group tasks submitted concurrently, appending replies (in segment
// order) plus any outbox frames to vals.
func (e *Engine) execSegment(cs *ConnState, argsAt func(int) [][]byte, start, end int, vals *[]resp.Value) {
	if cs.inls == nil {
		cs.inls = make([]*shard.Shard, e.Shards.ShardCount())
	}
	segLen := end - start
	if cap(cs.burstOut) < segLen {
		cs.burstOut = make([]resp.Value, segLen)
	}
	out := cs.burstOut[:segLen] // slot-indexed; groups never alias

	groups := cs.burstGroups // shard → segment positions, in order
	if groups == nil {
		groups = make(map[int][]int, 8)
		cs.burstGroups = groups
	}
	for si, pos := range groups { // keep backing arrays across bursts
		groups[si] = pos[:0]
	}
	for i := 0; i < segLen; i++ {
		si := e.Shards.ShardIndex(argsAt(start + i)[1])
		groups[si] = append(groups[si], i)
	}

	dones := cs.burstDones[:0]
	for si, positions := range groups {
		if len(positions) == 0 { // stale entry from a previous burst
			delete(groups, si)
			continue
		}
		done := e.Shards.DoTokAsync(cs.tok, si, func(s *shard.Shard) {
			cs.inls[si] = s
			defer func() { cs.inls[si] = nil }()
			for _, p := range positions {
				out[p] = e.Execute(cs, argsAt(start+p))
			}
		})
		dones = append(dones, done)
	}
	cs.burstDones = dones
	for _, done := range dones {
		<-done
		shard.PutDone(done)
	}
	*vals = append(*vals, out...)
	// Whitelisted commands emit no outbox frames; drain defensively so
	// nothing can leak into a later reply.
	*vals = append(*vals, cs.DrainOutbox()...)
}

// burstEligible reports whether a command is burst-eligible: whitelisted
// and well-formed enough to name its single key.
func (e *Engine) burstEligible(args [][]byte) bool {
	if len(args) < 2 {
		return false
	}
	_, ok := burstInlineable[lowerASCII(args[0])]
	return ok
}
