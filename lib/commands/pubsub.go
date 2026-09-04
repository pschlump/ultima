package commands

import (
	"fmt"
	"sort"

	"github.com/pschlump/ultima/lib/pubsub"
	"github.com/pschlump/ultima/lib/resp"
)

// --- classic pub/sub (M3, design doc §7 P2) --------------------------------
//
// SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH/PUBSUB. Sharded
// pub/sub (SSUBSCRIBE family) and keyspace notifications are deferred
// (§14.4). The subscribe-mode gate lives in Engine.Execute; this file has
// the command handlers. All semantics below were verified against Redis
// 7.2.7 (see tests/differential/scripts_m3.go).
//
// Multi-frame replies: (P)SUBSCRIBE/(P)UNSUBSCRIBE emit one ack frame per
// channel/pattern, but Execute returns a single resp.Value — the first
// ack is the command reply and the rest accumulate in cs.outbox, drained
// by the front-end after the reply (single writer ⇒ strict ordering).
// PUBLISH self-deliveries are postponed the same way: the subscriber's
// own message frames follow the :count reply (Redis
// pending_push_messages).

// subCount is the connection's total subscription count (channels +
// patterns): the third element of every subscribe/unsubscribe ack, and
// the condition for subscribe mode.
func (cs *ConnState) subCount() int64 {
	return int64(len(cs.subs) + len(cs.psubs))
}

// ensureDeliver lazily obtains the front-end push enqueue hook. It stays
// nil in unit tests that do not set StartPush; broker deliveries to this
// connection are then dropped (broker Sub.Deliver is nil-tolerant).
func (cs *ConnState) ensureDeliver() func(resp.Value) {
	if cs.deliver == nil && cs.StartPush != nil {
		cs.deliver = cs.StartPush()
	}
	return cs.deliver
}

// deferPush postpones a self-addressed publish push behind the command
// reply (appended to the outbox, drained after it).
func (cs *ConnState) deferPush(v resp.Value) {
	cs.outbox = append(cs.outbox, v)
}

// emitFrames returns the first frame and stashes the rest in the outbox.
func (cs *ConnState) emitFrames(frames []resp.Value) resp.Value {
	if len(frames) > 1 {
		cs.outbox = append(cs.outbox, frames[1:]...)
	}
	return frames[0]
}

func cmdSubscribe(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	cs.ensureDeliver()
	frames := make([]resp.Value, 0, len(args)-1)
	for _, a := range args[1:] {
		ch := string(a)
		if cs.subs == nil {
			cs.subs = map[string]struct{}{}
		}
		if _, ok := cs.subs[ch]; !ok {
			cs.subs[ch] = struct{}{}
			e.PubSub.Subscribe(ch, &pubsub.Sub{ID: cs.ID, Deliver: cs.deliver})
		}
		frames = append(frames, resp.Push(resp.BlobStr("subscribe"), resp.BlobStr(ch), resp.Int(cs.subCount())))
	}
	return cs.emitFrames(frames)
}

func cmdPSubscribe(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	cs.ensureDeliver()
	frames := make([]resp.Value, 0, len(args)-1)
	for _, a := range args[1:] {
		pat := string(a)
		if cs.psubs == nil {
			cs.psubs = map[string]struct{}{}
		}
		if _, ok := cs.psubs[pat]; !ok {
			cs.psubs[pat] = struct{}{}
			e.PubSub.PSubscribe(pat, &pubsub.Sub{ID: cs.ID, Deliver: cs.deliver})
		}
		frames = append(frames, resp.Push(resp.BlobStr("psubscribe"), resp.BlobStr(pat), resp.Int(cs.subCount())))
	}
	return cs.emitFrames(frames)
}

func cmdUnsubscribe(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return unsubscribe(e, cs, args, false)
}

func cmdPUnsubscribe(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return unsubscribe(e, cs, args, true)
}

// unsubscribe implements UNSUBSCRIBE/PUNSUBSCRIBE (verified against
// 7.2.7): with args, each arg is acked whether or not it was subscribed;
// without args every current subscription is acked, or — when there are
// none — a single ack with a null channel and count 0. The no-arg order
// is sorted for determinism (Redis iterates its hash table, so its order
// is arbitrary and untestable).
func unsubscribe(e *Engine, cs *ConnState, args [][]byte, pattern bool) resp.Value {
	kind, set := "unsubscribe", cs.subs
	if pattern {
		kind, set = "punsubscribe", cs.psubs
	}
	type target struct {
		name string
		null bool
	}
	var targets []target
	if len(args) > 1 {
		for _, a := range args[1:] {
			targets = append(targets, target{name: string(a)})
		}
	} else {
		for name := range set {
			targets = append(targets, target{name: name})
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i].name < targets[j].name })
		if len(targets) == 0 {
			targets = []target{{null: true}}
		}
	}
	frames := make([]resp.Value, 0, len(targets))
	for _, t := range targets {
		if !t.null {
			if _, ok := set[t.name]; ok {
				delete(set, t.name)
				if pattern {
					e.PubSub.PUnsubscribe(t.name, cs.ID)
				} else {
					e.PubSub.Unsubscribe(t.name, cs.ID)
				}
			}
		}
		ch := resp.BlobStr(t.name)
		if t.null {
			ch = resp.Null()
		}
		frames = append(frames, resp.Push(resp.BlobStr(kind), ch, resp.Int(cs.subCount())))
	}
	return cs.emitFrames(frames)
}

func cmdPublish(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	// Self-addressed deliveries are postponed behind the :count reply
	// via the outbox; the count includes them (Redis counts deliveries,
	// not unique clients).
	n := e.PubSub.Publish(string(args[1]), args[2], cs.ID, cs.deferPush)
	return resp.Int(int64(n))
}

// cmdPubSub implements PUBSUB CHANNELS/NUMSUB/NUMPAT (HELP is not
// implemented; unknown subcommands get Redis's error text).
func cmdPubSub(e *Engine, _ *ConnState, args [][]byte) resp.Value {
	switch sub := lowerASCII(args[1]); sub {
	case "channels":
		if len(args) > 3 {
			return resp.Err(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for '%s'. Try PUBSUB HELP.", string(args[1])))
		}
		pattern := ""
		if len(args) == 3 {
			pattern = string(args[2])
		}
		chans := e.PubSub.Channels(pattern)
		vals := make([]resp.Value, 0, len(chans))
		for _, ch := range chans {
			vals = append(vals, resp.BlobStr(ch))
		}
		return resp.Arr(vals...)
	case "numsub":
		names := make([]string, 0, len(args)-2)
		for _, a := range args[2:] {
			names = append(names, string(a))
		}
		counts := e.PubSub.NumSub(names...)
		vals := make([]resp.Value, 0, len(names)*2)
		for i, name := range names {
			vals = append(vals, resp.BlobStr(name), resp.Int(counts[i]))
		}
		return resp.Arr(vals...)
	case "numpat":
		if len(args) != 2 {
			return errArity("pubsub|numpat")
		}
		return resp.Int(e.PubSub.NumPat())
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s'. Try PUBSUB HELP.", string(args[1])))
	}
}
