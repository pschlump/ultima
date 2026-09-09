package differential

// M5a keyspace-notification scripts (design doc §14.4 M5; Redis
// notify-keyspace-events semantics, 7.2.7). conn0 runs commands (and owns
// the CONFIG SET, since a subscribed connection is gated), conn1 is the
// notification subscriber. Every frame is compared against real
// redis-server 7.2.7, so the scripts assert event ORDER and COUNT, not
// just content: a missing or spurious event desyncs the frame stream and
// fails the step. Event names/orders were probed against a live 7.2.7.
//
// Servers are shared across scripts within the suite (fresh FLUSHALL per
// script, but config persists), so every script that enables notifications
// resets notify-keyspace-events to "" as its last step.
//
// Not scripted: SET k v PXAT <past> — a known structural divergence (Redis
// stores the key and emits a lazy `expired` on reap; Ultima deletes inline
// at command time). Replies match; the event stream differs by design.

// notifyConfigScripts: CONFIG SET/GET parity for notify-keyspace-events,
// including byte-exact invalid-class errors and verbatim GET.
var notifyConfigScripts = []script{
	{"notify-config", []step{
		cmd("CONFIG", "GET", "notify-keyspace-events"), // default ""
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
		cmd("CONFIG", "GET", "notify-keyspace-events"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmd("CONFIG", "GET", "notify-keyspace-events"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "A"),
		cmd("CONFIG", "GET", "notify-keyspace-events"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "gx"),
		cmd("CONFIG", "GET", "notify-keyspace-events"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmd("CONFIG", "GET", "notify-keyspace-events"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "tmdn"),
		cmd("CONFIG", "GET", "notify-keyspace-events"),
		// Invalid class characters: byte-exact error, value untouched.
		cmd("CONFIG", "SET", "notify-keyspace-events", "B"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "q"),
		cmd("CONFIG", "SET", "notify-keyspace-events", "XYZ"),
		cmd("CONFIG", "GET", "notify-keyspace-events"), // still "tmdn"
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
}

// notifyKeyeventScripts: the __keyevent@0__ stream per command family.
var notifyKeyeventScripts = []script{
	{"notify-keyevent-strings", []step{
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		cmd("SET", "n5s", "v"),
		expectPush(1, mEq), // set
		cmd("SET", "n5s", "v2", "EX", "100"),
		expectPush(1, mEq), // set
		expectPush(1, mEq), // expire
		cmd("SET", "n5s", "v3", "KEEPTTL"),
		expectPush(1, mEq), // set only
		cmd("INCR", "n5n"),
		expectPush(1, mEq), // incrby
		cmd("DECR", "n5n"), // DECR notifies "incrby" in Redis
		expectPush(1, mEq), // incrby
		cmd("APPEND", "n5s", "x"),
		expectPush(1, mEq), // append
		cmd("MSET", "n5m1", "a", "n5m2", "b"),
		expectPush(1, mEq), // set n5m1 (arg order)
		expectPush(1, mEq), // set n5m2
		cmd("GETSET", "n5m1", "z"),
		expectPush(1, mEq), // set
		cmd("GETDEL", "n5m1"),
		expectPush(1, mEq),    // del
		cmd("GETDEL", "n5m1"), // missing: no event
		cmd("SET", "n5e", "v"),
		expectPush(1, mEq), // set (marker: proves GETDEL above emitted none)
		cmd("EXPIRE", "n5e", "100"),
		expectPush(1, mEq), // expire
		cmd("PERSIST", "n5e"),
		expectPush(1, mEq),    // persist
		cmd("PERSIST", "n5e"), // no TTL left: no event
		cmd("SET", "n5x", "v", "EX", "100"),
		expectPush(1, mEq), // set (marker for the no-op PERSIST above)
		expectPush(1, mEq), // expire
		cmd("GETEX", "n5x", "PERSIST"),
		expectPush(1, mEq),         // persist
		cmd("PEXPIRE", "n5x", "0"), // already past: immediate delete
		expectPush(1, mEq),         // del
		cmd("DEL", "n5s", "n5n", "n5gone", "n5m2"),
		expectPush(1, mEq), // del n5s
		expectPush(1, mEq), // del n5n
		expectPush(1, mEq), // del n5m2 (missing key emits none)
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
	{"notify-keyevent-collections", []step{
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		// hash
		cmd("HSET", "n5h", "f1", "v1", "f2", "v2"),
		expectPush(1, mEq),             // hset
		cmd("HSET", "n5h", "f1", "v1"), // 0 new fields: still hset
		expectPush(1, mEq),             // hset
		cmd("HINCRBY", "n5h", "n", "5"),
		expectPush(1, mEq), // hincrby
		cmd("HDEL", "n5h", "f1"),
		expectPush(1, mEq), // hdel (key survives)
		cmd("HDEL", "n5h", "f2", "n"),
		expectPush(1, mEq), // hdel
		expectPush(1, mEq), // del (hash emptied)
		// list
		cmd("LPUSH", "n5l", "a"),
		expectPush(1, mEq), // lpush
		cmd("RPUSH", "n5l", "b"),
		expectPush(1, mEq), // rpush
		cmd("LSET", "n5l", "0", "z"),
		expectPush(1, mEq), // lset
		cmd("LINSERT", "n5l", "AFTER", "z", "ins"),
		expectPush(1, mEq),             // linsert
		cmd("LTRIM", "n5l", "0", "-1"), // no-op trim: still ltrim
		expectPush(1, mEq),             // ltrim
		cmd("LREM", "n5l", "1", "ins"),
		expectPush(1, mEq), // lrem
		cmd("LPOP", "n5l"),
		expectPush(1, mEq), // lpop (one left)
		cmd("RPOP", "n5l"),
		expectPush(1, mEq), // rpop
		expectPush(1, mEq), // del (list emptied)
		// set
		cmd("SADD", "n5set", "a", "b"),
		expectPush(1, mEq), // sadd
		cmd("SREM", "n5set", "a"),
		expectPush(1, mEq), // srem (key survives)
		cmd("SPOP", "n5set"),
		expectPush(1, mEq), // spop
		expectPush(1, mEq), // del (set emptied)
		// sorted set
		cmd("ZADD", "n5z", "1", "m"),
		expectPush(1, mEq), // zadd
		cmd("ZINCRBY", "n5z", "2", "m"),
		expectPush(1, mEq), // zincr
		cmd("ZADD", "n5z", "9", "m2"),
		expectPush(1, mEq), // zadd
		cmd("ZREMRANGEBYSCORE", "n5z", "8", "10"),
		expectPush(1, mEq), // zremrangebyscore
		cmd("ZPOPMIN", "n5z"),
		expectPush(1, mEq), // zpopmin
		expectPush(1, mEq), // del (zset emptied)
		cmd("ZADD", "n5z2", "1", "a", "2", "b"),
		expectPush(1, mEq), // zadd
		cmd("ZREM", "n5z2", "a", "b"),
		expectPush(1, mEq), // zrem
		expectPush(1, mEq), // del
		// moves: dest event first, then source pop, then source del
		cmd("SADD", "n5mv1", "a"),
		expectPush(1, mEq), // sadd
		cmd("SMOVE", "n5mv1", "n5mv2", "a"),
		expectPush(1, mEq), // srem n5mv1
		expectPush(1, mEq), // del n5mv1
		expectPush(1, mEq), // sadd n5mv2
		cmd("RPUSH", "n5lm", "a"),
		expectPush(1, mEq), // rpush
		cmd("LMOVE", "n5lm", "n5ld", "LEFT", "RIGHT"),
		expectPush(1, mEq), // rpush n5ld (dest first)
		expectPush(1, mEq), // lpop n5lm
		expectPush(1, mEq), // del n5lm
		cmd("RPUSH", "n5rp", "a"),
		expectPush(1, mEq), // rpush
		cmd("RPOPLPUSH", "n5rp", "n5rd"),
		expectPush(1, mEq), // lpush n5rd (RPOPLPUSH pushes at head)
		expectPush(1, mEq), // rpop n5rp
		expectPush(1, mEq), // del n5rp
		// stores
		cmd("SADD", "n5i1", "a"),
		expectPush(1, mEq), // sadd
		cmd("SADD", "n5i2", "a", "b"),
		expectPush(1, mEq), // sadd
		cmd("SINTERSTORE", "n5id", "n5i1", "n5i2"),
		expectPush(1, mEq),                      // sinterstore
		cmd("ZUNIONSTORE", "n5zd", "1", "n5id"), // set input scores 1
		expectPush(1, mEq),                      // zunionstore
		// empty result over an existing dest: del INSTEAD of the store event
		cmd("SINTERSTORE", "n5id", "n5i1", "n5nosuch"),
		expectPush(1, mEq), // del n5id
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
	{"notify-edge-no-events", []step{
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		// SADD 0-added on an existing set: NO event (probed 7.2.7).
		cmd("SADD", "e5s", "a"),
		expectPush(1, mEq), // sadd
		cmd("SADD", "e5s", "a"),
		cmd("SREM", "e5s", "a"),
		expectPush(1, mEq), // srem (marker: no stray sadd before it)
		expectPush(1, mEq), // del
		// ZADD same-score rewrite: NO event; CH reports 0.
		cmd("ZADD", "e5z", "1", "m"),
		expectPush(1, mEq), // zadd
		cmd("ZADD", "e5z", "1", "m"),
		cmd("ZADD", "e5z", "2", "m"),
		expectPush(1, mEq), // zadd (score changed; no stray frame before)
		// ZADD XX on a missing key: NO event.
		cmd("ZADD", "e5zx", "XX", "1", "m"),
		cmd("ZADD", "e5zx", "1", "m"),
		expectPush(1, mEq), // zadd
		// ZADD INCR delta 0 on an existing member: NO event (probed).
		cmd("ZADD", "e5zi", "INCR", "0", "m"),
		cmd("ZINCRBY", "e5zi", "1", "m"),
		expectPush(1, mEq), // zincr
		// SMOVE with the member already in dst: srem+del, NO sadd (probed).
		cmd("SADD", "e5m1", "a"),
		expectPush(1, mEq), // sadd
		cmd("SADD", "e5m2", "a"),
		expectPush(1, mEq), // sadd
		cmd("SMOVE", "e5m1", "e5m2", "a"),
		expectPush(1, mEq), // srem e5m1
		expectPush(1, mEq), // del e5m1 (no sadd for e5m2)
		// LPUSHX on a missing key: NO event.
		cmd("LPUSHX", "e5l", "x"),
		cmd("RPUSH", "e5l", "x"),
		expectPush(1, mEq), // rpush
		// LPOP count 0: NO event.
		cmd("LPOP", "e5l", "0"),
		cmd("LPOP", "e5l"),
		expectPush(1, mEq), // lpop
		expectPush(1, mEq), // del
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
}

// notifyChannelScripts: the K/E channel split, class gating, and the
// keyspace-before-keyevent publish order (Redis notify.c).
var notifyChannelScripts = []script{
	{"notify-keyspace-channel", []step{
		// KA: keyspace channels only — the event name is the payload and
		// nothing may appear on keyevent channels (conn1 listens to both;
		// a spurious keyevent frame would desync the stream).
		cmd("CONFIG", "SET", "notify-keyspace-events", "KA"),
		cmdOn(1, "PSUBSCRIBE", "__keyspace@0__:*"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		cmd("SET", "k5s", "v"),
		expectPush(1, mEq), // pmessage __keyspace@0__:k5s "set"
		cmd("EXPIRE", "k5s", "100"),
		expectPush(1, mEq), // __keyspace@0__:k5s "expire"
		cmd("DEL", "k5s"),
		expectPush(1, mEq), // __keyspace@0__:k5s "del"
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
	{"notify-both-channel-order", []step{
		// KE: each event publishes on the keyspace channel FIRST, then the
		// keyevent channel (notify.c order; a both-pattern subscriber sees
		// the pair in that order).
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmdOn(1, "PSUBSCRIBE", "__keyspace@0__:*"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		cmd("SET", "b5k", "v"),
		expectPush(1, mEq), // keyspace first
		expectPush(1, mEq), // then keyevent
		cmd("DEL", "b5k"),
		expectPush(1, mEq),
		expectPush(1, mEq),
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
	{"notify-class-gating", []step{
		// Eg: keyevent channel, generic class only — string/list events
		// are dropped, del/expire pass.
		cmd("CONFIG", "SET", "notify-keyspace-events", "Eg"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		cmd("SET", "g5s", "v"),   // $ off: no frame
		cmd("LPUSH", "g5l", "a"), // l off: no frame
		cmd("EXPIRE", "g5s", "100"),
		expectPush(1, mEq), // expire (first frame: SET/LPUSH emitted none)
		cmd("DEL", "g5s", "g5l"),
		expectPush(1, mEq), // del g5s
		expectPush(1, mEq), // del g5l
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
}

// notifyMiscScripts: the expired event (active/passive expiry via
// Shard.OnKeyGone) and blocking-wake emissions.
var notifyMiscScripts = []script{
	{"notify-expired", []step{
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		cmd("SET", "x5e", "v", "PX", "60"),
		expectPush(1, mEq), // set
		expectPush(1, mEq), // expire
		// Generous slack: Redis's active cycle (hz=10) and Ultima's 100ms
		// sweep both reap the key well inside this window.
		cmd("SLEEP", "1400"),
		expectPush(1, mEq), // expired
		cmd("EXISTS", "x5e"),
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
	{"notify-blocking-wake", []step{
		// A woken BLPOP emits lpop (+del when it drains the list) from the
		// blocked connection's success path, after the pusher's rpush.
		cmd("CONFIG", "SET", "notify-keyspace-events", "KEA"),
		cmdOn(1, "PSUBSCRIBE", "__keyevent@0__:*"),
		sendOn(2, "BLPOP", "w5k", "2"),
		cmd("SLEEP", "50"),
		cmd("RPUSH", "w5k", "v"),
		expectPush(1, mEq), // rpush w5k
		recvOn(2, mEq),     // [w5k v]
		expectPush(1, mEq), // lpop w5k
		expectPush(1, mEq), // del w5k (list drained)
		// Timeout: no events at all.
		sendOn(2, "BLPOP", "w5none", "0.1"),
		recvOn(2, mEq), // null
		cmd("RPUSH", "w5k2", "v"),
		expectPush(1, mEq), // rpush w5k2 (marker: the timeout emitted none)
		// BLMOVE wake: pusher's rpush, then the move's dst push, src pop,
		// src del — in that order on every run (publish-before-wake).
		sendOn(2, "BLMOVE", "w5s", "w5d", "LEFT", "RIGHT", "2"),
		cmd("SLEEP", "50"),
		cmd("RPUSH", "w5s", "mv"),
		expectPush(1, mEq), // rpush w5s
		recvOn(2, mEq),     // mv
		expectPush(1, mEq), // rpush w5d
		expectPush(1, mEq), // lpop w5s
		expectPush(1, mEq), // del w5s
		// BZPOPMIN wake via ZADD: zadd precedes the woken pop's events.
		sendOn(2, "BZPOPMIN", "w5z", "2"),
		cmd("SLEEP", "50"),
		cmd("ZADD", "w5z", "1", "m"),
		expectPush(1, mEq), // zadd w5z
		recvOn(2, mEq),     // [w5z m 1]
		expectPush(1, mEq), // zpopmin w5z
		expectPush(1, mEq), // del w5z
		// Two parked waiters, one push of two elements: rpush, then the
		// first waiter's lpop, then the second's lpop + del — the wake
		// chain publishes each pop before signaling the next waiter.
		sendOn(2, "BLPOP", "w5c", "2"),
		cmd("SLEEP", "50"),
		sendOn(3, "BLPOP", "w5c", "2"),
		cmd("SLEEP", "50"),
		cmd("RPUSH", "w5c", "a", "b"),
		expectPush(1, mEq), // rpush w5c
		recvOn(2, mEq),     // [w5c a]
		expectPush(1, mEq), // lpop w5c (first waiter)
		recvOn(3, mEq),     // [w5c b]
		expectPush(1, mEq), // lpop w5c (second waiter)
		expectPush(1, mEq), // del w5c
		cmd("CONFIG", "SET", "notify-keyspace-events", ""),
	}},
}

// m5Scripts is the M5a differential table.
var m5Scripts = func() []script {
	out := make([]script, 0, len(notifyConfigScripts)+len(notifyKeyeventScripts)+
		len(notifyChannelScripts)+len(notifyMiscScripts))
	out = append(out, notifyConfigScripts...)
	out = append(out, notifyKeyeventScripts...)
	out = append(out, notifyChannelScripts...)
	out = append(out, notifyMiscScripts...)
	return out
}()
