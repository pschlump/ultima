package differential

// M3 transaction scripts (§7): MULTI/EXEC/DISCARD/WATCH/UNWATCH/RESET.
// The harness is single-connection, so WATCH dirty-abort paths (needing a
// second connection) live in lib/commands/tx_test.go; these scripts cover
// the queueing, EXECABORT, error-element, and RESET flows.

var m3TxScripts = []script{
	{"tx-basic", []step{
		cmd("MULTI"),
		cmd("SET", "ta", "1"),
		cmd("GET", "ta"),
		cmd("INCR", "ta"),
		cmd("EXEC"),
		cmd("GET", "ta"),
		cmd("EXEC"),    // without MULTI
		cmd("DISCARD"), // without MULTI
		// empty queue commits as an empty array
		cmd("MULTI"),
		cmd("EXEC"),
	}},
	{"tx-queue-error-unknown", []step{
		cmd("MULTI"),
		cmd("NOSUCHCMD", "x", "y"),
		cmd("SET", "tk", "v"),
		cmd("EXEC"), // EXECABORT
		cmd("GET", "tk"),
		cmd("EXEC"), // state fully cleared
	}},
	{"tx-queue-error-arity", []step{
		cmd("MULTI"),
		cmd("GET", "a", "b"),
		cmd("EXEC"), // EXECABORT
		cmd("EXEC"),
	}},
	{"tx-error-element", []step{
		cmd("SET", "bad", "abc"),
		cmd("MULTI"),
		cmd("INCR", "bad"),
		cmd("SET", "after", "1"),
		cmd("INCR", "n"),
		cmd("EXEC"),
		cmd("GET", "after"),
		cmd("GET", "n"),
		cmd("GET", "bad"),
	}},
	{"tx-nested-and-discard", []step{
		cmd("MULTI"),
		cmd("MULTI"), // nested: error, does not dirty
		cmd("SET", "nd", "1"),
		cmd("EXEC"),
		cmd("GET", "nd"),
		cmd("MULTI"),
		cmd("SET", "nd", "2"),
		cmd("DISCARD"),
		cmd("GET", "nd"),
		cmd("EXEC"), // without MULTI after DISCARD
	}},
	{"tx-watch-unwatch", []step{
		cmd("SET", "w", "1"),
		cmd("WATCH", "w", "missing"),
		cmd("MULTI"),
		cmd("SET", "w", "2"),
		cmd("EXEC"), // clean: applies
		cmd("GET", "w"),
		cmd("WATCH", "w"),
		cmd("UNWATCH"),
		cmd("MULTI"),
		cmd("WATCH", "w"), // error inside MULTI, not queued, does not dirty
		cmd("UNWATCH"),    // queued
		cmd("SET", "w", "3"),
		cmd("EXEC"),
		cmd("GET", "w"),
		cmd("WATCH"), // arity
	}},
	{"tx-mixed-types", []step{
		cmd("MULTI"),
		cmd("HSET", "mh", "f1", "v1", "f2", "v2"),
		cmd("LPUSH", "ml", "a", "b"),
		cmd("SADD", "ms", "x", "y"),
		cmd("ZADD", "mz", "1.5", "m"),
		cmd("MSET", "mk1", "v1", "mk2", "v2"),
		cmd("MGET", "mk1", "mk2"),
		cmd("EXEC"),
		cmd("HGETALL", "mh"),
		cmd("LRANGE", "ml", "0", "-1"),
		cmdM(mSetCmp, "SMEMBERS", "ms"),
		cmd("ZSCORE", "mz", "m"),
	}},
	{"tx-reset", []step{
		cmd("CLIENT", "SETNAME", "txr"),
		cmd("SELECT", "1"),
		cmd("SET", "rk", "in-db1"),
		cmd("MULTI"),
		cmd("SET", "queued", "1"),
		cmd("RESET"), // discards the transaction
		cmd("EXEC"),  // without MULTI
		cmd("GET", "queued"),
		cmd("CLIENT", "GETNAME"),
		cmd("GET", "rk"), // back on db0: gone
		cmd("SELECT", "1"),
		cmd("GET", "rk"),
		cmd("DBSIZE"),
		cmd("RESET"),
		cmd("DBSIZE"),     // db0
		cmd("RESET", "x"), // arity
	}},
	{"tx-resp3", []step{
		cmdM(mHello, "HELLO", "3"),
		cmd("MULTI"),
		cmd("HSET", "r3h", "f", "v"),
		cmd("HGETALL", "r3h"),
		cmd("EXEC"),
		cmd("HGETALL", "r3h"), // RESP3 map
		cmd("RESET"),          // reverts to RESP2 on both servers
		cmd("HGETALL", "r3h"), // RESP2 flat array
	}},
}

// M3 pub/sub scripts (§7): SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/
// PUNSUBSCRIBE/PUBLISH/PUBSUB. Multi-connection: conn1 is the
// subscriber, conn0 the publisher/observer. Each script uses its own
// channel names so leftover subscriptions from an earlier script (a conn
// closes only at subtest end) can never interfere. Exact strings/shapes
// were probed against redis-server 7.2.7 before encoding here.
var m3PubSubScripts = []script{
	{"ps-ack-shapes", []step{
		cmdOn(1, "SUBSCRIBE", "ack1"),            // ["subscribe","ack1",1]
		sendOn(1, "SUBSCRIBE", "ack2", "ack3"),   // multi-arg: two frames
		recvOn(1, mEq),                           // ack2 :2
		recvOn(1, mEq),                           // ack3 :3
		cmdOn(1, "SUBSCRIBE", "ack1"),            // duplicate: ack, count unchanged
		cmdOn(1, "PSUBSCRIBE", "ackp*"),          // ["psubscribe","ackp*",4]
		cmdOnM(mSetCmp, 0, "PUBSUB", "CHANNELS"), // ack1 ack2 ack3
		cmdOnM(mEq, 0, "PUBSUB", "NUMPAT"),       // :1
	}},
	{"ps-unsubscribe", []step{
		cmdOn(1, "SUBSCRIBE", "u1", "u2"),
		recvOn(1, mEq),                   // second subscribe ack
		cmdOn(1, "UNSUBSCRIBE", "u1"),    // arg form
		cmdOn(1, "UNSUBSCRIBE", "ghost"), // not subscribed: acked anyway
		cmdOn(1, "UNSUBSCRIBE"),          // no-arg: acks u2, count 0
		cmdOn(1, "UNSUBSCRIBE"),          // none left: null-channel ack
		cmdOn(1, "PUNSUBSCRIBE"),         // null-channel ack (patterns)
		cmdOn(1, "PSUBSCRIBE", "pu1*", "pu2*"),
		recvOn(1, mEq),
		sendOn(1, "PUNSUBSCRIBE", "pu1*", "zz*"), // mixed: subscribed + not
		recvOn(1, mEq),
		recvOn(1, mEq),
		cmdOnM(mEq, 0, "PUBSUB", "NUMPAT"), // :1
	}},
	{"ps-publish-delivery", []step{
		cmdOn(1, "SUBSCRIBE", "dl"),
		cmdOn(0, "PUBLISH", "dl", "m1"),      // :1
		expectPush(1, mEq),                   // ["message","dl","m1"]
		cmdOn(0, "PUBLISH", "dl-quiet", "x"), // :0
		cmdOn(1, "PSUBSCRIBE", "dl*"),
		cmdOn(0, "PUBLISH", "dl", "m2"), // :2 (channel + pattern, same client)
		expectPush(1, mEq),              // message
		expectPush(1, mEq),              // pmessage
		cmdOn(2, "SUBSCRIBE", "dl"),
		cmdOn(0, "PUBLISH", "dl", "m3"), // :3
		expectPush(1, mEq),
		expectPush(1, mEq),
		expectPush(2, mEq),
		cmdOnM(mEq, 0, "PUBSUB", "NUMSUB", "dl", "dl-quiet"), // [dl 2 dl-quiet 0]
	}},
	{"ps-resp3", []step{
		cmdOnM(mHello, 1, "HELLO", "3"), // conn1 negotiates RESP3
		cmdOn(1, "SUBSCRIBE", "r3"),     // ack as push frame
		cmdOn(0, "PUBLISH", "r3", "m"),  // conn0 stays RESP2
		expectPush(1, mEq),              // >3 message frame
		cmdOn(1, "PSUBSCRIBE", "r3*"),
		cmdOn(0, "PUBLISH", "r3", "m2"), // :2
		expectPush(1, mEq),
		expectPush(1, mEq),
		cmdOn(1, "GET", "nokey"), // RESP3: not gated
		cmdOn(1, "PING"),         // RESP3: plain +PONG
		cmdOn(1, "PING", "hi"),   // RESP3: bulk
		// Self-publish: :count reply first, postponed push after.
		cmdOn(1, "PUBLISH", "r3", "self"), // :2 (chan + pattern)
		expectPush(1, mEq),
		expectPush(1, mEq),
	}},
	{"ps-subscribe-mode-gate", []step{
		cmdOn(1, "SUBSCRIBE", "g1"),
		cmdOn(1, "GET", "x"),                   // gate error, lowercase name
		cmdOn(1, "CONFIG", "GET", "maxmemory"), // 'config|get' fullname
		cmdOn(1, "PUBSUB", "NUMSUB", "g1"),     // 'pubsub|numsub' fullname
		cmdOn(1, "PUBSUB", "BOGUS"),            // unknown subcmd beats gate
		cmdOn(1, "GET"),                        // arity beats gate
		cmdOn(1, "NOSUCHCMD"),                  // unknown command beats gate
		cmdOn(1, "MULTI"),                      // gated
		cmdOn(1, "EXEC"),                       // EXECABORT wrapping the gate error
		cmdOn(1, "PING"),                       // ["pong",""] in sub mode
		cmdOn(1, "PING", "hello"),              // ["pong","hello"]
		cmdOn(1, "UNSUBSCRIBE"),                // lifts the gate
		cmdOn(1, "GET", "x"),                   // runs again
	}},
	{"ps-pubsub-introspection", []step{
		cmdOn(1, "SUBSCRIBE", "in1", "in2"),
		recvOn(1, mEq),
		cmdOn(2, "SUBSCRIBE", "in1"),
		cmdOn(1, "PSUBSCRIBE", "inp*"),
		cmdOnM(mSetCmp, 0, "PUBSUB", "CHANNELS"),        // in1 in2, unordered
		cmdOnM(mSetCmp, 0, "PUBSUB", "CHANNELS", "in*"), // glob filter
		cmdOnM(mEq, 0, "PUBSUB", "CHANNELS", "zz*"),     // no match: empty
		cmdOnM(mEq, 0, "PUBSUB", "NUMSUB"),              // bare: empty array
		cmdOnM(mEq, 0, "PUBSUB", "NUMSUB", "in1", "in2", "none"),
		cmdOnM(mEq, 0, "PUBSUB", "NUMPAT"),       // :1
		cmdOn(0, "PUBSUB", "BOGUS"),              // unknown subcommand
		cmdOn(0, "PUBSUB", "CHANNELS", "a", "b"), // subcommand syntax error
		cmdOn(0, "PUBSUB", "NUMPAT", "x"),        // subcommand arity
		cmdOn(0, "PUBSUB"),                       // container arity
	}},
	{"ps-reset", []step{
		cmdOn(1, "SUBSCRIBE", "rs1"),
		cmdOn(1, "PSUBSCRIBE", "rs*"),
		cmdOn(1, "RESET"),                         // +RESET, clears subs
		cmdOnM(mEq, 0, "PUBSUB", "CHANNELS"),      // empty
		cmdOnM(mEq, 0, "PUBSUB", "NUMPAT"),        // :0
		cmdOn(1, "GET", "x"),                      // gate lifted
		cmdOn(0, "PUBLISH", "rs1", "after-reset"), // :0
	}},
}

// m3Scripts is the M3 differential table; later M3 parts (blocking)
// append their scripts here.
var m3Scripts = func() []script {
	out := make([]script, 0, len(m3TxScripts)+len(m3PubSubScripts)+len(m3BlockScripts))
	out = append(out, m3TxScripts...)
	out = append(out, m3PubSubScripts...)
	out = append(out, m3BlockScripts...)
	return out
}()

// M3 blocking scripts (§7): BLPOP/BRPOP/BLMOVE/BLMPOP/BRPOPLPUSH and
// BZPOPMIN/BZPOPMAX/BZMPOP. Immediate-data paths, key priority, parse
// errors (strings probed against 7.2.7), a handful of ≤0.1s timeout
// nulls, wake-across-connections via sendOn/recvOn, blocking-inside-MULTI,
// and RESP3 score shapes on a HELLO 3 connection.
var m3BlockScripts = []script{
	{"bl-fast", []step{
		cmd("RPUSH", "bl1", "a", "b", "c"),
		cmd("BLPOP", "bl1", "0.05"), // [bl1 a]
		cmd("BRPOP", "bl1", "0.05"), // [bl1 c]
		cmd("RPUSH", "bl2", "x"),
		cmd("BLPOP", "bl1", "bl2", "0.05"), // first key wins: [bl1 b]
		cmd("BLPOP", "bl1", "bl2", "0.05"), // bl1 drained: [bl2 x]
		cmd("BRPOP", "bl1", "bl2", "0.05"), // both empty: null
		cmd("LPUSH", "bl3", "v1", "v2"),
		cmd("BRPOP", "bl3", "0.05"),        // [bl3 v1]
		cmd("BLPOP", "bl3", "bl3", "0.05"), // duplicate key args: [bl3 v2]
	}},
	{"bl-wrongtype-order", []step{
		// Probed 7.2.7: keys scan in arg order; a wrong-type key errors
		// only when reached before any poppable key.
		cmd("SET", "ws", "s"),
		cmd("RPUSH", "wl", "a"),
		cmd("BLPOP", "ws", "wl", "0.05"),   // WRONGTYPE
		cmd("BLPOP", "wl", "ws", "0.05"),   // pops wl, no error
		cmd("BLPOP", "gone", "ws", "0.05"), // WRONGTYPE (missing first, wrong-type second)
		cmd("RPUSH", "wl", "b"),
		cmd("BLMPOP", "0.05", "2", "wl", "ws", "LEFT"),   // pops wl: [wl [b]]
		cmd("BLMPOP", "0.05", "2", "gone", "ws", "LEFT"), // WRONGTYPE
		cmd("BZPOPMIN", "ws", "0.05"),                    // WRONGTYPE
		cmd("ZADD", "wz", "1", "m"),
		cmd("BZPOPMIN", "wz", "ws", "0.05"),   // pops wz, no error
		cmd("BZPOPMIN", "gone", "ws", "0.05"), // WRONGTYPE
		// BLMOVE: wrong-type source errors; a missing source blocks even
		// with a wrong-type destination (probed).
		cmd("BLMOVE", "ws", "wl", "LEFT", "LEFT", "0.05"), // WRONGTYPE source
		cmd("RPUSH", "wl", "c"),
		cmd("BLMOVE", "wl", "ws", "LEFT", "LEFT", "0.05"),   // WRONGTYPE dst
		cmd("BLMOVE", "gone", "ws", "LEFT", "LEFT", "0.05"), // null after timeout
	}},
	{"bl-timeout-errors", []step{
		// Probed 7.2.7 getTimeoutFromObjectOrReply strings.
		cmd("BLPOP", "te", "abc"),  // not a float or out of range
		cmd("BLPOP", "te", "nan"),  // same
		cmd("BLPOP", "te", "-1"),   // timeout is negative
		cmd("BLPOP", "te", "-0.5"), // negative
		cmd("BLPOP", "te", "inf"),  // timeout is out of range
		cmd("BRPOP", "te", "-1"),
		cmd("BZPOPMIN", "te", "abc"),
		cmd("BZPOPMAX", "te", "-1"),
		cmd("BZPOPMIN", "te", "inf"),
		cmd("BLMOVE", "a", "b", "LEFT", "LEFT", "-1"),
		cmd("BRPOPLPUSH", "a", "b", "abc"),
		cmd("BLMPOP", "abc", "1", "te", "LEFT"),
		cmd("BZMPOP", "-1", "1", "te", "MIN"),
		cmd("BLPOP", "te"),              // arity
		cmd("BZPOPMIN", "te"),           // arity
		cmd("BLMOVE", "a", "b", "LEFT"), // arity
	}},
	{"bl-move", []step{
		cmd("RPUSH", "mv1", "a", "b", "c", "d"),
		cmd("BLMOVE", "mv1", "mv2", "LEFT", "LEFT", "0.05"),
		cmd("BLMOVE", "mv1", "mv2", "RIGHT", "RIGHT", "0.05"),
		cmd("LRANGE", "mv2", "0", "-1"),
		cmd("BLMOVE", "mv1", "mv1", "LEFT", "RIGHT", "0.05"), // same-key rotate
		cmd("LRANGE", "mv1", "0", "-1"),
		cmd("BRPOPLPUSH", "mv1", "mv3", "0.05"),
		cmd("LRANGE", "mv3", "0", "-1"),
		cmd("BLMOVE", "mv1", "mv2", "UP", "LEFT", "0.05"), // syntax error
		cmd("BLMOVE", "mv1", "mv2", "LEFT", "UP", "0.05"), // syntax error
		cmd("BRPOPLPUSH", "gone", "mv2", "0.05"),          // null
	}},
	{"bl-mpop", []step{
		cmd("RPUSH", "mp1", "a", "b", "c", "d"),
		cmd("BLMPOP", "0.05", "1", "mp1", "LEFT"),                // [mp1 [a]] default COUNT 1
		cmd("BLMPOP", "0.05", "1", "mp1", "RIGHT", "COUNT", "2"), // [mp1 [d c]]
		cmd("RPUSH", "mp2", "x"),
		cmd("BLMPOP", "0.05", "2", "mp1", "mp2", "LEFT"),  // first key wins
		cmd("BLMPOP", "0.05", "2", "mp1", "mp2", "RIGHT"), // mp1 drained: mp2
		cmd("LRANGE", "mp1", "0", "-1"),
		// Parse errors (probed).
		cmd("BLMPOP", "0", "0", "mp1", "LEFT"),          // numkeys
		cmd("BLMPOP", "0", "-1", "mp1", "LEFT"),         // numkeys
		cmd("BLMPOP", "0", "abc", "mp1", "LEFT"),        // numkeys
		cmd("BLMPOP", "0", "2", "mp1", "LEFT"),          // numkeys eats direction: syntax
		cmd("BLMPOP", "0", "1", "mp1", "UP"),            // direction: syntax
		cmd("BLMPOP", "0", "1", "mp1", "LEFT", "JUNK"),  // syntax
		cmd("BLMPOP", "0", "1", "mp1", "LEFT", "COUNT"), // syntax
		cmd("BLMPOP", "0", "1", "mp1", "LEFT", "COUNT", "0"),
		cmd("BLMPOP", "0", "1", "mp1", "LEFT", "COUNT", "-1"),
		cmd("BLMPOP", "0", "1", "mp1", "LEFT", "COUNT", "abc"),
		cmd("BLMPOP", "0", "1"), // arity
	}},
	{"bl-timeouts", []step{
		// Few and ≤0.1s each: null replies after a real wait.
		cmd("BLPOP", "tn1", "0.1"),
		cmd("BLMOVE", "tn1", "tn2", "LEFT", "LEFT", "0.1"),
		cmd("BLMPOP", "0.1", "1", "tn1", "RIGHT"),
		cmd("BZPOPMIN", "tn1", "0.1"),
		cmd("BZMPOP", "0.1", "2", "tn1", "tn2", "MAX"),
		cmd("BLPOP", "tn1", "0.0001"), // sub-ms rounds up to a short wait, not forever
	}},
	{"bl-wake", []step{
		// conn1 parks; conn0 pushes; conn1's late reply compares.
		sendOn(1, "BLPOP", "wk1", "2"),
		cmd("SLEEP", "50"),
		cmd("LPUSH", "wk1", "v"),
		recvOn(1, mEq), // [wk1 v]
		cmd("LLEN", "wk1"),
		// BLMOVE wake.
		sendOn(1, "BLMOVE", "wk2", "wk3", "LEFT", "RIGHT", "2"),
		cmd("SLEEP", "50"),
		cmd("RPUSH", "wk2", "mv"),
		recvOn(1, mEq), // mv
		cmd("LRANGE", "wk3", "0", "-1"),
		// BZPOPMIN wake via ZADD; BZMPOP wake via ZINCRBY.
		sendOn(1, "BZPOPMIN", "wk4", "2"),
		cmd("SLEEP", "50"),
		cmd("ZADD", "wk4", "2.5", "zm"),
		recvOn(1, mEq), // [wk4 zm 2.5]
		sendOn(1, "BZMPOP", "2", "1", "wk5", "MAX"),
		cmd("SLEEP", "50"),
		cmd("ZINCRBY", "wk5", "4", "zn"),
		recvOn(1, mEq), // [wk5 [[zn 4]]]
		// "-0" blocks forever (probed): only a push unblocks it.
		sendOn(1, "BLPOP", "wk6", "-0"),
		cmd("SLEEP", "50"),
		cmd("LPUSH", "wk6", "zv"),
		recvOn(1, mEq), // [wk6 zv]
		// Two parked waiters, one push each: FIFO service. The SLEEP
		// orders the two registrations on both servers.
		sendOn(1, "BLPOP", "wk7", "2"),
		cmd("SLEEP", "50"),
		sendOn(2, "BLPOP", "wk7", "2"),
		cmd("SLEEP", "50"),
		cmd("RPUSH", "wk7", "e1"),
		recvOn(1, mEq), // first waiter served first
		cmd("RPUSH", "wk7", "e2"),
		recvOn(2, mEq),
	}},
	{"bl-in-multi", []step{
		// Blocking commands inside MULTI take the non-blocking fast path.
		cmd("MULTI"),
		cmd("BLPOP", "mq1", "5"),
		cmd("BZPOPMIN", "mq2", "5"),
		cmd("BLMOVE", "mq1", "mq3", "LEFT", "LEFT", "5"),
		cmd("EXEC"), // [null null null]
		cmd("RPUSH", "mq1", "e1"),
		cmd("MULTI"),
		cmd("BLPOP", "mq1", "5"),
		cmd("EXEC"), // pops immediately
		cmd("EXISTS", "mq1"),
	}},
	{"bl-zset", []step{
		cmd("ZADD", "zp1", "1.5", "m1", "2", "m2", "3", "m3"),
		cmd("BZPOPMIN", "zp1", "0.05"), // [zp1 m1 1.5]
		cmd("BZPOPMAX", "zp1", "0.05"), // [zp1 m3 3]
		cmd("ZADD", "zp2", "9", "n1"),
		cmd("BZPOPMIN", "zp1", "zp2", "0.05"), // first key wins: [zp1 m2 2]
		cmd("BZPOPMIN", "zp1", "zp2", "0.05"), // zp1 drained: [zp2 n1 9]
		cmd("EXISTS", "zp1"),
		cmd("ZADD", "zm1", "1", "a", "2", "b", "3", "c"),
		cmd("BZMPOP", "0.05", "1", "zm1", "MIN"),               // [[a 1]]
		cmd("BZMPOP", "0.05", "1", "zm1", "MAX", "COUNT", "2"), // [[c 3],[b 2]]
		cmd("EXISTS", "zm1"),
		// Parse errors (probed).
		cmd("BZMPOP", "0", "0", "zm1", "MIN"),
		cmd("BZMPOP", "0", "abc", "zm1", "MIN"),
		cmd("BZMPOP", "0", "1", "zm1", "SIDEWAYS"),
		cmd("BZMPOP", "0", "1", "zm1", "MIN", "COUNT", "0"),
		cmd("BZMPOP", "0", "1", "zm1", "MIN", "JUNK"),
		cmd("BZMPOP", "0", "1"), // arity
	}},
	{"bl-resp3", []step{
		cmdOnM(mHello, 1, "HELLO", "3"),
		cmd("ZADD", "r3", "1.5", "m1", "2", "m2"),
		cmdOn(1, "BZPOPMIN", "r3", "0.05"),           // RESP3: score as double
		cmdOn(1, "BZMPOP", "0.05", "1", "r3", "MAX"), // [[m2 ,2]]
		cmd("RPUSH", "r3l", "a", "b"),
		cmdOn(1, "BLPOP", "r3l", "0.05"),                // [r3l a]
		cmdOn(1, "BLMPOP", "0.05", "1", "r3l", "RIGHT"), // [r3l [b]]
		cmdOn(1, "BLPOP", "gone", "0.05"),               // RESP3 null
		cmdOn(1, "BLMPOP", "0.05", "1", "gone", "LEFT"), // RESP3 null
	}},
}
