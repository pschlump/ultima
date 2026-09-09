// notify.go — keyspace notifications (design doc §14.4 M5; Redis
// notify-keyspace-events semantics, compatibility version 7.2.7).
//
// Notifications ride the classic pub/sub broker: with the K class the
// event name is published as the payload of __keyspace@<db>__:<key>, with
// the E class the key is published to __keyevent@<db>__:<event>. Event
// names and classes mirror Redis's notify.c (validated byte-exact by the
// differential harness).

package commands

import "fmt"

// Notification classes (Redis notify-keyspace-events flag letters).
const (
	nfKeyspace uint64 = 1 << iota // K — __keyspace@<db>__:<key> channels
	nfKeyevent                    // E — __keyevent@<db>__:<event> channels
	nfGeneric                     // g — del, expire, persist, rename_*, …
	nfString                      // $
	nfList                        // l
	nfSet                         // s
	nfHash                        // h
	nfZset                        // z
	nfExpired                     // x
	nfEvicted                     // e
	nfStream                      // t — accepted, never emitted (streams are P3/M8)
	nfKeyMiss                     // m — accepted, never emitted yet
	nfModule                      // d — accepted, never emitted (no modules)
	nfNew                         // n — accepted, never emitted yet
)

// nfAll mirrors Redis's NOTIFY_ALL (the 'A' alias): every class except
// keymiss (m) and new-key (n), which Redis excludes on purpose.
const nfAll = nfGeneric | nfString | nfList | nfSet | nfHash | nfZset |
	nfExpired | nfEvicted | nfStream | nfModule

// notifyClassChars is the valid-character list verbatim from Redis 7.2.7's
// configSetHandler error message — the differential gate diffs it.
const notifyClassChars = "Ag$lshzxeKEtmdn"

// notifyEventClass maps each event name to its class, mirroring the
// NOTIFY_* arguments at Redis's notifyKeyspaceEvent call sites.
var notifyEventClass = map[string]uint64{
	// generic
	"del": nfGeneric, "expire": nfGeneric, "persist": nfGeneric,
	"rename_from": nfGeneric, "rename_to": nfGeneric,
	"copy_to": nfGeneric, "restore": nfGeneric,
	// string
	"set": nfString, "setrange": nfString, "append": nfString,
	"incrby": nfString, "incrbyfloat": nfString,
	// list
	"lpush": nfList, "rpush": nfList, "lpop": nfList, "rpop": nfList,
	"linsert": nfList, "lset": nfList, "lrem": nfList, "ltrim": nfList,
	// hash
	"hset": nfHash, "hdel": nfHash, "hincrby": nfHash, "hincrbyfloat": nfHash,
	// set
	"sadd": nfSet, "srem": nfSet, "spop": nfSet,
	"sinterstore": nfSet, "sunionstore": nfSet, "sdiffstore": nfSet,
	// sorted set
	"zadd": nfZset, "zincr": nfZset, "zrem": nfZset,
	"zpopmin": nfZset, "zpopmax": nfZset,
	"zremrangebyrank": nfZset, "zremrangebyscore": nfZset, "zremrangebylex": nfZset,
	"zinterstore": nfZset, "zunionstore": nfZset, "zdiffstore": nfZset,
	// expiry / eviction
	"expired": nfExpired, "evicted": nfEvicted,
}

// parseNotifyFlags parses a notify-keyspace-events value into a class
// bitmask, mirroring notify.c's flags parsing. Empty is valid (off).
func parseNotifyFlags(s string) (uint64, bool) {
	var flags uint64
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 'A':
			flags |= nfAll
		case 'g':
			flags |= nfGeneric
		case '$':
			flags |= nfString
		case 'l':
			flags |= nfList
		case 's':
			flags |= nfSet
		case 'h':
			flags |= nfHash
		case 'z':
			flags |= nfZset
		case 'x':
			flags |= nfExpired
		case 'e':
			flags |= nfEvicted
		case 'K':
			flags |= nfKeyspace
		case 'E':
			flags |= nfKeyevent
		case 't':
			flags |= nfStream
		case 'm':
			flags |= nfKeyMiss
		case 'd':
			flags |= nfModule
		case 'n':
			flags |= nfNew
		default:
			return 0, false
		}
	}
	return flags, true
}

// notifyFlagsToString renders the canonical CONFIG GET form of a flags
// bitmask, mirroring notify.c's keyspaceEventsFlagsToString: "A" when the
// whole NOTIFY_ALL class set is present, else the classes in g$lshzxetdn
// order, then K, E, and m last. Redis regenerates the string from the
// bitmask rather than keeping the CONFIG SET input verbatim.
func notifyFlagsToString(f uint64) string {
	var b []byte
	if f&nfAll == nfAll {
		b = append(b, 'A')
	} else {
		for _, c := range []struct {
			bit uint64
			ch  byte
		}{
			{nfGeneric, 'g'}, {nfString, '$'}, {nfList, 'l'}, {nfSet, 's'},
			{nfHash, 'h'}, {nfZset, 'z'}, {nfExpired, 'x'}, {nfEvicted, 'e'},
			{nfStream, 't'}, {nfModule, 'd'}, {nfNew, 'n'},
		} {
			if f&c.bit != 0 {
				b = append(b, c.ch)
			}
		}
	}
	if f&nfKeyspace != 0 {
		b = append(b, 'K')
	}
	if f&nfKeyevent != 0 {
		b = append(b, 'E')
	}
	if f&nfKeyMiss != 0 {
		b = append(b, 'm')
	}
	return string(b)
}

// SetNotifyKeyspaceEvents validates and installs a notify-keyspace-events
// value (config file, CONFIG SET). Returns false on an invalid class
// character.
func (e *Engine) SetNotifyKeyspaceEvents(v string) bool {
	f, ok := parseNotifyFlags(v)
	if !ok {
		return false
	}
	e.notifyFlags.Store(f)
	return true
}

// NotifyKeyspaceEvents returns the canonical flags string (CONFIG GET).
func (e *Engine) NotifyKeyspaceEvents() string {
	return notifyFlagsToString(e.notifyFlags.Load())
}

// notifyKeyspace emits a keyspace notification for event on key in db.
// It is the single emission point for every mutation path — command
// handlers call it after mutating, and the shard engine's OnKeyGone hook
// routes "expired" (M5b: "evicted") here. Safe to call from shard
// goroutines: Broker.Publish is mutex-guarded and delivery is
// non-blocking. No-ops fast when the relevant classes are off.
func (e *Engine) notifyKeyspace(db int, key, event string) {
	f := e.notifyFlags.Load()
	cls, ok := notifyEventClass[event]
	if !ok || f&(nfKeyspace|nfKeyevent) == 0 || f&cls == 0 {
		return
	}
	if f&nfKeyspace != 0 {
		e.PubSub.Publish(fmt.Sprintf("__keyspace@%d__:%s", db, key), []byte(event), 0, nil)
	}
	if f&nfKeyevent != 0 {
		e.PubSub.Publish(fmt.Sprintf("__keyevent@%d__:%s", db, event), []byte(key), 0, nil)
	}
}
