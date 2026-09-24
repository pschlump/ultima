package commands

// AOF capture (M5c, design doc §13.1): the command engine reports every
// successful write command to a Persister sink (lib/persist.Manager,
// installed at startup after restore). Commands that are nondeterministic
// or connection-scoped are rewritten to a deterministic, replayable form
// first — mirroring Redis 7.2.7's AOF rewrites (verified by probing a
// live 7.2.7 server):
//
//   - relative expire forms become absolute: EXPIRE/PEXPIRE/EXPIREAT →
//     PEXPIREAT key <abs-ms> [cond]; SET … EX/PX/EXAT → SET … PXAT;
//     GETEX … EX/PX/EXAT → PEXPIREAT, GETEX … PERSIST → PERSIST.
//   - blocking pops become their plain effect: BLPOP/BRPOP → LPOP/RPOP of
//     the key the reply names (timeout, dropped); BLMOVE → LMOVE without
//     the timeout; BRPOPLPUSH → RPOPLPUSH; BZPOPMIN/MAX → ZPOPMIN/MAX;
//     BLMPOP/BZMPOP → LMPOP/ZMPOP of the single popped key. A null reply
//     (timeout) logs nothing.
//   - SPOP → SREM of the members the reply returned (set pops are random).
//   - Shard-originated deletions (expiry sweep, passive expiry, eviction)
//     arrive via LogKeyGone as synthesized DELs.
//   - EXEC is not logged as a unit: Redis 7.2.7's AOF records the inner
//     commands without MULTI/EXEC framing (replay is single-threaded, so
//     atomicity is moot). cmdExec captures each queued command as it runs.
//
// Capture happens on the connection goroutine after the handler returns;
// the sink must be cheap and mutex-guarded (lib/persist's per-shard logs).

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pschlump/ultima/lib/resp"
)

// ErrPersistBusy is the Persister's "a background save/rewrite is already
// running" signal; the SAVE/BGSAVE handlers map it to Redis's strings.
var ErrPersistBusy = errors.New("persist: background operation already in progress")

// ErrPersistChildActive is BGSAVE's "an AOF rewrite is running" signal
// (Redis's single-child rule), mapped to the byte-exact 7.2.7 error.
var ErrPersistChildActive = errors.New("persist: another child process is active")

// Persister is the command engine's view of lib/persist.Manager. The
// interface lives here (not in persist) to keep the import direction
// one-way: persist → commands.
type Persister interface {
	// LogCommand appends one (already rewritten) command: shardHint is
	// the owning shard of the record's key, -1 broadcasts to every shard
	// log (FLUSHDB/FLUSHALL). db selects the logical DB (SELECT records).
	LogCommand(shardHint, db int, argv [][]byte)
	// LogKeyGone synthesizes DEL for a shard-originated removal (expiry,
	// eviction).
	LogKeyGone(db int, key string)

	Save() error
	BGSave() error
	LastSave() int64
	BGRewriteAOF() error

	Loading() bool
	AppendOnly() bool
	SetAppendOnly(on bool) error
	AppendFsync() string
	SetAppendFsync(v string)
	Dir() string
	DbFilename() string
	SetSaveRules(v string)
	InfoPersistence(sb *strings.Builder)
}

// SetPersister installs the persistence sink. Called once at startup,
// after restore (replayed commands must not be re-logged).
func (e *Engine) SetPersister(p Persister) { e.persister = p }

// Persist returns the installed sink, nil when persistence is off.
func (e *Engine) Persist() Persister { return e.persister }

// capturePersist reports one executed command to the sink. It runs after
// the handler returns, on the connection goroutine; name is the lowercased
// command name. EXEC is excluded here — cmdExec captures its queued
// commands individually (they bypass Execute, tx.go).
//
// M9b: with appendonly off the AOF record is never written, so the
// rewrite runs in check-only mode (build=false — no argv cloning, same
// mutation decision) and LogCommand receives a nil argv, which the
// Manager reduces to its save-rule dirty bump (lib/persist).
func (e *Engine) capturePersist(cs *ConnState, def *CmdDef, name string, args [][]byte, v resp.Value) {
	p := e.persister
	if p == nil || name == "exec" {
		return
	}
	if !slices.Contains(def.Flags, "write") || v.Kind == resp.KindError {
		return
	}
	build := p.AppendOnly()
	argv, keyHint, ok := rewriteForPersist(name, args, v, build)
	if !ok {
		return
	}
	if !build {
		p.LogCommand(0, cs.DB, nil) // dirty-count only; no AOF record
		return
	}
	hint := -1
	if keyHint != nil {
		hint = e.Shards.ShardIndex(keyHint)
	}
	p.LogCommand(hint, cs.DB, argv)
}

// rewriteForPersist maps (command, reply) to the deterministic argv to
// log. ok == false logs nothing (no mutation: timeouts, conditional
// failures, read-only GETEX). keyHint is the key to route the record by
// (nil = broadcast to all shard logs). With build == false (appendonly
// off, M9b) only the ok decision is computed — argv and keyHint are nil
// and no cloning happens.
func rewriteForPersist(name string, args [][]byte, v resp.Value, build bool) (argv [][]byte, keyHint []byte, ok bool) {
	switch name {
	case "blpop", "brpop":
		// Reply [key, value]; a timeout (null) logs nothing.
		key, got := popReplyKey(v)
		if !got {
			return nil, nil, false
		}
		if !build {
			return nil, nil, true
		}
		op := "LPOP"
		if name == "brpop" {
			op = "RPOP"
		}
		return [][]byte{[]byte(op), key}, key, true

	case "bzpopmin", "bzpopmax":
		key, got := popReplyKey(v)
		if !got {
			return nil, nil, false
		}
		if !build {
			return nil, nil, true
		}
		op := "ZPOPMIN"
		if name == "bzpopmax" {
			op = "ZPOPMAX"
		}
		return [][]byte{[]byte(op), key}, key, true

	case "blmpop":
		key, got := popReplyKey(v)
		if !got {
			return nil, nil, false
		}
		if !build {
			return nil, nil, true
		}
		// BLMPOP timeout numkeys key… LEFT|RIGHT [COUNT n]
		return mpopRewrite("LMPOP", args, key), key, true

	case "bzmpop":
		key, got := popReplyKey(v)
		if !got {
			return nil, nil, false
		}
		if !build {
			return nil, nil, true
		}
		// BZMPOP timeout numkeys key… MIN|MAX [COUNT n]
		return mpopRewrite("ZMPOP", args, key), key, true

	case "blmove":
		// BLMOVE src dst from to timeout → LMOVE src dst from to
		if !build {
			return nil, nil, true
		}
		argv := [][]byte{[]byte("LMOVE"), args[1], args[2], args[3], args[4]}
		return argv, args[1], true

	case "brpoplpush":
		// BRPOPLPUSH src dst timeout → RPOPLPUSH src dst
		if !build {
			return nil, nil, true
		}
		return [][]byte{[]byte("RPOPLPUSH"), args[1], args[2]}, args[1], true

	case "spop":
		// SPOP is random: log the popped members as SREM.
		switch v.Kind {
		case resp.KindBlobString:
			if !build {
				return nil, nil, true
			}
			return [][]byte{[]byte("SREM"), args[1], v.Blob}, args[1], true
		case resp.KindSet, resp.KindArray:
			if len(v.Arr) == 0 {
				return nil, nil, false
			}
			if !build {
				return nil, nil, true
			}
			members := make([][]byte, 0, len(v.Arr))
			for _, m := range v.Arr {
				members = append(members, m.Blob)
			}
			argv := append([][]byte{[]byte("SREM"), args[1]}, members...)
			return argv, args[1], true
		default:
			return nil, nil, false
		}

	case "expire", "pexpire", "expireat", "pexpireat":
		// Only an applied expire mutates (reply 1); reply 0 (missing key,
		// failed NX/XX/GT/LT condition) logs nothing.
		if v.Kind != resp.KindInt || v.Int != 1 {
			return nil, nil, false
		}
		n, okN := parseIntStrict(args[2])
		if !okN {
			return nil, nil, false
		}
		if !build {
			return nil, nil, true
		}
		abs := expireAbsMs(name, n)
		argv := [][]byte{[]byte("PEXPIREAT"), args[1], []byte(strconv.FormatInt(abs, 10))}
		if len(args) > 3 { // keep a failed-at-origin-impossible condition (GT/XX/…)
			argv = append(argv, args[3])
		}
		return argv, args[1], true

	case "getex":
		if v.Kind != resp.KindBlobString || len(args) < 3 {
			return nil, nil, false // miss, or no expire change requested
		}
		opt := strings.ToLower(string(args[2]))
		if opt == "persist" {
			if !build {
				return nil, nil, true
			}
			return [][]byte{[]byte("PERSIST"), args[1]}, args[1], true
		}
		if len(args) < 4 {
			return nil, nil, false
		}
		n, okN := parseIntStrict(args[3])
		if !okN {
			return nil, nil, false
		}
		switch opt {
		case "ex", "px", "exat", "pxat":
		default:
			return nil, nil, false
		}
		if !build {
			return nil, nil, true
		}
		var abs int64
		switch opt {
		case "ex":
			abs = expireAbsMs("expire", n)
		case "px":
			abs = expireAbsMs("pexpire", n)
		case "exat":
			abs = expireAbsMs("expireat", n)
		default: // pxat
			abs = n
		}
		return [][]byte{[]byte("PEXPIREAT"), args[1], []byte(strconv.FormatInt(abs, 10))}, args[1], true

	case "set":
		// Relative expire options become absolute PXAT; every other
		// option (NX/XX/GET/KEEPTTL) is deterministic and stays verbatim.
		if !build {
			return nil, nil, true
		}
		argv := cloneArgs(args)
		for i := 3; i+1 < len(argv); i++ {
			var rel string
			switch strings.ToLower(string(argv[i])) {
			case "ex":
				rel = "expire"
			case "px":
				rel = "pexpire"
			case "exat":
				rel = "expireat"
			default:
				continue
			}
			n, okN := parseIntStrict(argv[i+1])
			if !okN {
				continue
			}
			argv[i] = []byte("PXAT")
			argv[i+1] = []byte(strconv.FormatInt(expireAbsMs(rel, n), 10))
		}
		return argv, args[1], true
	}
	// Verbatim: every other write command is deterministic (DEL, INCR,
	// HSET, LPUSH/RPUSH, ZADD, SINTERSTORE, FLUSHDB/FLUSHALL, …).
	if !build {
		return nil, nil, true
	}
	argv = cloneArgs(args)
	if def := table[name]; def != nil && def.FirstKey > 0 && len(args) > def.FirstKey {
		return argv, args[def.FirstKey], true
	}
	return argv, nil, true
}

// popReplyKey extracts the key from a blocking pop's reply: the first
// element of the [key, payload] array. got is false for a timeout (null)
// or an unexpected shape.
func popReplyKey(v resp.Value) (key []byte, got bool) {
	if v.Kind != resp.KindArray || len(v.Arr) == 0 || v.Arr[0].Kind != resp.KindBlobString {
		return nil, false
	}
	return v.Arr[0].Blob, true
}

// mpopRewrite builds the LMPOP/ZMPOP form of a BLMPOP/BZMPOP: one key
// (the one the reply popped), the direction, and the COUNT tail.
func mpopRewrite(op string, args [][]byte, key []byte) [][]byte {
	argv := [][]byte{[]byte(op), []byte("1"), key}
	nk, err := strconv.Atoi(string(args[2]))
	if err != nil || nk < 0 || 3+nk > len(args) {
		return argv
	}
	argv = append(argv, args[3+nk]) // LEFT|RIGHT / MIN|MAX
	if 3+nk+1 < len(args) {         // COUNT n tail, verbatim
		argv = append(argv, args[3+nk+1:]...)
	}
	return argv
}

// expireAbsMs converts an EXPIRE-family argument to absolute ms: expire
// is relative seconds, pexpire relative ms, expireat absolute seconds,
// pexpireat absolute ms.
func expireAbsMs(name string, n int64) int64 {
	nowMs := time.Now().UnixMilli()
	switch name {
	case "expire":
		return nowMs + n*1000
	case "pexpire":
		return nowMs + n
	case "expireat":
		return n * 1000
	default: // pexpireat
		return n
	}
}
