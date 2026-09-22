package commands

// SCRIPT container command + the redis.call bridge (M8, §5.3–§5.4 of the
// integration guide). Subcommand semantics and error strings probed
// against Redis 7.2.7 (docs/Redis-Errors.md §2, §5).

import (
	"fmt"
	"slices"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/scripting"
	"github.com/pschlump/ultima/lib/shard"
)

var scriptHelpLines = []string{
	"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"DEBUG (YES|SYNC|NO)",
	"    Set the debug mode for subsequent scripts executed.",
	"EXISTS <sha1> [<sha1> ...]",
	"    Return information about the existence of the scripts in the script cache.",
	"FLUSH [ASYNC|SYNC]",
	"    Flush the Lua scripts cache. Very dangerous on replicas.",
	"    When called without the optional mode argument, the behavior is determined by the",
	"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
	"    * ASYNC: Asynchronously flush the scripts cache.",
	"    * SYNC: Synchronously flush the scripts cache.",
	"KILL",
	"    Kill the currently executing Lua script.",
	"LOAD <script>",
	"    Load a script into the scripts cache without executing it.",
	"HELP",
	"    Print this help.",
}

func cmdScript(e *Engine, _ *ConnState, args [][]byte) resp.Value {
	sub := lowerASCII(args[1])
	switch sub {
	case "load":
		if len(args) != 3 {
			return errArity("script|load")
		}
		if e.Scripts == nil {
			return resp.Err("ERR scripting support is not enabled in this build")
		}
		sha, _, err := e.Scripts.Compile(args[2])
		if err != nil {
			return scripting.CompileErrorReply(err)
		}
		return resp.BlobStr(sha)
	case "exists":
		if len(args) < 3 {
			return errArity("script|exists")
		}
		shas := make([]string, 0, len(args)-2)
		for _, a := range args[2:] {
			shas = append(shas, string(a))
		}
		exists := e.Scripts.Exists(shas...)
		out := make([]resp.Value, 0, len(exists))
		for _, ex := range exists {
			if ex {
				out = append(out, resp.Int(1))
			} else {
				out = append(out, resp.Int(0))
			}
		}
		return resp.Arr(out...)
	case "flush":
		// Probed: extra args take the SYNC|ASYNC error, not the arity form.
		if len(args) > 3 {
			return resp.Err("ERR SCRIPT FLUSH only support SYNC|ASYNC option")
		}
		if len(args) == 3 {
			switch lowerASCII(args[2]) {
			case "sync", "async":
				// ASYNC drops the cache map under a lock swap — cheap
				// enough that both behave the same; the flag is kept for
				// client compatibility.
			default:
				return resp.Err("ERR SCRIPT FLUSH only support SYNC|ASYNC option")
			}
		}
		e.Scripts.Flush()
		return replyOK
	case "kill":
		if len(args) != 2 {
			return errArity("script|kill")
		}
		switch e.Scripts.Kill() {
		case scripting.KillNotBusy:
			return resp.Err("NOTBUSY No scripts in execution right now.")
		case scripting.KillUnkillable:
			return resp.Err("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.")
		default:
			return replyOK
		}
	case "debug":
		// 7.2.7 ships the Lua debugger (SCRIPT DEBUG YES/SYNC/NO answers
		// OK); the wasm backend exposes no debug hooks — documented
		// refusal (ledgered divergence).
		return resp.Err("ERR SCRIPT DEBUG is not supported by this server.")
	case "help":
		out := make([]resp.Value, 0, len(scriptHelpLines))
		for _, l := range scriptHelpLines {
			out = append(out, resp.Simple(l))
		}
		return resp.Arr(out...)
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s'. Try SCRIPT HELP.", string(args[1])))
	}
}

// --- the redis.call bridge (M8c) ----------------------------------------------

// runScriptCommand is the CallPath the scripting manager invokes for
// redis.call / redis.pcall (decisions S2/S9): one command executed under
// the EVAL pause token with the calling connection's identity. It mirrors
// cmdExec's inner-command path — direct handler call, per-command AOF
// capture (effects-only replication: EVAL itself is never logged), the
// monitor feed in Redis's [db lua] form — plus the script-specific gates
// probed against 7.2.7 (docs/Redis-Errors.md §5). wrote reports a
// successful write (SCRIPT KILL's UNKILLABLE gate).
func (e *Engine) runScriptCommand(cs *ConnState, argv []string, ro bool) (resp.Value, bool) {
	args := make([][]byte, len(argv))
	for i, a := range argv {
		args[i] = []byte(a)
	}
	name := lowerASCII(args[0])
	d, ok := table[name]
	if !ok {
		return resp.Err("ERR Unknown Redis command called from script"), false
	}
	// noscript commands may not run from scripts (probed: eval, script,
	// subscribe, multi, watch, config, client, monitor, auth, save…).
	if slices.Contains(d.Flags, "noscript") {
		return resp.Err("ERR This Redis command is not allowed from script"), false
	}
	// Arity: scripts get a lib-level error, not the command's own text.
	if (d.Arity > 0 && len(args) != d.Arity) || (d.Arity < 0 && len(args) < -d.Arity) {
		return resp.Err("ERR Wrong number of args calling Redis command from script"), false
	}
	// Read-only scripts may not write (probed text).
	if ro && slices.Contains(d.Flags, "write") {
		return resp.Err("ERR Write commands are not allowed from read-only scripts."), false
	}
	// OOM gate, mirroring Execute's (S9): denyoom commands are rejected
	// when eviction can't get back under the limit.
	if e.Shards.OverMemory() && slices.Contains(d.Flags, "denyoom") {
		outOfMem := true
		if e.Shards.Policy() != shard.PolicyNoEviction {
			outOfMem = e.Shards.EvictNow(cs.tok)
		}
		if outOfMem {
			return errOOM, false
		}
	}

	track := e.persister != nil && slices.Contains(d.Flags, "write")
	if track {
		e.persistInFlight.Add(1)
	}
	v := d.Handler(e, cs, args)
	e.capturePersist(cs, d, name, args, v) // effects replication (S2)
	if track {
		e.persistInFlight.Add(-1)
	}
	e.notifyMonitorsLua(cs, args)
	wrote := slices.Contains(d.Flags, "write") && v.Kind != resp.KindError
	return v, wrote
}
