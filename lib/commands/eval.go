package commands

// Lua scripting commands (M8, design doc §7 P3, D12): EVAL / EVALSHA /
// EVAL_RO / EVALSHA_RO. Semantics and error strings are probed byte-exact
// against Redis 7.2.7 (docs/Redis-Errors.md). The execution model mirrors
// cmdExec (decision S3): the script runs under the shard engine's
// PauseAll token, so it is atomic across shards; effects replicate
// per-command through the bridge (S2 — EVAL itself is never logged).
//
// Cost split: the host VM (wazero runtime + Lua state) is checked out of
// the per-script pool BEFORE the pause (M8e R3) — a pool miss pays the
// ~44 ms instantiation, which touches no keyspace and must not happen
// under PauseAll; only the compiled script's run happens inside the pause.

import (
	"context"

	"github.com/yuin/gopher-lua/host"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/scripting"
)

var errNoscript = resp.Err("NOSCRIPT No matching script. Please use EVAL.")

func cmdEval(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return evalImpl(e, cs, args, evalInputScript, false)
}

func cmdEvalSha(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return evalImpl(e, cs, args, evalInputSHA, false)
}

func cmdEvalRO(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return evalImpl(e, cs, args, evalInputScript, true)
}

func cmdEvalShaRO(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	return evalImpl(e, cs, args, evalInputSHA, true)
}

type evalInputKind int

const (
	evalInputScript evalInputKind = iota // args[1] is the script body
	evalInputSHA                         // args[1] is a cached SHA-1
)

func evalImpl(e *Engine, cs *ConnState, args [][]byte, input evalInputKind, ro bool) resp.Value {
	if e.Scripts == nil {
		return resp.Err("ERR scripting support is not enabled in this build")
	}
	// Argument validation order probed against 7.2.7 (docs/Redis-Errors.md
	// §1): numkeys parse, then negative, then too-large.
	numkeys, ok := parseIntStrict(args[2])
	if !ok {
		return errNotInt
	}
	if numkeys < 0 {
		return resp.Err("ERR Number of keys can't be negative")
	}
	if numkeys > int64(len(args)-3) {
		return resp.Err("ERR Number of keys can't be greater than number of args")
	}

	var sha string
	var script *host.Script
	if input == evalInputSHA {
		sha = string(args[1])
		s, found := e.Scripts.Lookup(sha)
		if !found {
			return errNoscript
		}
		script = s
	} else {
		csha, s, err := e.Scripts.Compile(args[1])
		if err != nil {
			return scripting.CompileErrorReply(err)
		}
		sha, script = csha, s
	}

	keys := make([]string, 0, numkeys)
	for _, k := range args[3 : 3+numkeys] {
		keys = append(keys, string(k))
	}
	argv := make([]string, 0, len(args)-3-int(numkeys))
	for _, a := range args[3+numkeys:] {
		argv = append(argv, string(a))
	}

	// VM checkout before the pause: a pool hit is µs; a miss is the slow
	// wazero instantiation, which must not happen under PauseAll.
	vm, err := e.Scripts.CheckoutVM(script)
	if err != nil {
		e.logger().Error("scripting: VM creation failed", "err", err)
		return resp.Err("ERR Internal error running script")
	}
	// discard is set when the run ends in an error class that leaves the
	// VM unfit for reuse (kill/deadline/OOM/trap — scripting.IsVMFatal);
	// ReleaseVM then closes it instead of returning it to the pool.
	discard := false
	defer func() { e.Scripts.ReleaseVM(vm, script.SHA1, discard) }()

	// Atomicity (S3): take PauseAll exactly like cmdExec — unless the
	// connection already holds a pause (EVAL queued inside MULTI, now
	// running under EXEC's token; txMu is not reentrant).
	paused := cs.tok == 0
	var resume func()
	if paused {
		var tok uint64
		tok, resume = e.Shards.PauseAll()
		cs.tok = tok
	}
	cs.inExec = true // blocking commands take their non-blocking fast path
	defer func() {
		cs.inExec = false
		if paused {
			cs.tok = 0
			resume()
		}
	}()

	call := func(cargv []string) (resp.Value, bool) {
		return e.runScriptCommand(cs, cargv, ro)
	}
	res, respVer, rerr := e.Scripts.RunOnVM(context.Background(), vm, script, keys, argv, ro, call)
	if rerr != nil {
		discard = scripting.IsVMFatal(rerr)
		return e.Scripts.RunErrorReply(sha, rerr)
	}
	return scripting.ToReply(res, respVer)
}
