package commands

import (
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// --- transactions (M3, design doc §4.2) --------------------------------------
//
// MULTI/EXEC/DISCARD/WATCH/UNWATCH. The queue gate lives in Engine.Execute;
// these handlers implement the control commands. EXEC runs the queued
// commands under the shard engine's PauseAll token, so a transaction is
// fully atomic across shards (§4.2 strict cross-shard path), and checks
// WATCHed keys for modification (shard WatchVersion/WatchDirty) before
// committing.

var (
	errMultiNested    = resp.Err("ERR MULTI calls can not be nested")
	errExecNoMulti    = resp.Err("ERR EXEC without MULTI")
	errDiscardNoMulti = resp.Err("ERR DISCARD without MULTI")
	errWatchInMulti   = resp.Err("ERR WATCH inside MULTI is not allowed")
	errExecAbort      = resp.Err("EXECABORT Transaction discarded because of previous errors.")
	replyQueued       = resp.Simple("QUEUED") // (emitted by the Execute queue gate)
)

func cmdMulti(_ *Engine, cs *ConnState, _ [][]byte) resp.Value {
	if cs.Multi {
		return errMultiNested
	}
	cs.Multi = true
	return replyOK
}

func cmdDiscard(_ *Engine, cs *ConnState, _ [][]byte) resp.Value {
	if !cs.Multi {
		return errDiscardNoMulti
	}
	cs.clearTx()
	return replyOK
}

func cmdWatch(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if cs.Multi {
		return errWatchInMulti
	}
	// Successive WATCH calls accumulate keys; each is sampled on its own
	// shard (versions and epochs are shard-goroutine state).
	for _, key := range args[1:] {
		var epoch, ver uint64
		e.do(cs, key, func(s *shard.Shard) {
			epoch, ver = s.WatchVersion(cs.DB, string(key))
		})
		cs.Watch = append(cs.Watch, WatchRef{DB: cs.DB, Key: string(key), Epoch: epoch, Ver: ver})
	}
	return replyOK
}

func cmdUnwatch(_ *Engine, cs *ConnState, _ [][]byte) resp.Value {
	cs.Watch = nil
	return replyOK
}

func cmdExec(e *Engine, cs *ConnState, _ [][]byte) resp.Value {
	if !cs.Multi {
		return errExecNoMulti
	}
	// EXEC always leaves multi mode, whatever the outcome; the defers
	// also make the pause panic-safe (Engine.Execute must never take the
	// connection down with it).
	defer cs.clearTx()
	if cs.QueueErr {
		return errExecAbort
	}
	tok, resume := e.Shards.PauseAll()
	defer resume()
	defer func() { cs.tok, cs.inExec = 0, false }()

	// Dirty check: a watched key touched since WATCH aborts EXEC with a
	// null-array reply (the engine is paused, so the check and the commit
	// below are one atomic step).
	for _, w := range cs.Watch {
		dirty := false
		e.Shards.DoTok(tok, e.Shards.ShardIndex([]byte(w.Key)), func(s *shard.Shard) {
			dirty = s.WatchDirty(w.DB, w.Key, w.Epoch, w.Ver)
		})
		if dirty {
			return resp.Null()
		}
	}

	// Commit: run the queue with the pause token. Arity was validated at
	// queue time; handler errors become error elements in the reply array
	// and execution continues, as in Redis.
	cs.tok = tok
	cs.inExec = true
	replies := make([]resp.Value, 0, len(cs.Queue))
	for _, qargs := range cs.Queue {
		if qdef, qok := table[lowerASCII(qargs[0])]; qok {
			replies = append(replies, qdef.Handler(e, cs, qargs))
		}
	}
	return resp.Arr(replies...)
}
