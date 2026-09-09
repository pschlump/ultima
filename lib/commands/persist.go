package commands

// Persistence commands (M5c, design doc §13.1): SAVE, BGSAVE, LASTSAVE,
// BGREWRITEAOF. The work lives in the Persister (lib/persist.Manager);
// these handlers map its outcomes onto Redis 7.2.7's reply strings
// (probed against a live 7.2.7 server).

import (
	"errors"

	"github.com/pschlump/ultima/lib/resp"
)

// errNoPersist replies when no persister is installed (bare test engines
// — Redis always has persistence compiled in, so there is no exact
// analogue; this is the documented divergence).
var errNoPersist = resp.Err("ERR persistence is not enabled on this server")

func cmdSave(e *Engine, _ *ConnState, _ [][]byte) resp.Value {
	p := e.persister
	if p == nil {
		return errNoPersist
	}
	if err := p.Save(); err != nil {
		return resp.Err("ERR " + err.Error())
	}
	return replyOK
}

func cmdBGSave(e *Engine, _ *ConnState, _ [][]byte) resp.Value {
	p := e.persister
	if p == nil {
		return errNoPersist
	}
	if err := p.BGSave(); err != nil {
		if errors.Is(err, ErrPersistChildActive) {
			return resp.Err("ERR Another child process is active (AOF?): can't BGSAVE right now. Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible.")
		}
		if errors.Is(err, ErrPersistBusy) {
			return resp.Err("ERR Background save already in progress")
		}
		return resp.Err("ERR " + err.Error())
	}
	return resp.Simple("Background saving started")
}

func cmdLastSave(e *Engine, _ *ConnState, _ [][]byte) resp.Value {
	p := e.persister
	if p == nil {
		return resp.Int(0)
	}
	return resp.Int(p.LastSave())
}

func cmdBGRewriteAOF(e *Engine, _ *ConnState, _ [][]byte) resp.Value {
	p := e.persister
	if p == nil {
		return errNoPersist
	}
	if err := p.BGRewriteAOF(); err != nil {
		// Redis replies "scheduled" (a simple string, not an error) when
		// a rewrite is already running or must wait for a child.
		if errors.Is(err, ErrPersistBusy) {
			return resp.Simple("Background append only file rewriting scheduled")
		}
		return resp.Err("ERR " + err.Error())
	}
	return resp.Simple("Background append only file rewriting started")
}
