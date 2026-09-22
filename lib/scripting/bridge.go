package scripting

// The redis.* Lua host functions (M8, §5.4 of the integration guide).
// They run on the goroutine executing the script (the host A9 law), find
// their per-run context in the manager's runs map, and raise Redis's
// {err=...} table error class for command failures — the text then
// propagates verbatim (docs/Redis-Errors.md §5).

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pschlump/gopher-lua/host"
	"github.com/pschlump/ultima/lib/resp"
)

// raiseErr raises a redis.call-class error: a plain STRING whose text
// starts with an error-code word (Redis raises the same text with an
// error-object metatable we cannot reproduce — ErrorReply's code-prefix
// rule renders it verbatim). A Lua pcall over redis.call therefore
// catches a string, exactly as on Redis (type/tostring/nested rendering
// all match — see docs/Redis-Errors.md §5).
func raiseErr(text string) error {
	return &host.ValueError{V: host.String(text)}
}

func kv(k string, v host.Value) host.KV { return host.KV{Key: host.String(k), Val: v} }

func errTable(text string) host.Value { return host.Table(kv("err", host.String(text))) }
func okTable(text string) host.Value  { return host.Table(kv("ok", host.String(text))) }

// pcallErrTable is redis.pcall's error return (probed 7.2.7):
// {err="<text>", ignore_error_stats_update=1}.
func pcallErrTable(text string) host.Value {
	return host.Table(kv("ignore_error_stats_update", host.Int(1)), kv("err", host.String(text)))
}

// luaCall is redis.call: run the command; an error reply aborts the
// script by raising the {err} table.
func (m *Manager) luaCall(vm *host.VM, args []host.Value) ([]host.Value, error) {
	return m.redisCallCommon(vm, args, false)
}

// luaPCall is redis.pcall: an error reply comes back as a {err} table
// instead of aborting the script.
func (m *Manager) luaPCall(vm *host.VM, args []host.Value) ([]host.Value, error) {
	return m.redisCallCommon(vm, args, true)
}

func (m *Manager) redisCallCommon(vm *host.VM, args []host.Value, pcall bool) ([]host.Value, error) {
	fail := func(text string) ([]host.Value, error) {
		if pcall {
			return []host.Value{pcallErrTable(text)}, nil
		}
		return nil, raiseErr(text)
	}
	if len(args) == 0 {
		return fail("ERR Please specify at least one argument for this redis lib call")
	}
	argv := make([]string, len(args))
	for i, a := range args {
		switch a.Kind {
		case host.KindString:
			argv[i] = a.Str
		case host.KindNumber:
			argv[i] = NumToArg(a.Num) // S7 host-side formatting (probed rules)
		default:
			return fail("ERR Lua redis lib command arguments must be strings or integers")
		}
	}
	r := m.findRun(vm)
	if r == nil || r.call == nil {
		return fail("ERR redis.call is not available in this build")
	}
	reply, wrote := r.call(argv)
	if wrote {
		r.wrote.Store(true)
	}
	if reply.Kind == resp.KindError {
		return fail(reply.Str)
	}
	return []host.Value{respToLua(reply, r.respVer)}, nil
}

// luaErrorReply is redis.error_reply(s): the script RETURNS the {err}
// table; Redis prepends "ERR " when s is a bare word with no space (the
// first word of an error is its code — probed). Bad usage itself returns
// the usage-error table (probed: no script suffix in that case — it is a
// return value, not a raise).
func (m *Manager) luaErrorReply(_ *host.VM, args []host.Value) ([]host.Value, error) {
	if len(args) != 1 || args[0].Kind != host.KindString {
		return []host.Value{errTable("ERR wrong number or type of arguments")}, nil
	}
	s := args[0].Str
	if !strings.Contains(s, " ") {
		s = "ERR " + s
	}
	return []host.Value{errTable(s)}, nil
}

// luaStatusReply is redis.status_reply(s) — same shape as error_reply.
func (m *Manager) luaStatusReply(_ *host.VM, args []host.Value) ([]host.Value, error) {
	if len(args) != 1 || args[0].Kind != host.KindString {
		return []host.Value{errTable("ERR wrong number or type of arguments")}, nil
	}
	return []host.Value{okTable(args[0].Str)}, nil
}

// luaSHA1Hex is redis.sha1hex(s).
func (m *Manager) luaSHA1Hex(_ *host.VM, args []host.Value) ([]host.Value, error) {
	if len(args) != 1 {
		return nil, raiseErr("ERR wrong number of arguments")
	}
	var s string
	switch args[0].Kind {
	case host.KindString:
		s = args[0].Str
	case host.KindNumber:
		s = NumToArg(args[0].Num)
	default:
		return nil, raiseErr("ERR wrong number of arguments")
	}
	sum := sha1.Sum([]byte(s))
	return []host.Value{host.String(hex.EncodeToString(sum[:]))}, nil
}

// luaLog is redis.log(level, message): routed to the server log at the
// matching slog level; LOG_DEBUG/VERBOSE map to slog debug, NOTICE to
// info, WARNING to warn.
func (m *Manager) luaLog(_ *host.VM, args []host.Value) ([]host.Value, error) {
	if len(args) != 2 {
		return nil, raiseErr("ERR wrong number of arguments")
	}
	if args[0].Kind != host.KindNumber || args[1].Kind != host.KindString {
		return nil, raiseErr("ERR wrong number or type of arguments")
	}
	switch int(args[0].Num) {
	case 0, 1: // LOG_DEBUG, LOG_VERBOSE
		m.logger.Debug("lua", "msg", args[1].Str)
	case 2: // LOG_NOTICE
		m.logger.Info("lua", "msg", args[1].Str)
	case 3: // LOG_WARNING
		m.logger.Warn("lua", "msg", args[1].Str)
	default:
		return nil, raiseErr("ERR Invalid log level")
	}
	return nil, nil
}

// luaSetResp is redis.setresp(2|3): switches the RESP→Lua conversion of
// later redis.call results on this run (aggregate replies become Lua
// {map=...}/{set=...} wrappers, doubles {double=...}, nulls nil). Error
// texts probed.
func (m *Manager) luaSetResp(vm *host.VM, args []host.Value) ([]host.Value, error) {
	if len(args) != 1 {
		return nil, raiseErr("ERR redis.setresp() requires one argument.")
	}
	var v int
	switch args[0].Kind {
	case host.KindNumber:
		v = int(args[0].Num)
	case host.KindString:
		n, err := strconv.Atoi(args[0].Str)
		if err != nil {
			return nil, raiseErr("ERR RESP version must be 2 or 3.")
		}
		v = n
	default:
		return nil, raiseErr("ERR RESP version must be 2 or 3.")
	}
	if v != 2 && v != 3 {
		return nil, raiseErr("ERR RESP version must be 2 or 3.")
	}
	if r := m.findRun(vm); r != nil {
		r.respVer = v
	}
	return nil, nil
}

// RunErrorReply maps a Run error to the client reply: a ScriptError is
// shaped per the probed rules (ErrorReply); a TrapError or host failure
// is, by contract, a backend bug — logged with the module SHA, answered
// with an internal error, never surfaced as a script error.
func (m *Manager) RunErrorReply(sha string, err error) resp.Value {
	var se *host.ScriptError
	if errors.As(err, &se) {
		return ErrorReply(sha, se)
	}
	var te *host.TrapError
	if errors.As(err, &te) {
		m.logger.Error("lua backend trap (backend bug class)", "sha", sha, "err", te.Err)
	} else {
		m.logger.Error("lua host error", "sha", sha, "err", err)
	}
	return resp.Err("ERR Internal error running script")
}

// CompileErrorReply wraps a frontend compile error in Redis's form. The
// inner wording follows the gopher-lua frontend, not PUC Lua (ledgered
// divergence — docs/Redis-Errors.md §11).
func CompileErrorReply(err error) resp.Value {
	return resp.Err(fmt.Sprintf("ERR Error compiling script (new function): %s", err.Error()))
}
