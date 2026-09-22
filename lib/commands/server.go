package commands

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// --- connection ---------------------------------------------------------------

func cmdPing(_ *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args) > 2 {
		return errArity("ping")
	}
	// A RESP2 connection in subscribe mode gets ["pong", msg|""] instead
	// of +PONG/$msg (Redis pingCommand; RESP3 connections are never in
	// subscribe mode and reply normally).
	if cs.Proto == 2 && cs.subCount() > 0 {
		msg := ""
		if len(args) == 2 {
			msg = string(args[1])
		}
		return resp.Arr(resp.BlobStr("pong"), resp.BlobStr(msg))
	}
	if len(args) == 2 {
		return resp.BlobString(args[1])
	}
	return resp.Simple("PONG")
}

func cmdEcho(_ *Engine, _ *ConnState, args [][]byte) resp.Value {
	return resp.BlobString(args[1])
}

func cmdQuit(_ *Engine, cs *ConnState, _ [][]byte) resp.Value {
	cs.Quit = true
	return replyOK
}

// cmdReset resets the connection to its freshly-connected state (Redis
// RESET, verified against 7.2.7): any transaction (queue, queue errors,
// watches) is discarded, all pub/sub channel and pattern subscriptions
// are dropped, the connection drops back to DB 0, loses its client name
// and its authentication, and reverts to RESP2. Inside MULTI RESET is
// executed immediately, discarding the transaction (it is part of the
// queue-gate exclusion set).
func cmdReset(e *Engine, cs *ConnState, _ [][]byte) resp.Value {
	e.PubSub.UnsubscribeAll(cs.ID)
	cs.subs, cs.psubs = nil, nil
	cs.clearTx()
	cs.DB = 0
	cs.Name = ""
	cs.Authed = false
	cs.Proto = 2
	return resp.Simple("RESET")
}

// authAttempt validates user/password against requirepass. With no
// requirepass the default user is nopass: any password authenticates it,
// and unknown users are rejected (Redis 7.2 ACL semantics).
func (e *Engine) authAttempt(user, pass string) resp.Value {
	pw := e.RequirePass()
	if pw == "" {
		if user != "default" {
			return resp.Err("WRONGPASS invalid username-password pair or user is disabled.")
		}
		return resp.Value{}
	}
	if user != "default" || pass != pw {
		return resp.Err("WRONGPASS invalid username-password pair or user is disabled.")
	}
	return resp.Value{}
}

func cmdAuth(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if len(args) > 3 {
		return errSyntax
	}
	user, pass := "default", string(args[1])
	if len(args) == 3 {
		user, pass = string(args[1]), string(args[2])
	}
	if pw := e.RequirePass(); pw == "" && len(args) == 2 {
		return resp.Err("ERR AUTH <password> called without any password configured for the " +
			"default user. Are you sure your configuration is correct?")
	}
	if errV := e.authAttempt(user, pass); errV.Kind == resp.KindError {
		return errV
	}
	cs.Authed = true
	return replyOK
}

func cmdHello(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	proto := cs.Proto
	if len(args) >= 2 {
		v, ok := parseIntStrict(args[1])
		if !ok {
			return resp.Err("ERR Protocol version is not an integer or out of range")
		}
		if v != 2 && v != 3 {
			return resp.Err("NOPROTO unsupported protocol version")
		}
		proto = int(v)
	}
	var user, pass, name string
	hasAuth := false
	hasName := false
	for i := 2; i < len(args); {
		switch opt := strings.ToUpper(string(args[i])); opt {
		case "AUTH":
			if i+2 >= len(args) {
				return resp.Err("ERR Syntax error in HELLO option 'AUTH'")
			}
			user, pass, hasAuth = string(args[i+1]), string(args[i+2]), true
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				return resp.Err("ERR Syntax error in HELLO option 'SETNAME'")
			}
			name, hasName = string(args[i+1]), true
			i += 2
		default:
			// Redis reports the option in the client's original casing.
			return resp.Err(fmt.Sprintf("ERR Syntax error in HELLO option '%s'", string(args[i])))
		}
	}
	if hasAuth {
		if errV := e.authAttempt(user, pass); errV.Kind == resp.KindError {
			return errV
		}
		cs.Authed = true
	}
	if hasName {
		cs.Name = name
	}
	cs.Proto = proto
	return e.helloReply(cs)
}

func (e *Engine) helloReply(cs *ConnState) resp.Value {
	return resp.Map(
		resp.BlobStr("server"), resp.BlobStr("redis"),
		resp.BlobStr("version"), resp.BlobStr(CompatVersion),
		resp.BlobStr("proto"), resp.Int(int64(cs.Proto)),
		resp.BlobStr("id"), resp.Int(int64(cs.ID)),
		resp.BlobStr("mode"), resp.BlobStr("standalone"),
		resp.BlobStr("role"), resp.BlobStr("master"),
		resp.BlobStr("modules"), resp.Arr(),
	)
}

func cmdSelect(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	v, ok := parseIntStrict(args[1])
	if !ok {
		return errNotInt
	}
	if v < 0 || v >= int64(e.Shards.MaxDBs()) {
		return resp.Err("ERR DB index is out of range")
	}
	cs.DB = int(v)
	return replyOK
}

// --- INFO ----------------------------------------------------------------------

func cmdInfo(e *Engine, _ *ConnState, args [][]byte) resp.Value {
	section := ""
	if len(args) == 2 {
		section = lowerASCII(args[1])
	}
	if section == "script" {
		// Redis 7.2.7 has no scripting INFO section, so plain INFO must
		// not gain one (the differential mInfo gate compares section
		// sets); the counters are emitted only on an explicit request.
		var sb strings.Builder
		sb.WriteString("# Script\r\n")
		if e.Scripts != nil {
			fmt.Fprintf(&sb, "loaded_scripts:%d\r\n", e.Scripts.Cached())
			fmt.Fprintf(&sb, "script_time_limit_ms:%d\r\n", e.Scripts.TimeLimitMs())
			fmt.Fprintf(&sb, "script_hard_deadline_ms:%d\r\n", e.Scripts.HardDeadlineMs())
			fmt.Fprintf(&sb, "script_max_memory_mb:%d\r\n", e.ScriptMaxMemoryMB())
		} else {
			sb.WriteString("loaded_scripts:0\r\n")
		}
		sb.WriteString("\r\n")
		return resp.BlobStr(sb.String())
	}
	var sb strings.Builder
	writeSection := func(name string, body func()) {
		if section != "" && section != "all" && section != "default" && section != "everything" && section != name {
			return
		}
		fmt.Fprintf(&sb, "# %s\r\n", sectionHeader(name))
		body()
		sb.WriteString("\r\n")
	}
	writeSection("server", func() {
		uptime := int64(time.Since(e.Started).Seconds())
		fmt.Fprintf(&sb, "redis_version:%s\r\n", CompatVersion)
		fmt.Fprintf(&sb, "ultima_version:%s\r\n", e.Version)
		sb.WriteString("redis_mode:standalone\r\n")
		fmt.Fprintf(&sb, "os:%s %s\r\n", runtime.GOOS, runtime.GOARCH)
		sb.WriteString("arch_bits:64\r\n")
		fmt.Fprintf(&sb, "process_id:%d\r\n", os.Getpid())
		fmt.Fprintf(&sb, "run_id:%s\r\n", e.RunID)
		fmt.Fprintf(&sb, "tcp_port:%d\r\n", e.RespPort)
		fmt.Fprintf(&sb, "uptime_in_seconds:%d\r\n", uptime)
		fmt.Fprintf(&sb, "uptime_in_days:%d\r\n", uptime/86400)
		sb.WriteString("hz:10\r\n")
	})
	writeSection("clients", func() {
		fmt.Fprintf(&sb, "connected_clients:%d\r\n", e.conns.Load())
		fmt.Fprintf(&sb, "blocked_clients:%d\r\n", e.blockedClients.Load())
	})
	writeSection("memory", func() {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		// used_memory is the keyspace estimate (M5b) — the number the
		// maxmemory gate enforces; Go's heap total is reported separately
		// (no Redis equivalent; absolute values diverge by design, D10).
		fmt.Fprintf(&sb, "used_memory:%d\r\n", e.Shards.UsedBytes())
		fmt.Fprintf(&sb, "used_memory_process:%d\r\n", ms.Alloc)
		fmt.Fprintf(&sb, "maxmemory:%d\r\n", e.MaxMemory())
		fmt.Fprintf(&sb, "maxmemory_policy:%s\r\n", e.Shards.Policy())
	})
	writeSection("persistence", func() {
		if p := e.persister; p != nil {
			p.InfoPersistence(&sb)
		} else {
			sb.WriteString("loading:0\r\n")
			sb.WriteString("rdb_changes_since_last_save:0\r\n")
			sb.WriteString("rdb_bgsave_in_progress:0\r\n")
			sb.WriteString("rdb_last_save_time:0\r\n")
			sb.WriteString("rdb_last_bgsave_status:ok\r\n")
			sb.WriteString("aof_enabled:0\r\n")
			sb.WriteString("aof_rewrite_in_progress:0\r\n")
			sb.WriteString("aof_last_bgrewrite_status:ok\r\n")
		}
	})
	writeSection("stats", func() {
		fmt.Fprintf(&sb, "total_connections_received:%d\r\n", e.totalConns.Load())
		fmt.Fprintf(&sb, "total_commands_processed:%d\r\n", e.totalCmds.Load())
		fmt.Fprintf(&sb, "expired_keys:%d\r\n", e.Shards.ExpiredKeys.Load())
		fmt.Fprintf(&sb, "evicted_keys:%d\r\n", e.Shards.EvictedKeys.Load())
		fmt.Fprintf(&sb, "pubsub_channels:%d\r\n", e.PubSub.NumChannels())
		fmt.Fprintf(&sb, "pubsub_patterns:%d\r\n", e.PubSub.NumPat())
	})
	writeSection("replication", func() {
		sb.WriteString("role:master\r\n")
		sb.WriteString("connected_slaves:0\r\n")
	})
	writeSection("cpu", func() {
		sb.WriteString("used_cpu_sys:0.000000\r\n")
		sb.WriteString("used_cpu_user:0.000000\r\n")
	})
	writeSection("modules", func() {
	})
	writeSection("errorstats", func() {
	})
	writeSection("cluster", func() {
		sb.WriteString("cluster_enabled:0\r\n")
	})
	writeSection("keyspace", func() {
		for _, st := range e.Shards.DBStats() {
			fmt.Fprintf(&sb, "db%d:keys=%d,expires=%d,avg_ttl=0\r\n", st[0], st[1], st[2])
		}
	})
	return resp.BlobStr(sb.String())
}

func sectionHeader(name string) string {
	if name == "cpu" {
		return "CPU"
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

// --- server ops -----------------------------------------------------------------

func cmdDBSize(e *Engine, cs *ConnState, _ [][]byte) resp.Value {
	return resp.Int(int64(e.Shards.DBSizeTok(cs.tok, cs.DB)))
}

// flushArgs validates the optional SYNC/ASYNC modifier (accepted, always
// synchronous in M1 — no persistence yet).
func flushArgs(args [][]byte) (resp.Value, bool) {
	if len(args) == 1 {
		return resp.Value{}, false
	}
	if len(args) == 2 {
		switch lowerASCII(args[1]) {
		case "sync", "async":
			return resp.Value{}, false
		}
		return errSyntax, true
	}
	return errSyntax, true // Redis: FLUSHDB with extra args is a syntax error
}

func cmdFlushDB(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if errV, failed := flushArgs(args); failed {
		return errV
	}
	e.Shards.FlushDBTok(cs.tok, cs.DB)
	return replyOK
}

func cmdFlushAll(e *Engine, cs *ConnState, args [][]byte) resp.Value {
	if errV, failed := flushArgs(args); failed {
		return errV
	}
	e.Shards.FlushAllTok(cs.tok)
	return replyOK
}

// --- CONFIG ----------------------------------------------------------------------

// configParam describes one entry of the M1 CONFIG subset.
type configParam struct {
	name string
	get  func(e *Engine) string
	set  func(e *Engine, v string) (resp.Value, bool) // nil = immutable
}

var configParams = []configParam{
	{name: "appendfsync",
		get: func(e *Engine) string {
			if p := e.persister; p != nil {
				return p.AppendFsync()
			}
			return "everysec"
		},
		set: func(e *Engine, v string) (resp.Value, bool) {
			switch strings.ToLower(v) {
			case "everysec", "always", "no":
			default:
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'appendfsync') - argument(s) must be one of the following: everysec, always, no"), true
			}
			if p := e.persister; p != nil {
				p.SetAppendFsync(strings.ToLower(v))
			}
			return resp.Value{}, false
		}},
	{name: "appendonly",
		get: func(e *Engine) string {
			if e.appendOnly.Load() {
				return "yes"
			}
			return "no"
		},
		set: func(e *Engine, v string) (resp.Value, bool) {
			var on bool
			switch strings.ToLower(v) {
			case "yes":
				on = true
			case "no":
			default:
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'"), true
			}
			e.appendOnly.Store(on)
			if p := e.persister; p != nil {
				if err := p.SetAppendOnly(on); err != nil {
					return resp.Err("ERR CONFIG SET failed (possibly related to argument 'appendonly') - " + err.Error()), true
				}
			}
			return resp.Value{}, false
		}},
	{name: "databases",
		get: func(e *Engine) string { return fmt.Sprintf("%d", e.Shards.MaxDBs()) },
		set: nil},
	{name: "dbfilename",
		get: func(e *Engine) string {
			if p := e.persister; p != nil {
				return p.DbFilename()
			}
			return "dump.rdb"
		},
		// Redis 7.2.7: dbfilename is a protected config, not settable.
		set: func(_ *Engine, _ string) (resp.Value, bool) {
			return resp.Err("ERR CONFIG SET failed (possibly related to argument 'dbfilename') - can't set protected config"), true
		}},
	{name: "dir",
		get: func(e *Engine) string {
			if p := e.persister; p != nil {
				return p.Dir()
			}
			return ""
		},
		// Redis 7.2.7: dir is a protected config, not settable at runtime.
		set: func(_ *Engine, _ string) (resp.Value, bool) {
			return resp.Err("ERR CONFIG SET failed (possibly related to argument 'dir') - can't set protected config"), true
		}},
	{name: "maxmemory",
		get: func(e *Engine) string { return fmt.Sprintf("%d", e.MaxMemory()) },
		set: func(e *Engine, v string) (resp.Value, bool) {
			n, ok := parseMemBytes([]byte(v))
			if !ok {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value"), true
			}
			e.SetMaxMemory(n)
			return resp.Value{}, false
		}},
	{name: "maxmemory-policy",
		get: func(e *Engine) string { return e.Shards.Policy().String() },
		set: func(e *Engine, v string) (resp.Value, bool) {
			p, ok := shard.ParseEvictPolicy(v)
			if !ok {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: " + strings.Join(shard.EvictPolicyNames, ", ")), true
			}
			e.Shards.SetPolicy(p)
			return resp.Value{}, false
		}},
	{name: "notify-keyspace-events",
		get: func(e *Engine) string { return e.NotifyKeyspaceEvents() },
		set: func(e *Engine, v string) (resp.Value, bool) {
			if !e.SetNotifyKeyspaceEvents(v) {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use '" + notifyClassChars + "'."), true
			}
			return resp.Value{}, false
		}},
	{name: "lua-time-limit",
		get: func(e *Engine) string {
			if e.Scripts == nil {
				return "5000"
			}
			return fmt.Sprintf("%d", e.Scripts.TimeLimitMs())
		},
		set: func(e *Engine, v string) (resp.Value, bool) {
			// Byte-exact against 7.2.7 (probed).
			n, ok := parseIntStrict([]byte(v))
			if !ok {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'lua-time-limit') - argument couldn't be parsed into an integer"), true
			}
			if n < 0 {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'lua-time-limit') - argument must be between 0 and 9223372036854775807 inclusive"), true
			}
			if e.Scripts != nil {
				e.Scripts.SetTimeLimitMs(n)
			}
			return resp.Value{}, false
		}},
	{name: "script-hard-deadline-ms", // Ultima extension (S5 watchdog kill)
		get: func(e *Engine) string {
			if e.Scripts == nil {
				return "30000"
			}
			return fmt.Sprintf("%d", e.Scripts.HardDeadlineMs())
		},
		set: nil}, // engine-level VM budget knob: fixed at startup
	{name: "script-max-memory-mb", // Ultima extension (per-VM Lua budget)
		get: func(e *Engine) string { return fmt.Sprintf("%d", e.ScriptMaxMemoryMB()) },
		set: nil},
	{name: "script-rng-seed", // Ultima extension (S6 determinism)
		get: func(e *Engine) string {
			if e.Scripts == nil {
				return "0"
			}
			return fmt.Sprintf("%d", e.Scripts.RngSeedBase())
		},
		set: func(e *Engine, v string) (resp.Value, bool) {
			n, ok := parseIntStrict([]byte(v))
			if !ok {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'script-rng-seed') - argument couldn't be parsed into an integer"), true
			}
			if e.Scripts != nil {
				e.Scripts.SetRngSeedBase(n)
			}
			return resp.Value{}, false
		}},
	{name: "requirepass",
		get: func(e *Engine) string { return e.RequirePass() }, // Redis 7.2 does not mask it
		set: func(e *Engine, v string) (resp.Value, bool) {
			e.SetRequirePass(v)
			return resp.Value{}, false
		}},
	{name: "save",
		get: func(e *Engine) string { return e.save.Load().(string) },
		set: func(e *Engine, v string) (resp.Value, bool) {
			if !validSaveParam(v) {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters"), true
			}
			e.save.Store(v)
			if p := e.persister; p != nil {
				p.SetSaveRules(v) // M5c: the save string now schedules
			}
			return resp.Value{}, false
		}},
	{name: "slowlog-log-slower-than",
		get: func(e *Engine) string { return fmt.Sprintf("%d", e.SlowlogSlowerThan()) },
		set: func(e *Engine, v string) (resp.Value, bool) {
			// Byte-exact against 7.2.7 (probed): bad integer → "couldn't
			// be parsed into an integer"; below -1 → the range error.
			n, ok := parseIntStrict([]byte(v))
			if !ok {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - argument couldn't be parsed into an integer"), true
			}
			if n < -1 {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - argument must be between -1 and 9223372036854775807 inclusive"), true
			}
			e.SetSlowlogSlowerThan(n)
			return resp.Value{}, false
		}},
	{name: "slowlog-max-len",
		get: func(e *Engine) string { return fmt.Sprintf("%d", e.SlowlogMaxLen()) },
		set: func(e *Engine, v string) (resp.Value, bool) {
			n, ok := parseIntStrict([]byte(v))
			if !ok {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument couldn't be parsed into an integer"), true
			}
			if n < 0 {
				return resp.Err("ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument must be between 0 and 9223372036854775807 inclusive"), true
			}
			e.SetSlowlogMaxLen(n)
			return resp.Value{}, false
		}},
}

// validSaveParam accepts "" or whitespace-separated "seconds changes" pairs.
func validSaveParam(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" || v == `""` {
		return true
	}
	fields := strings.Fields(v)
	if len(fields)%2 != 0 {
		return false
	}
	for _, f := range fields {
		if _, ok := parseIntStrict([]byte(f)); !ok {
			return false
		}
	}
	return true
}

func cmdConfig(e *Engine, _ *ConnState, args [][]byte) resp.Value {
	sub := lowerASCII(args[1])
	switch sub {
	case "get":
		if len(args) < 3 {
			return errArity("config|get")
		}
		var pairs []resp.Value
		for _, pat := range args[2:] {
			for _, p := range configParams {
				if GlobMatch([]byte(lowerASCII(pat)), []byte(p.name)) {
					pairs = append(pairs, resp.BlobStr(p.name), resp.BlobStr(p.get(e)))
				}
			}
		}
		if pairs == nil {
			pairs = []resp.Value{}
		}
		return resp.Map(pairs...)
	case "set":
		if len(args) < 3 {
			return errArity("config|set")
		}
		if (len(args)-2)%2 != 0 {
			return errSyntax
		}
		for i := 2; i+1 < len(args); i += 2 {
			name, val := lowerASCII(args[i]), string(args[i+1])
			var p *configParam
			for j := range configParams {
				if configParams[j].name == name {
					p = &configParams[j]
					break
				}
			}
			if p == nil {
				return resp.Err(fmt.Sprintf("ERR Unknown option or number of arguments for CONFIG SET - '%s'", name))
			}
			if p.set == nil {
				return resp.Err(fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - can't set immutable config", name))
			}
			if errV, failed := p.set(e, val); failed {
				return errV
			}
		}
		return replyOK
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s'. Try CONFIG HELP.", string(args[1])))
	}
}

// --- CLIENT ------------------------------------------------------------------------

func cmdClient(_ *Engine, cs *ConnState, args [][]byte) resp.Value {
	sub := lowerASCII(args[1])
	switch sub {
	case "setname":
		if len(args) != 3 {
			return errArity("client|setname")
		}
		name := string(args[2])
		for i := 0; i < len(name); i++ {
			if name[i] < '!' || name[i] > '~' {
				return resp.Err("ERR Client names cannot contain spaces, newlines or special characters.")
			}
		}
		cs.Name = name
		return replyOK
	case "getname":
		if len(args) != 2 {
			return errArity("client|getname")
		}
		if cs.Name == "" {
			return resp.Null()
		}
		return resp.BlobStr(cs.Name)
	case "id":
		if len(args) != 2 {
			return errArity("client|id")
		}
		return resp.Int(int64(cs.ID))
	case "setinfo":
		// client libraries (go-redis et al.) announce themselves with
		// CLIENT SETINFO lib-name/lib-ver; accepted and recorded in name
		// metadata only.
		if len(args) != 4 {
			return errArity("client|setinfo")
		}
		switch lowerASCII(args[2]) {
		case "lib-name", "lib-ver":
			return replyOK
		}
		return resp.Err(fmt.Sprintf("ERR Unrecognized option '%s'", string(args[2])))
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", string(args[1])))
	}
}

// --- COMMAND -----------------------------------------------------------------------

func cmdCommand(_ *Engine, _ *ConnState, args [][]byte) resp.Value {
	sub := "info" // bare COMMAND is COMMAND INFO with no arguments
	if len(args) > 1 {
		sub = lowerASCII(args[1])
	}
	switch sub {
	case "count":
		if len(args) != 2 {
			return errArity("command|count")
		}
		return resp.Int(int64(CommandCount()))
	case "info":
		var names []string
		if len(args) <= 2 {
			names = CommandNames()
			sort.Strings(names)
		} else {
			for _, a := range args[2:] {
				names = append(names, lowerASCII(a))
			}
		}
		elems := make([]resp.Value, 0, len(names))
		for _, n := range names {
			d, ok := CommandInfo(n)
			if !ok {
				elems = append(elems, resp.Null())
				continue
			}
			flags := make([]resp.Value, 0, len(d.Flags))
			for _, f := range d.Flags {
				flags = append(flags, resp.Simple(f))
			}
			elems = append(elems, resp.Arr(
				resp.BlobStr(d.Name),
				resp.Int(int64(d.Arity)),
				resp.Arr(flags...),
				resp.Int(int64(d.FirstKey)),
				resp.Int(int64(d.LastKey)),
				resp.Int(int64(d.KeyStep)),
			))
		}
		return resp.Arr(elems...)
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s'. Try COMMAND HELP.", string(args[1])))
	}
}
