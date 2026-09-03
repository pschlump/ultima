package commands

import (
	"github.com/pschlump/ultima/lib/resp"
)

// CmdDef is one command-table entry: the executor plus the metadata
// COMMAND INFO reports (design doc §7).
type CmdDef struct {
	Name     string // lowercase, as Redis reports it in arity errors
	Arity    int    // >0 exact arg count (incl. name), <0 at least -Arity
	Flags    []string
	FirstKey int
	LastKey  int
	KeyStep  int
	Group    string
	Handler  func(e *Engine, cs *ConnState, args [][]byte) resp.Value
}

// FullName is the name arity errors report ("config|get" style for
// subcommands; plain Name for top-level commands).
func (d *CmdDef) FullName() string { return d.Name }

var table = map[string]*CmdDef{}

func def(name string, arity int, flags []string, first, last, step int, group string,
	h func(e *Engine, cs *ConnState, args [][]byte) resp.Value,
) {
	table[name] = &CmdDef{
		Name: name, Arity: arity, Flags: flags,
		FirstKey: first, LastKey: last, KeyStep: step, Group: group,
		Handler: h,
	}
}

func init() {
	// connection
	def("ping", -1, []string{"fast"}, 0, 0, 0, "connection", cmdPing)
	def("echo", 2, []string{"fast"}, 0, 0, 0, "connection", cmdEcho)
	def("hello", -1, []string{"fast"}, 0, 0, 0, "connection", cmdHello)
	def("auth", -2, []string{"fast"}, 0, 0, 0, "connection", cmdAuth)
	def("select", 2, []string{"fast"}, 0, 0, 0, "connection", cmdSelect)
	def("quit", -1, []string{"fast"}, 0, 0, 0, "connection", cmdQuit)

	// string
	def("set", -3, []string{"write", "denyoom"}, 1, 1, 1, "string", cmdSet)
	def("get", 2, []string{"readonly", "fast"}, 1, 1, 1, "string", cmdGet)
	def("getset", 3, []string{"write", "denyoom"}, 1, 1, 1, "string", cmdGetSet)
	def("getdel", 2, []string{"write", "fast"}, 1, 1, 1, "string", cmdGetDel)
	def("getex", -2, []string{"write", "fast"}, 1, 1, 1, "string", cmdGetEx)
	def("incr", 2, []string{"write", "denyoom", "fast"}, 1, 1, 1, "string", cmdIncr)
	def("decr", 2, []string{"write", "denyoom", "fast"}, 1, 1, 1, "string", cmdDecr)
	def("incrby", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "string", cmdIncrBy)
	def("decrby", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "string", cmdDecrBy)
	def("append", 3, []string{"write", "denyoom"}, 1, 1, 1, "string", cmdAppend)
	def("strlen", 2, []string{"readonly", "fast"}, 1, 1, 1, "string", cmdStrLen)
	def("mget", -2, []string{"readonly", "fast"}, 1, -1, 1, "string", cmdMGet)
	def("mset", -3, []string{"write", "denyoom"}, 1, -1, 2, "string", cmdMSet)
	def("msetnx", -3, []string{"write", "denyoom"}, 1, -1, 2, "string", cmdMSetNX)

	// keyspace / generic
	def("del", -2, []string{"write"}, 1, -1, 1, "keyspace", cmdDel)
	def("exists", -2, []string{"readonly", "fast"}, 1, -1, 1, "keyspace", cmdExists)
	def("expire", -3, []string{"write", "fast"}, 1, 1, 1, "keyspace", cmdExpire)
	def("pexpire", -3, []string{"write", "fast"}, 1, 1, 1, "keyspace", cmdPExpire)
	def("ttl", 2, []string{"readonly", "fast"}, 1, 1, 1, "keyspace", cmdTTL)
	def("pttl", 2, []string{"readonly", "fast"}, 1, 1, 1, "keyspace", cmdPTTL)
	def("persist", 2, []string{"write", "fast"}, 1, 1, 1, "keyspace", cmdPersist)
	def("type", 2, []string{"readonly", "fast"}, 1, 1, 1, "keyspace", cmdType)
	def("scan", -2, []string{"readonly"}, 0, 0, 0, "keyspace", cmdScan)

	// server
	def("info", -1, []string{"readonly"}, 0, 0, 0, "server", cmdInfo)
	def("dbsize", 1, []string{"readonly", "fast"}, 0, 0, 0, "server", cmdDBSize)
	def("flushdb", -1, []string{"write"}, 0, 0, 0, "server", cmdFlushDB)
	def("flushall", -1, []string{"write"}, 0, 0, 0, "server", cmdFlushAll)
	def("config", -2, []string{"admin"}, 0, 0, 0, "server", cmdConfig)
	def("client", -2, []string{"admin"}, 0, 0, 0, "server", cmdClient)
	def("command", -1, []string{"readonly"}, 0, 0, 0, "server", cmdCommand)
}

// CommandCount reports the command table size (COMMAND COUNT).
func CommandCount() int { return len(table) }

// CommandInfo looks up a table entry by lowercase name.
func CommandInfo(name string) (*CmdDef, bool) {
	d, ok := table[name]
	return d, ok
}

// CommandNames returns every registered name (COMMAND INFO with no args).
func CommandNames() []string {
	names := make([]string, 0, len(table))
	for n := range table {
		names = append(names, n)
	}
	return names
}
