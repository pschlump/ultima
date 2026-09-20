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
	def("reset", 1, []string{"fast"}, 0, 0, 0, "connection", cmdReset)

	// transaction
	def("multi", 1, []string{"fast"}, 0, 0, 0, "transaction", cmdMulti)
	def("exec", 1, []string{"write"}, 0, 0, 0, "transaction", cmdExec)
	def("discard", 1, []string{"fast"}, 0, 0, 0, "transaction", cmdDiscard)
	def("unwatch", 1, []string{"fast"}, 0, 0, 0, "transaction", cmdUnwatch)
	def("watch", -2, []string{"fast"}, 1, -1, 1, "transaction", cmdWatch)

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
	def("incrbyfloat", 3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "string", cmdIncrByFloat)
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
	def("expireat", -3, []string{"write", "fast"}, 1, 1, 1, "keyspace", cmdExpireAt)
	def("pexpireat", -3, []string{"write", "fast"}, 1, 1, 1, "keyspace", cmdPExpireAt)
	def("ttl", 2, []string{"readonly", "fast"}, 1, 1, 1, "keyspace", cmdTTL)
	def("pttl", 2, []string{"readonly", "fast"}, 1, 1, 1, "keyspace", cmdPTTL)
	def("persist", 2, []string{"write", "fast"}, 1, 1, 1, "keyspace", cmdPersist)
	def("type", 2, []string{"readonly", "fast"}, 1, 1, 1, "keyspace", cmdType)
	def("scan", -2, []string{"readonly"}, 0, 0, 0, "keyspace", cmdScan)

	// hash
	def("hset", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "hash", cmdHSet)
	def("hget", 3, []string{"readonly", "fast"}, 1, 1, 1, "hash", cmdHGet)
	def("hmset", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "hash", cmdHMSet)
	def("hmget", -3, []string{"readonly", "fast"}, 1, 1, 1, "hash", cmdHMGet)
	def("hgetall", 2, []string{"readonly"}, 1, 1, 1, "hash", cmdHGetAll)
	def("hdel", -3, []string{"write", "fast"}, 1, 1, 1, "hash", cmdHDel)
	def("hexists", 3, []string{"readonly", "fast"}, 1, 1, 1, "hash", cmdHExists)
	def("hlen", 2, []string{"readonly", "fast"}, 1, 1, 1, "hash", cmdHLen)
	def("hkeys", 2, []string{"readonly"}, 1, 1, 1, "hash", cmdHKeys)
	def("hvals", 2, []string{"readonly"}, 1, 1, 1, "hash", cmdHVals)
	def("hincrby", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "hash", cmdHIncrBy)
	def("hincrbyfloat", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "hash", cmdHIncrByFloat)
	def("hsetnx", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "hash", cmdHSetNX)
	def("hstrlen", 3, []string{"readonly", "fast"}, 1, 1, 1, "hash", cmdHStrLen)
	def("hrandfield", -2, []string{"readonly"}, 1, 1, 1, "hash", cmdHRandField)
	def("hscan", -3, []string{"readonly"}, 0, 0, 0, "hash", cmdHScan)

	// list
	def("lpush", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "list", cmdLPush)
	def("rpush", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "list", cmdRPush)
	def("lpushx", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "list", cmdLPushX)
	def("rpushx", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "list", cmdRPushX)
	def("lpop", -2, []string{"write", "fast"}, 1, 1, 1, "list", cmdLPop)
	def("rpop", -2, []string{"write", "fast"}, 1, 1, 1, "list", cmdRPop)
	def("llen", 2, []string{"readonly", "fast"}, 1, 1, 1, "list", cmdLLen)
	def("lrange", 4, []string{"readonly"}, 1, 1, 1, "list", cmdLRange)
	def("lindex", 3, []string{"readonly"}, 1, 1, 1, "list", cmdLIndex)
	def("lset", 4, []string{"write", "denyoom"}, 1, 1, 1, "list", cmdLSet)
	def("linsert", 5, []string{"write", "denyoom"}, 1, 1, 1, "list", cmdLInsert)
	def("lrem", 4, []string{"write"}, 1, 1, 1, "list", cmdLRem)
	def("ltrim", 4, []string{"write"}, 1, 1, 1, "list", cmdLTrim)
	def("rpoplpush", 3, []string{"write", "denyoom"}, 1, 2, 1, "list", cmdRPopLPush)
	def("lpos", -3, []string{"readonly"}, 1, 1, 1, "list", cmdLPos)
	def("lmove", 5, []string{"write", "denyoom"}, 1, 2, 1, "list", cmdLMove)
	def("blpop", -3, []string{"write", "blocking"}, 1, -2, 1, "list", cmdBLPop)
	def("brpop", -3, []string{"write", "blocking"}, 1, -2, 1, "list", cmdBRPop)
	def("blmove", 6, []string{"write", "denyoom", "blocking"}, 1, 2, 1, "list", cmdBLMove)
	def("blmpop", -5, []string{"write", "blocking"}, 0, 0, 0, "list", cmdBLMPop)
	def("brpoplpush", 4, []string{"write", "denyoom", "blocking"}, 1, 2, 1, "list", cmdBRPopLPush)

	// set
	def("sadd", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1, "set", cmdSAdd)
	def("srem", -3, []string{"write", "fast"}, 1, 1, 1, "set", cmdSRem)
	def("smembers", 2, []string{"readonly"}, 1, 1, 1, "set", cmdSMembers)
	def("sismember", 3, []string{"readonly", "fast"}, 1, 1, 1, "set", cmdSIsMember)
	def("smismember", -3, []string{"readonly", "fast"}, 1, 1, 1, "set", cmdSMIsMember)
	def("scard", 2, []string{"readonly", "fast"}, 1, 1, 1, "set", cmdSCard)
	def("spop", -2, []string{"write", "random"}, 1, 1, 1, "set", cmdSPop)
	def("srandmember", -2, []string{"readonly", "random"}, 1, 1, 1, "set", cmdSRandMember)
	def("smove", 4, []string{"write", "fast"}, 1, 2, 1, "set", cmdSMove)
	def("sinter", -2, []string{"readonly"}, 1, -1, 1, "set", cmdSInter)
	def("sunion", -2, []string{"readonly"}, 1, -1, 1, "set", cmdSUnion)
	def("sdiff", -2, []string{"readonly"}, 1, -1, 1, "set", cmdSDiff)
	def("sinterstore", -3, []string{"write", "denyoom"}, 1, -1, 1, "set", cmdSInterStore)
	def("sunionstore", -3, []string{"write", "denyoom"}, 1, -1, 1, "set", cmdSUnionStore)
	def("sdiffstore", -3, []string{"write", "denyoom"}, 1, -1, 1, "set", cmdSDiffStore)
	def("sintercard", -3, []string{"readonly"}, 0, 0, 0, "set", cmdSInterCard)
	def("sscan", -3, []string{"readonly"}, 0, 0, 0, "set", cmdSScan)

	// sorted set
	def("zadd", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "sorted_set", cmdZAdd)
	def("zscore", 3, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZScore)
	def("zmscore", -3, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZMScore)
	def("zincrby", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1, "sorted_set", cmdZIncrBy)
	def("zrank", -3, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZRank)
	def("zrevrank", -3, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZRevRank)
	def("zrange", -4, []string{"readonly"}, 1, 1, 1, "sorted_set", cmdZRange)
	def("zrangebyscore", -4, []string{"readonly"}, 1, 1, 1, "sorted_set", cmdZRangeByScore)
	def("zrangebylex", -4, []string{"readonly"}, 1, 1, 1, "sorted_set", cmdZRangeByLex)
	def("zrevrange", -4, []string{"readonly"}, 1, 1, 1, "sorted_set", cmdZRevRange)
	def("zrevrangebyscore", -4, []string{"readonly"}, 1, 1, 1, "sorted_set", cmdZRevRangeByScore)
	def("zremrangebyrank", 4, []string{"write"}, 1, 1, 1, "sorted_set", cmdZRemRangeByRank)
	def("zremrangebyscore", 4, []string{"write"}, 1, 1, 1, "sorted_set", cmdZRemRangeByScore)
	def("zremrangebylex", 4, []string{"write"}, 1, 1, 1, "sorted_set", cmdZRemRangeByLex)
	def("zcard", 2, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZCard)
	def("zcount", 4, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZCount)
	def("zlexcount", 4, []string{"readonly", "fast"}, 1, 1, 1, "sorted_set", cmdZLexCount)
	def("zrem", -3, []string{"write", "fast"}, 1, 1, 1, "sorted_set", cmdZRem)
	def("zpopmin", -2, []string{"write", "fast"}, 1, 1, 1, "sorted_set", cmdZPopMin)
	def("zpopmax", -2, []string{"write", "fast"}, 1, 1, 1, "sorted_set", cmdZPopMax)
	def("bzpopmin", -3, []string{"write", "blocking"}, 1, -2, 1, "sorted_set", cmdBZPopMin)
	def("bzpopmax", -3, []string{"write", "blocking"}, 1, -2, 1, "sorted_set", cmdBZPopMax)
	def("bzmpop", -5, []string{"write", "blocking"}, 0, 0, 0, "sorted_set", cmdBZMPop)
	def("zrandmember", -2, []string{"readonly", "random"}, 1, 1, 1, "sorted_set", cmdZRandMember)
	def("zdiff", -3, []string{"readonly"}, 0, 0, 0, "sorted_set", cmdZDiff)
	def("zinter", -3, []string{"readonly"}, 0, 0, 0, "sorted_set", cmdZInter)
	def("zunion", -3, []string{"readonly"}, 0, 0, 0, "sorted_set", cmdZUnion)
	def("zinterstore", -4, []string{"write", "denyoom"}, 0, 0, 0, "sorted_set", cmdZInterStore)
	def("zunionstore", -4, []string{"write", "denyoom"}, 0, 0, 0, "sorted_set", cmdZUnionStore)
	def("zscan", -3, []string{"readonly"}, 0, 0, 0, "sorted_set", cmdZScan)

	// pubsub
	def("subscribe", -2, []string{"fast"}, 0, 0, 0, "pubsub", cmdSubscribe)
	def("psubscribe", -2, []string{"fast"}, 0, 0, 0, "pubsub", cmdPSubscribe)
	def("unsubscribe", -1, []string{"fast"}, 0, 0, 0, "pubsub", cmdUnsubscribe)
	def("punsubscribe", -1, []string{"fast"}, 0, 0, 0, "pubsub", cmdPUnsubscribe)
	def("publish", 3, []string{"fast"}, 0, 0, 0, "pubsub", cmdPublish)
	def("pubsub", -2, []string{"random"}, 0, 0, 0, "pubsub", cmdPubSub)

	// server
	def("info", -1, []string{"readonly"}, 0, 0, 0, "server", cmdInfo)
	def("dbsize", 1, []string{"readonly", "fast"}, 0, 0, 0, "server", cmdDBSize)
	def("flushdb", -1, []string{"write"}, 0, 0, 0, "server", cmdFlushDB)
	def("flushall", -1, []string{"write"}, 0, 0, 0, "server", cmdFlushAll)
	def("config", -2, []string{"admin"}, 0, 0, 0, "server", cmdConfig)
	def("client", -2, []string{"admin"}, 0, 0, 0, "server", cmdClient)
	def("command", -1, []string{"readonly"}, 0, 0, 0, "server", cmdCommand)
	def("monitor", 1, []string{"admin"}, 0, 0, 0, "server", cmdMonitor)
	def("save", 1, []string{"admin"}, 0, 0, 0, "server", cmdSave)
	def("bgsave", 1, []string{"admin"}, 0, 0, 0, "server", cmdBGSave)
	def("lastsave", 1, []string{"readonly", "fast"}, 0, 0, 0, "server", cmdLastSave)
	def("bgrewriteaof", 1, []string{"admin"}, 0, 0, 0, "server", cmdBGRewriteAOF)
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
