package differential

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/persist"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/scripting"
	"github.com/pschlump/ultima/lib/shard"
)

// startUltima runs the full RESP stack in-process on an ephemeral port.
func startUltima(t *testing.T, requirepass string) string {
	t.Helper()
	lisAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	sh := shard.NewEngine(4, 16)
	t.Cleanup(sh.Close)
	eng := commands.NewEngine(sh, "differential", 0)
	if requirepass != "" {
		eng.SetRequirePass(requirepass)
	}
	// M8: the scripting manager (EVAL/SCRIPT) with a generous hard
	// deadline — KILL/BUSY scripts must behave like Redis's (killable by
	// SCRIPT KILL long before the watchdog would fire).
	scr, err := scripting.New(scripting.Config{
		LuaTimeLimitMs: 5000, HardDeadlineMs: 30000, MaxMemoryMB: 64,
		CompatVersion: commands.CompatVersion, RunID: eng.RunID,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("scripting manager: %v", err)
	}
	t.Cleanup(func() { _ = scr.Close() })
	eng.Scripts = scr
	eng.SetScriptMaxMemoryMB(64)
	// M5c: persistence manager on a per-test temp dir, so SAVE/BGSAVE/
	// CONFIG-persist scripts run against real persistence wiring (AOF off
	// by default, as Redis).
	pm := persist.NewManager(persist.Config{
		Dir:           t.TempDir(),
		DbFilename:    "dump.rdb",
		AppendDirname: "appendonlydir",
		AppendFsync:   "everysec",
		// Save "": the harness's redis-server runs with --save ''.
	}, sh, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := pm.Start(eng); err != nil {
		t.Fatalf("persist manager start: %v", err)
	}
	t.Cleanup(pm.Close) // registered after sh.Close → runs first
	srv := respserver.New(lisAddr, eng)
	ready := make(chan error, 1)
	go func() {
		ln, err := net.Listen("tcp", lisAddr)
		if err != nil {
			ready <- err
			return
		}
		ready <- nil
		_ = srv.Serve(ln)
	}()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return lisAddr
}

// --- scripted comparison ----------------------------------------------------------
//
// Step constructors (each step runs identically against Ultima and Redis):
//
//	cmd(args...)            — full request/reply on connection 0
//	cmdM(m, args...)        — same, with matcher m
//	cmdOn(conn, args...)    — full request/reply on connection `conn`
//	cmdOnM(m, conn, args...)— same, with matcher m
//	sendOn(conn, args...)   — send on connection `conn`, read nothing
//	recvOn(conn, m)         — read one reply frame on connection `conn`
//	                          (e.g. a parked BLPOP's late reply after sendOn)
//	expectPush(conn, m)     — read one async push frame on connection `conn`
//	                          (a pub/sub message that is not a command reply)
//
// Connection 0 is the default per-script connection (today's behavior).
// Higher connection indexes are dialed lazily on BOTH servers when first
// referenced and AUTHed when the suite runs with a password. RECONNECT
// closes and re-dials every open connection on both sides. recvOn and
// expectPush read with a 5s deadline so a missing frame fails the step
// instead of hanging. Push frames (`>`) compare like arrays: eqValue folds
// RESP3 push/map/set kinds to '*'.
//
// matcher selects how a step's two replies are compared.
type matcher int

const (
	mEq           matcher = iota // decoded values must be identical (errors: strings)
	mTTL                         // both ints, > 0, differ by at most 1
	mInfo                        // both bulk; compare the set of "# Section" headers
	mHello                       // both maps/flat-arrays; compare server/mode/role keys
	mAnyInt                      // both ints (values may differ, e.g. CLIENT ID)
	mConfigPairs                 // both maps/flat arrays; compare as unordered key sets + values per key
	mScanAll                     // SCAN full-iteration: [cursor, keys] where key SETS are compared
	mCount                       // COMMAND COUNT: both ints, both > 0
	mCommandInfo                 // COMMAND INFO: names/nullness per slot (7.2 returns 10-element entries; we return the classic 6)
	mAnyArr                      // both non-empty arrays (bare COMMAND full table)
	mSetCmp                      // aggregate (array/set) compared as an unordered set of elements
	mPairCmp                     // flat or nested field/value pairs compared as an unordered map
	mScanAllPairs                // HSCAN/ZSCAN full-iteration: [cursor, pairs] with pair SETS compared
)

// stepOp selects what a step does on its connection.
type stepOp int

const (
	opDo   stepOp = iota // send args, read one reply, compare
	opSend               // send args, read nothing
	opRecv               // read one reply frame (after opSend), compare
	opPush               // read one async push frame, compare
)

type step struct {
	args []string
	m    matcher
	on   int
	op   stepOp
}

func cmd(args ...string) step { return step{args: args, m: mEq} }
func cmdM(m matcher, args ...string) step {
	return step{args: args, m: m}
}

// The conn/op constructors below are the scripting API for the M3
// pub/sub and blocking differential scripts.
func cmdOn(conn int, args ...string) step { return step{args: args, m: mEq, on: conn} }

func cmdOnM(m matcher, conn int, args ...string) step {
	return step{args: args, m: m, on: conn}
}

func sendOn(conn int, args ...string) step { return step{args: args, on: conn, op: opSend} }

func recvOn(conn int, m matcher) step { return step{m: m, on: conn, op: opRecv} }

func expectPush(conn int, m matcher) step { return step{m: m, on: conn, op: opPush} }

// label renders the step for mismatch reports.
func (st step) label() string {
	switch st.op {
	case opSend:
		return fmt.Sprintf("sendOn(%d) %s", st.on, strings.Join(st.args, " "))
	case opRecv:
		return fmt.Sprintf("recvOn(%d)", st.on)
	case opPush:
		return fmt.Sprintf("expectPush(%d)", st.on)
	}
	if st.on != 0 {
		return fmt.Sprintf("conn%d %s", st.on, strings.Join(st.args, " "))
	}
	return strings.Join(st.args, " ")
}

// script is a named sequence; both servers are FLUSHALLed first, and the
// runner fails at the first mismatch.
type script struct {
	name  string
	steps []step
}

func runScripts(t *testing.T, scripts []script, requirepass string) {
	t.Helper()
	uAddr := startUltima(t, requirepass)
	rAddr := startRedis(t, requirepass)
	total, failed := 0, 0
	for _, sc := range scripts {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			uConns := map[int]*rconn{0: dial(t, uAddr)}
			rConns := map[int]*rconn{0: dial(t, rAddr)}
			// conn lazily dials connection n on one side; new connections
			// are AUTHed when the suite runs with a password.
			conn := func(conns map[int]*rconn, addr string, n int) *rconn {
				c, ok := conns[n]
				if !ok {
					c = dial(t, addr)
					if requirepass != "" {
						if v := c.do("AUTH", "default", requirepass); v.Kind != '+' {
							t.Fatalf("conn %d AUTH failed: %v", n, v)
						}
					}
					conns[n] = c
				}
				return c
			}
			both := func(n int) (*rconn, *rconn) {
				return conn(uConns, uAddr, n), conn(rConns, rAddr, n)
			}
			if requirepass != "" {
				for _, c := range []*rconn{uConns[0], rConns[0]} {
					if v := c.do("AUTH", "default", requirepass); v.Kind != '+' {
						t.Fatalf("AUTH failed: %v", v)
					}
				}
			}
			for _, c := range []*rconn{uConns[0], rConns[0]} {
				if v := c.do("FLUSHALL"); v.Kind != '+' {
					t.Fatalf("FLUSHALL failed: %v", v)
				}
			}
			for i, st := range sc.steps {
				u, r := uConns[0], rConns[0]
				if st.op == opDo && st.on == 0 {
					switch st.args[0] {
					case "SLEEP": // harness pseudo-command
						ms, _ := strconv.Atoi(st.args[1])
						time.Sleep(time.Duration(ms) * time.Millisecond)
						continue
					case "RECONNECT": // fresh connections on both sides (no re-AUTH)
						for _, c := range uConns {
							_ = c.conn.Close()
						}
						for _, c := range rConns {
							_ = c.conn.Close()
						}
						uConns = map[int]*rconn{0: dial(t, uAddr)}
						rConns = map[int]*rconn{0: dial(t, rAddr)}
						continue
					case "SCANLOOP": // full incremental SCAN on both; compare key sets
						total++
						if err := scanLoopCompare(u, r); err != nil {
							failed++
							t.Errorf("step %d SCANLOOP: %v", i, err)
						}
						continue
					case "HSCANLOOP", "SSCANLOOP", "ZSCANLOOP": // full incremental collection scan
						total++
						cmdName := st.args[0][:len(st.args[0])-len("LOOP")]
						if err := collScanCompare(u, r, cmdName, st.args[1:]); err != nil {
							failed++
							t.Errorf("step %d %s: %v", i, st.args[0], err)
						}
						continue
					}
				}
				switch st.op {
				case opSend:
					uc, rc := both(st.on)
					if err := uc.send(st.args...); err != nil {
						t.Fatalf("step %d %s: ultima send: %v", i, st.label(), err)
					}
					if err := rc.send(st.args...); err != nil {
						t.Fatalf("step %d %s: redis send: %v", i, st.label(), err)
					}
					continue
				case opRecv, opPush:
					uc, rc := both(st.on)
					uv, uerr := uc.recvTimeout(5 * time.Second)
					rv, rerr := rc.recvTimeout(5 * time.Second)
					total++
					if uerr != nil || rerr != nil {
						failed++
						t.Errorf("step %d %s: read timeout/error (ultima: %v, redis: %v)",
							i, st.label(), uerr, rerr)
						continue
					}
					if err := compare(st.m, uv, rv); err != nil {
						failed++
						t.Errorf("step %d %s: %v\n  ultima: %s\n  redis:  %s",
							i, st.label(), err, uv, rv)
					}
					continue
				}
				uc, rc := both(st.on)
				uv := uc.do(st.args...)
				rv := rc.do(st.args...)
				total++
				if err := compare(st.m, uv, rv); err != nil {
					failed++
					t.Errorf("step %d %s: %v\n  ultima: %s\n  redis:  %s",
						i, st.label(), err, uv, rv)
				}
			}
		})
	}
	t.Logf("differential: %d comparisons, %d mismatches", total, failed)
}

func compare(m matcher, u, r Value) error {
	switch m {
	case mTTL:
		if u.Kind != ':' || r.Kind != ':' {
			return fmt.Errorf("want both ints, got %c and %c", u.Kind, r.Kind)
		}
		if u.Int <= 0 || r.Int <= 0 {
			return fmt.Errorf("want positive TTLs, got %d and %d", u.Int, r.Int)
		}
		if d := u.Int - r.Int; d < -1 || d > 1 {
			return fmt.Errorf("TTLs differ by %d", d)
		}
		return nil
	case mInfo:
		if u.Kind != '$' || r.Kind != '$' {
			return fmt.Errorf("want both bulk, got %c and %c", u.Kind, r.Kind)
		}
		us, rs := infoSections(u.Str), infoSections(r.Str)
		if strings.Join(us, ",") != strings.Join(rs, ",") {
			return fmt.Errorf("INFO sections %v vs %v", us, rs)
		}
		return nil
	case mHello:
		uk, rk := helloKeys(u), helloKeys(r)
		for _, k := range []string{"server", "mode", "role", "proto"} {
			if uk[k] != rk[k] {
				return fmt.Errorf("HELLO key %q: %q vs %q", k, uk[k], rk[k])
			}
		}
		return nil
	case mAnyInt, mCount:
		if u.Kind != ':' || r.Kind != ':' {
			return fmt.Errorf("want both ints, got %c and %c", u.Kind, r.Kind)
		}
		if m == mCount && (u.Int <= 0 || r.Int <= 0) {
			return fmt.Errorf("counts %d and %d", u.Int, r.Int)
		}
		return nil
	case mConfigPairs:
		uk, err1 := pairMap(u)
		rk, err2 := pairMap(r)
		if err1 != nil || err2 != nil {
			return fmt.Errorf("pair decode: %v / %v", err1, err2)
		}
		if len(uk) != len(rk) {
			return fmt.Errorf("config keys %v vs %v", keysOf(uk), keysOf(rk))
		}
		for k, uv := range uk {
			rv, ok := rk[k]
			if !ok {
				return fmt.Errorf("config key %q missing on redis side", k)
			}
			if err := compare(mEq, uv, rv); err != nil {
				return fmt.Errorf("config key %q: %v", k, err)
			}
		}
		return nil
	case mScanAll:
		// [cursor, keys]: cursor must be "0" on both; keys compared as sets
		if len(u.Vals) != 2 || len(r.Vals) != 2 {
			return fmt.Errorf("malformed SCAN replies")
		}
		if u.Vals[0].Str != "0" || r.Vals[0].Str != "0" {
			return fmt.Errorf("SCAN cursors %q vs %q", u.Vals[0].Str, r.Vals[0].Str)
		}
		us, rs := keySet(u.Vals[1]), keySet(r.Vals[1])
		for k := range rs {
			if !us[k] {
				return fmt.Errorf("SCAN: redis has %q, ultima does not", k)
			}
		}
		for k := range us {
			if !rs[k] {
				return fmt.Errorf("SCAN: ultima has %q, redis does not", k)
			}
		}
		return nil
	case mAnyArr:
		if u.Kind != '*' || r.Kind != '*' || len(u.Vals) == 0 || len(r.Vals) == 0 {
			return fmt.Errorf("want both non-empty arrays, got %c[%d] and %c[%d]",
				u.Kind, len(u.Vals), r.Kind, len(r.Vals))
		}
		return nil
	case mSetCmp:
		us, rs := elemMultiset(u), elemMultiset(r)
		return cmpStringSets(us, rs, "SMEMBERS-like")
	case mPairCmp:
		up, err1 := canonPairs(u)
		rp, err2 := canonPairs(r)
		if err1 != nil || err2 != nil {
			return fmt.Errorf("pair decode: %v / %v", err1, err2)
		}
		if len(up) != len(rp) {
			return fmt.Errorf("pair counts differ: %v vs %v", sortedKeys(up), sortedKeys(rp))
		}
		for k, uv := range up {
			rv, ok := rp[k]
			if !ok {
				return fmt.Errorf("pair %q missing on redis side", k)
			}
			if uv != rv {
				return fmt.Errorf("pair %q: %q vs %q", k, uv, rv)
			}
		}
		return nil
	case mScanAllPairs:
		// [cursor, flatpairs]: cursor must be "0" on both; pairs unordered
		if len(u.Vals) != 2 || len(r.Vals) != 2 {
			return fmt.Errorf("malformed *SCAN replies")
		}
		if u.Vals[0].Str != "0" || r.Vals[0].Str != "0" {
			return fmt.Errorf("SCAN cursors %q vs %q", u.Vals[0].Str, r.Vals[0].Str)
		}
		up, err1 := canonPairs(u.Vals[1])
		rp, err2 := canonPairs(r.Vals[1])
		if err1 != nil || err2 != nil {
			return fmt.Errorf("pair decode: %v / %v", err1, err2)
		}
		if len(up) != len(rp) {
			return fmt.Errorf("pair counts differ: %v vs %v", sortedKeys(up), sortedKeys(rp))
		}
		for k, uv := range up {
			if rv, ok := rp[k]; !ok || rv != uv {
				return fmt.Errorf("pair %q: %q vs %q", k, uv, rp[k])
			}
		}
		return nil
	case mCommandInfo:
		if len(u.Vals) != len(r.Vals) {
			return fmt.Errorf("COMMAND INFO slot counts differ: %d vs %d", len(u.Vals), len(r.Vals))
		}
		for i := range u.Vals {
			uv, rv := u.Vals[i], r.Vals[i]
			if uv.Null != rv.Null {
				return fmt.Errorf("slot %d: nullness differs", i)
			}
			if uv.Null {
				continue
			}
			if len(uv.Vals) == 0 || len(rv.Vals) == 0 || uv.Vals[0].Str != rv.Vals[0].Str {
				return fmt.Errorf("slot %d: command names differ", i)
			}
			if len(uv.Vals) > 1 && len(rv.Vals) > 1 && uv.Vals[1].Int != rv.Vals[1].Int {
				return fmt.Errorf("slot %d (%s): arity differs: %d vs %d",
					i, uv.Vals[0].Str, uv.Vals[1].Int, rv.Vals[1].Int)
			}
		}
		return nil
	}
	// mEq
	return eqValue(u, r)
}

// scanLoopCompare walks SCAN to exhaustion on both servers (COUNT 10) and
// compares the accumulated key sets.
func scanLoopCompare(u, r *rconn) error {
	walk := func(c *rconn) map[string]bool {
		got := map[string]bool{}
		cursor := "0"
		for range 10000 {
			v := c.do("SCAN", cursor, "COUNT", "10")
			if len(v.Vals) != 2 {
				return nil
			}
			for _, k := range v.Vals[1].Vals {
				got[k.Str] = true
			}
			cursor = v.Vals[0].Str
			if cursor == "0" {
				return got
			}
		}
		return got
	}
	us, rs := walk(u), walk(r)
	for k := range rs {
		if !us[k] {
			return fmt.Errorf("redis has key %q, ultima does not", k)
		}
	}
	for k := range us {
		if !rs[k] {
			return fmt.Errorf("ultima has key %q, redis does not", k)
		}
	}
	return nil
}

func eqValue(u, r Value) error {
	// RESP2/RESP3 cosmetic differences that mean the same thing:
	kind := func(v Value) byte {
		if v.Null {
			return '_'
		}
		switch v.Kind {
		case '%', '~', '>': // map/set/push compared as flat sequences
			return '*'
		}
		return v.Kind
	}
	if kind(u) != kind(r) {
		return fmt.Errorf("reply kinds differ: %s vs %s", u, r)
	}
	switch kind(u) {
	case '_':
		return nil
	case '+', '-', '$', '(':
		if u.Str != r.Str {
			return fmt.Errorf("strings differ: %q vs %q", u.Str, r.Str)
		}
	case ':':
		if u.Int != r.Int {
			return fmt.Errorf("ints differ: %d vs %d", u.Int, r.Int)
		}
	case ',':
		if u.Dbl != r.Dbl {
			return fmt.Errorf("doubles differ: %q vs %q", u.Dbl, r.Dbl)
		}
	case '#':
		if u.Str != r.Str {
			return fmt.Errorf("bools differ: %q vs %q", u.Str, r.Str)
		}
	case '*':
		if len(u.Vals) != len(r.Vals) {
			return fmt.Errorf("array lengths differ: %d vs %d", len(u.Vals), len(r.Vals))
		}
		for i := range u.Vals {
			if err := eqValue(u.Vals[i], r.Vals[i]); err != nil {
				return fmt.Errorf("element %d: %v", i, err)
			}
		}
	}
	return nil
}

func infoSections(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "# ") {
			out = append(out, line[2:])
		}
	}
	return out
}

func helloKeys(v Value) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(v.Vals); i += 2 {
		m[v.Vals[i].Str] = v.Vals[i+1].Str
	}
	return m
}

func pairMap(v Value) (map[string]Value, error) {
	if v.Kind != '%' && v.Kind != '*' {
		return nil, fmt.Errorf("not a map/array: %c", v.Kind)
	}
	m := map[string]Value{}
	for i := 0; i+1 < len(v.Vals); i += 2 {
		m[v.Vals[i].Str] = v.Vals[i+1]
	}
	return m, nil
}

func keysOf(m map[string]Value) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keySet(v Value) map[string]bool {
	m := map[string]bool{}
	for _, e := range v.Vals {
		m[e.Str] = true
	}
	return m
}

// canon renders a scalar (or nested) Value canonically for unordered
// comparison.
func canon(v Value) string {
	switch {
	case v.Null:
		return "_"
	case v.Kind == ',':
		return "dbl:" + v.Dbl
	case v.Kind == ':':
		return strconv.FormatInt(v.Int, 10)
	case v.Kind == '*' || v.Kind == '%' || v.Kind == '~' || v.Kind == '>':
		parts := make([]string, len(v.Vals))
		for i, e := range v.Vals {
			parts[i] = canon(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	default:
		return fmt.Sprintf("%c:%q", v.Kind, v.Str)
	}
}

// elemMultiset renders each element of an aggregate reply canonically
// into a multiset.
func elemMultiset(v Value) map[string]int {
	m := map[string]int{}
	for _, e := range v.Vals {
		m[canon(e)]++
	}
	return m
}

func cmpStringSets(u, r map[string]int, what string) error {
	if len(u) != len(r) {
		return fmt.Errorf("%s: size %d vs %d", what, len(u), len(r))
	}
	for k, n := range r {
		if u[k] != n {
			return fmt.Errorf("%s: element %s count %d vs %d", what, k, u[k], n)
		}
	}
	return nil
}

// canonPairs normalizes a pairs reply into an unordered map: RESP3 maps
// and flat RESP2 arrays pair consecutive elements; an array whose
// elements are all 2-element arrays is pairs-of-pairs (RESP3 WITHVALUES/
// WITHSCORES forms).
func canonPairs(v Value) (map[string]string, error) {
	if v.Kind != '%' && v.Kind != '*' && v.Kind != '~' {
		return nil, fmt.Errorf("not an aggregate: %c", v.Kind)
	}
	m := map[string]string{}
	nested := len(v.Vals) > 0
	for _, e := range v.Vals {
		if e.Kind != '*' || len(e.Vals) != 2 {
			nested = false
			break
		}
	}
	if nested {
		for _, e := range v.Vals {
			m[canon(e.Vals[0])] = canon(e.Vals[1])
		}
		return m, nil
	}
	if len(v.Vals)%2 != 0 {
		return nil, fmt.Errorf("odd element count %d", len(v.Vals))
	}
	for i := 0; i+1 < len(v.Vals); i += 2 {
		m[canon(v.Vals[i])] = canon(v.Vals[i+1])
	}
	return m, nil
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// collScanCompare fully iterates HSCAN/SSCAN/ZSCAN on both servers with
// COUNT 10 and compares the accumulated element (or pair) sets.
func collScanCompare(u, r *rconn, cmdName string, args []string) error {
	walk := func(c *rconn) (map[string]int, error) {
		got := map[string]int{}
		cursor := "0"
		for range 10000 {
			call := append([]string{cmdName}, args...)
			call = append(call, cursor, "COUNT", "10")
			v := c.do(call...)
			if len(v.Vals) != 2 {
				return nil, fmt.Errorf("malformed %s reply: %v", cmdName, v)
			}
			for _, e := range v.Vals[1].Vals {
				got[canon(e)]++
			}
			cursor = v.Vals[0].Str
			if cursor == "0" {
				return got, nil
			}
		}
		return nil, fmt.Errorf("%s did not terminate", cmdName)
	}
	us, err := walk(u)
	if err != nil {
		return err
	}
	rs, err := walk(r)
	if err != nil {
		return err
	}
	return cmpStringSets(us, rs, cmdName)
}
