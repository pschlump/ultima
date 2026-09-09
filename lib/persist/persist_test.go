package persist

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// Goroutine-leak guard: shard owner goroutines, the manager ticker and
// BGSAVE/BGREWRITEAOF workers must all be gone when the tests end.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// --- helpers -------------------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newCmdEngine builds a shard engine + command engine pair; the shard
// engine is closed via t.Cleanup.
func newCmdEngine(t *testing.T) (*shard.Engine, *commands.Engine) {
	t.Helper()
	sh := shard.NewEngine(4, 16)
	t.Cleanup(sh.Close)
	return sh, commands.NewEngine(sh, "test", 0)
}

func testConnState(e *commands.Engine, db int) *commands.ConnState {
	cs := e.NewConnState("127.0.0.1:1")
	cs.Authed = true
	cs.DB = db
	return cs
}

// exec runs one command on a fresh connection state and fails the test on
// an error reply.
func exec(t *testing.T, e *commands.Engine, db int, args ...string) resp.Value {
	t.Helper()
	bb := make([][]byte, len(args))
	for i, a := range args {
		bb[i] = []byte(a)
	}
	v := e.Execute(testConnState(e, db), bb)
	if v.Kind == resp.KindError {
		t.Fatalf("%s: error reply: %s", args[0], v.Str)
	}
	return v
}

// keyDigest renders one key's observable state (type, TTL presence and
// full contents, hex-encoded so binary-unsafe data is comparable) as a
// canonical string, driving the engine through commands.Engine.Execute.
func keyDigest(t *testing.T, e *commands.Engine, db int, key string) string {
	t.Helper()
	typ := exec(t, e, db, "TYPE", key)
	var sb strings.Builder
	sb.WriteString("type=" + typ.Str)
	if pttl := exec(t, e, db, "PTTL", key); pttl.Int > 0 {
		sb.WriteString(" ttl=1")
	} else {
		sb.WriteString(" ttl=0")
	}
	blobs := func(v resp.Value) []string {
		var out []string
		for _, it := range v.Arr {
			out = append(out, hex.EncodeToString(it.Blob))
		}
		return out
	}
	switch typ.Str {
	case "none":
		// nothing more
	case "string":
		v := exec(t, e, db, "GET", key)
		sb.WriteString(" val=" + hex.EncodeToString(v.Blob))
	case "list":
		v := exec(t, e, db, "LRANGE", key, "0", "-1")
		sb.WriteString(" val=" + strings.Join(blobs(v), ","))
	case "set":
		v := exec(t, e, db, "SMEMBERS", key)
		ms := blobs(v)
		sort.Strings(ms)
		sb.WriteString(" val=" + strings.Join(ms, ","))
	case "zset":
		v := exec(t, e, db, "ZRANGE", key, "0", "-1", "WITHSCORES")
		var pairs []string
		for i := 0; i+1 < len(v.Arr); i += 2 {
			pairs = append(pairs, hex.EncodeToString(v.Arr[i].Blob)+"="+string(v.Arr[i+1].Blob))
		}
		sb.WriteString(" val=" + strings.Join(pairs, ","))
	case "hash":
		v := exec(t, e, db, "HGETALL", key)
		var pairs []string
		for i := 0; i+1 < len(v.Arr); i += 2 {
			pairs = append(pairs, hex.EncodeToString(v.Arr[i].Blob)+"="+hex.EncodeToString(v.Arr[i+1].Blob))
		}
		sort.Strings(pairs)
		sb.WriteString(" val=" + strings.Join(pairs, ","))
	default:
		t.Fatalf("unexpected TYPE reply %q for key %q", typ.Str, key)
	}
	return sb.String()
}

// digestKeyspace digests every (db, key) pair, keyed "db/key".
func digestKeyspace(t *testing.T, e *commands.Engine, dbs map[int][]string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for db, keys := range dbs {
		for _, k := range keys {
			out[strconv.Itoa(db)+"/"+k] = keyDigest(t, e, db, k)
		}
	}
	return out
}

func compareKeyspaces(t *testing.T, nameA string, eA *commands.Engine, nameB string, eB *commands.Engine, dbs map[int][]string) {
	t.Helper()
	da := digestKeyspace(t, eA, dbs)
	dbb := digestKeyspace(t, eB, dbs)
	for k, va := range da {
		if vb, ok := dbb[k]; !ok || vb != va {
			t.Errorf("key %q: %s digest %q != %s digest %q", k, nameA, va, nameB, vb)
		}
	}
}

// --- 1. entry codec round-trip ---------------------------------------------

func listEntry(vals ...[]byte) *shard.Entry {
	l := types.NewList()
	for _, v := range vals {
		l.PushTail(v)
	}
	return &shard.Entry{Type: shard.TypeList, Obj: l}
}

func setEntry(members ...string) *shard.Entry {
	s := types.NewSet()
	for _, m := range members {
		s.Add(m)
	}
	return &shard.Entry{Type: shard.TypeSet, Obj: s}
}

func zsetEntry(pairs ...any) *shard.Entry {
	z := types.NewZSet()
	for i := 0; i+1 < len(pairs); i += 2 {
		z.Add(pairs[i].(string), pairs[i+1].(float64))
	}
	return &shard.Entry{Type: shard.TypeZSet, Obj: z}
}

func hashEntry(pairs ...string) *shard.Entry {
	h := types.NewHash()
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set(pairs[i], pairs[i+1])
	}
	return &shard.Entry{Type: shard.TypeHash, Obj: h}
}

func entriesEqual(a, b *shard.Entry) bool {
	if a == nil || b == nil || a.Type != b.Type || a.ExpireAtMs != b.ExpireAtMs {
		return false
	}
	switch a.Type {
	case shard.TypeString:
		return bytes.Equal(a.Str, b.Str)
	case shard.TypeList:
		la, lb := a.Obj.(*types.List), b.Obj.(*types.List)
		if la.Len() != lb.Len() {
			return false
		}
		var av, bv [][]byte
		for _, v := range la.All() {
			av = append(av, v)
		}
		for _, v := range lb.All() {
			bv = append(bv, v)
		}
		for i := range av {
			if !bytes.Equal(av[i], bv[i]) {
				return false
			}
		}
		return true
	case shard.TypeSet:
		ma, mb := a.Obj.(*types.Set).Members(), b.Obj.(*types.Set).Members()
		sort.Strings(ma)
		sort.Strings(mb)
		if len(ma) != len(mb) {
			return false
		}
		for i := range ma {
			if ma[i] != mb[i] {
				return false
			}
		}
		return true
	case shard.TypeZSet:
		za, zb := a.Obj.(*types.ZSet), b.Obj.(*types.ZSet)
		if za.Len() != zb.Len() {
			return false
		}
		ok := true
		za.Each(func(member string, score float64) {
			s, found := zb.Score(member)
			if !found || s != score {
				ok = false
			}
		})
		return ok
	case shard.TypeHash:
		ha, hb := a.Obj.(*types.Hash), b.Obj.(*types.Hash)
		if ha.Len() != hb.Len() {
			return false
		}
		mb := map[string]string{}
		hb.Each(func(f, v string) { mb[f] = v })
		ok := true
		ha.Each(func(f, v string) {
			if w, found := mb[f]; !found || w != v {
				ok = false
			}
		})
		return ok
	}
	return false
}

func TestEntryCodecRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		key  string
		e    *shard.Entry
	}{
		{"string", "k1", &shard.Entry{Type: shard.TypeString, Str: []byte("hello")}},
		{"empty-string", "k2", &shard.Entry{Type: shard.TypeString, Str: []byte{}}},
		{"binary-unsafe", "k\x00\xff",
			&shard.Entry{Type: shard.TypeString, Str: []byte{0x00, 0x01, 0xfe, 0xff, '\r', '\n'}}},
		{"string-with-ttl", "k3",
			&shard.Entry{Type: shard.TypeString, Str: []byte("v"), ExpireAtMs: 1_800_000_000_000}},
		{"list", "l1", listEntry([]byte("a"), []byte("b"), []byte{}, []byte{0x00, 0xff})},
		{"empty-list", "l2", listEntry()},
		{"set", "s1", setEntry("m1", "m2", "m\x00\xff")},
		{"zset", "z1", zsetEntry("neg", -1.5, "frac", 2.25, "zero", 0.0, "big", 1e17)},
		{"hash", "h1", hashEntry("f1", "v1", "emptyval", "", "", "emptyfield", "bin", "\x01\xfe")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := encodeEntry(nil, tc.key, tc.e)
			d := &decoder{b: buf}
			key, got := decodeEntry(d)
			if d.err != nil {
				t.Fatalf("decodeEntry: %v", d.err)
			}
			if key != tc.key {
				t.Errorf("key = %q, want %q", key, tc.key)
			}
			if !entriesEqual(tc.e, got) {
				t.Errorf("entry mismatch after round-trip")
			}
			if d.off != len(buf) {
				t.Errorf("decoder consumed %d of %d bytes", d.off, len(buf))
			}
		})
	}
}

func TestDecodeEntryBadTag(t *testing.T) {
	buf := encodeEntry(nil, "k", &shard.Entry{Type: shard.TypeString, Str: []byte("v")})
	// Tag sits after keyLen(4) + key(1) + expireAt(8).
	buf[4+1+8] = 99
	d := &decoder{b: buf}
	_, e := decodeEntry(d)
	if !errors.Is(d.err, errBadTag) {
		t.Errorf("err = %v, want errBadTag", d.err)
	}
	if e != nil {
		t.Errorf("expected nil entry on bad tag, got %+v", e)
	}
}

func TestDecodeEntryTruncated(t *testing.T) {
	full := encodeEntry(nil, "key", &shard.Entry{Type: shard.TypeString, Str: []byte("value")})
	for _, n := range []int{0, 3, 8, len(full) - 1} {
		d := &decoder{b: full[:n]}
		decodeEntry(d)
		if !errors.Is(d.err, errShort) {
			t.Errorf("truncated to %d bytes: err = %v, want errShort", n, d.err)
		}
	}
}

// --- 2. snapshot round-trip -------------------------------------------------

// populateSnapshotData loads a spread of keys across dbs 0 and 2.
// Returns the digest map of the keys that must survive a round-trip.
func populateSnapshotData(t *testing.T, e *commands.Engine) map[int][]string {
	t.Helper()
	nowMs := time.Now().UnixMilli()

	exec(t, e, 0, "SET", "s1", "hello")
	exec(t, e, 0, "SET", "s2", "")
	exec(t, e, 0, "SET", "bin\x00\xff", "\x01v\xfe")
	exec(t, e, 0, "SET", "ttl1", "v")
	exec(t, e, 0, "PEXPIREAT", "ttl1", strconv.FormatInt(nowMs+3_600_000, 10))
	exec(t, e, 0, "HSET", "h1", "f1", "v1", "f2", "", "binf", "\x02")
	exec(t, e, 0, "RPUSH", "l1", "a", "b", "c")
	exec(t, e, 0, "SADD", "set1", "m1", "m2", "m3")
	exec(t, e, 0, "ZADD", "z1", "-1.5", "neg", "2.25", "frac", "100", "big")
	exec(t, e, 2, "SET", "other", "x")

	// Deleted inline by PEXPIREAT-in-the-past; must not be restored.
	exec(t, e, 0, "SET", "gone", "v")
	exec(t, e, 0, "PEXPIREAT", "gone", strconv.FormatInt(nowMs-1000, 10))

	// Alive at snapshot time, expired before load: exercises the loader's
	// skip of entries that expired while the server was down.
	exec(t, e, 0, "SET", "soon", "v")
	exec(t, e, 0, "PEXPIREAT", "soon", strconv.FormatInt(nowMs+1000, 10))

	return map[int][]string{
		0: {"s1", "s2", "bin\x00\xff", "ttl1", "h1", "l1", "set1", "z1"},
		2: {"other"},
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	for _, compress := range []bool{false, true} {
		t.Run(fmt.Sprintf("compress=%v", compress), func(t *testing.T) {
			shA, eA := newCmdEngine(t)
			keys := populateSnapshotData(t, eA)

			// Sanity: both TTL keys are alive before the snapshot.
			if v := exec(t, eA, 0, "TYPE", "soon"); v.Str != "string" {
				t.Fatalf("soon: TYPE = %q before snapshot, want string", v.Str)
			}
			if v := exec(t, eA, 0, "TYPE", "gone"); v.Str != "none" {
				t.Fatalf("gone: TYPE = %q after past PEXPIREAT, want none", v.Str)
			}

			dir := t.TempDir()
			man, err := WriteSnapshot(shA, dir, "dump.ultima", compress)
			if err != nil {
				t.Fatalf("WriteSnapshot: %v", err)
			}
			if man.Shards != 4 || man.DBs != 16 {
				t.Errorf("manifest shards=%d dbs=%d, want 4/16", man.Shards, man.DBs)
			}
			if man.Compress != compress {
				t.Errorf("manifest compress = %v, want %v", man.Compress, compress)
			}
			if len(man.Segments) == 0 {
				t.Error("manifest has no segments")
			}
			if !SnapshotExists(dir, "dump.ultima") {
				t.Fatal("SnapshotExists = false after write")
			}
			// Let "soon" expire, then load into a fresh engine.
			time.Sleep(1200 * time.Millisecond)
			shB, eB := newCmdEngine(t)
			manB, err := LoadSnapshot(shB, dir, "dump.ultima")
			if err != nil {
				t.Fatalf("LoadSnapshot: %v", err)
			}
			if manB.Compress != compress {
				t.Errorf("loaded manifest compress = %v, want %v", manB.Compress, compress)
			}
			if len(manB.Segments) != len(man.Segments) {
				t.Errorf("loaded %d segments, wrote %d", len(manB.Segments), len(man.Segments))
			}

			compareKeyspaces(t, "original", eA, "restored", eB, keys)

			// The expired keys must not come back.
			for _, k := range []string{"gone", "soon"} {
				if v := exec(t, eB, 0, "TYPE", k); v.Str != "none" {
					t.Errorf("restored %s: TYPE = %q, want none", k, v.Str)
				}
			}
			// The far-future TTL survives with its absolute expiry intact.
			if v := exec(t, eB, 0, "PTTL", "ttl1"); v.Int <= 3_590_000 || v.Int > 3_600_000 {
				t.Errorf("restored PTTL ttl1 = %d, want in (3590000, 3600000]", v.Int)
			}
		})
	}
}

// --- 3. corrupt / truncated snapshot rejection --------------------------------

func writeSmallSnapshot(t *testing.T, dir string) {
	t.Helper()
	shA, eA := newCmdEngine(t)
	exec(t, eA, 0, "SET", "k1", "v1")
	exec(t, eA, 0, "RPUSH", "l1", "a", "b")
	if _, err := WriteSnapshot(shA, dir, "dump.ultima", false); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
}

func TestSnapshotCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	writeSmallSnapshot(t, dir)
	p := filepath.Join(dir, "dump.ultima")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF // flip one payload byte
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	shB := shard.NewEngine(4, 16)
	t.Cleanup(shB.Close)
	if _, err := LoadSnapshot(shB, dir, "dump.ultima"); !errors.Is(err, errBadCRC) {
		t.Errorf("LoadSnapshot of corrupted file: err = %v, want errBadCRC", err)
	}
}

func TestSnapshotTruncated(t *testing.T) {
	dir := t.TempDir()
	writeSmallSnapshot(t, dir)
	p := filepath.Join(dir, "dump.ultima")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw[:len(raw)-10], 0o644); err != nil {
		t.Fatal(err)
	}
	shB := shard.NewEngine(4, 16)
	t.Cleanup(shB.Close)
	if _, err := LoadSnapshot(shB, dir, "dump.ultima"); err == nil {
		t.Error("LoadSnapshot of truncated file: err = nil, want an error")
	}
}

func TestSnapshotBadMagic(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "dump.ultima"),
		[]byte("NOTULTIMA at all, really"), 0o644); err != nil {
		t.Fatal(err)
	}
	shB := shard.NewEngine(4, 16)
	t.Cleanup(shB.Close)
	if _, err := LoadSnapshot(shB, dir, "dump.ultima"); !errors.Is(err, errBadMagic) {
		t.Errorf("LoadSnapshot of non-snapshot: err = %v, want errBadMagic", err)
	}
}

// --- 4. AOF replay equivalence -------------------------------------------------

func aofTestConfig(dir string) Config {
	return Config{
		Dir:           dir,
		DbFilename:    "dump.ultima",
		AppendDirname: "appendonlydir",
		AppendOnly:    true,
		AppendFsync:   "no",
		Save:          "",
	}
}

// runAOFScript executes the mutation script against engine e and returns
// the keys (per db) that must survive an AOF replay.
func runAOFScript(t *testing.T, e *commands.Engine) map[int][]string {
	t.Helper()

	// Doomed keys: FLUSHDB must wipe these from the replayed state too.
	exec(t, e, 0, "SET", "junk", "j")
	exec(t, e, 0, "HSET", "doomed", "f", "v")
	exec(t, e, 0, "FLUSHDB")

	exec(t, e, 0, "SET", "k", "v", "PX", "600000")
	exec(t, e, 0, "SET", "kx", "vx")
	exec(t, e, 0, "EXPIRE", "kx", "600")
	exec(t, e, 0, "HSET", "h", "f1", "v1", "f2", "v2")
	exec(t, e, 0, "RPUSH", "l", "a", "b", "c")
	exec(t, e, 0, "SADD", "s", "m1", "m2", "m3")
	exec(t, e, 0, "SPOP", "s") // logged as SREM of the popped member
	exec(t, e, 0, "INCR", "n")

	// MULTI/EXEC on one connection: EXEC logs the inner commands.
	csTx := testConnState(e, 0)
	for _, args := range [][]string{{"MULTI"}, {"SET", "a", "1"}, {"EXEC"}} {
		bb := make([][]byte, len(args))
		for i, a := range args {
			bb[i] = []byte(a)
		}
		if v := e.Execute(csTx, bb); v.Kind == resp.KindError {
			t.Fatalf("%s in MULTI/EXEC: %s", args[0], v.Str)
		}
	}

	// SETEX-style: relative EX rewritten to absolute PXAT in the AOF.
	exec(t, e, 0, "SET", "k", "v2", "EX", "100")

	exec(t, e, 2, "SET", "d2key", "val")

	return map[int][]string{
		0: {"k", "kx", "h", "l", "s", "n", "a", "junk", "doomed"},
		2: {"d2key"},
	}
}

func TestAOFReplayEquivalence(t *testing.T) {
	dir := t.TempDir()

	shA, eA := newCmdEngine(t)
	mA := NewManager(aofTestConfig(dir), shA, testLogger())
	if err := mA.Start(eA); err != nil {
		t.Fatalf("Start A: %v", err)
	}
	keys := runAOFScript(t, eA)

	// Sanity on the scripted outcome.
	if v := exec(t, eA, 0, "SMEMBERS", "s"); len(v.Arr) != 2 {
		t.Fatalf("A: SCARD s via SMEMBERS = %d, want 2 (SPOP popped one)", len(v.Arr))
	}
	if v := exec(t, eA, 0, "TYPE", "junk"); v.Str != "none" {
		t.Fatalf("A: TYPE junk = %q, want none (FLUSHDB)", v.Str)
	}

	mA.Close()

	// Engine B: fresh shard engine, restore from the AOF via Manager.Start.
	shB, eB := newCmdEngine(t)
	mB := NewManager(aofTestConfig(dir), shB, testLogger())
	if err := mB.Start(eB); err != nil {
		t.Fatalf("Start B (replay): %v", err)
	}
	defer mB.Close()

	compareKeyspaces(t, "A", eA, "B", eB, keys)

	// The db-2 key came back, with its value.
	if v := exec(t, eB, 2, "GET", "d2key"); string(v.Blob) != "val" {
		t.Errorf("B: GET d2key@2 = %q, want val", v.Blob)
	}
	// k carries its TTL across the replay (SET ... EX → PXAT rewrite).
	if v := exec(t, eB, 0, "PTTL", "k"); v.Int <= 90_000 || v.Int > 100_000 {
		t.Errorf("B: PTTL k = %d, want in (90000, 100000]", v.Int)
	}
	// FLUSHDB replayed: the doomed keys are gone in B.
	for _, k := range []string{"junk", "doomed"} {
		if v := exec(t, eB, 0, "TYPE", k); v.Str != "none" {
			t.Errorf("B: TYPE %s = %q, want none", k, v.Str)
		}
	}
}

// --- 5. BGREWRITEAOF ---------------------------------------------------------

func TestBGRewriteAOF(t *testing.T) {
	dir := t.TempDir()

	shA, eA := newCmdEngine(t)
	mA := NewManager(aofTestConfig(dir), shA, testLogger())
	if err := mA.Start(eA); err != nil {
		t.Fatalf("Start A: %v", err)
	}
	keys := runAOFScript(t, eA)

	if err := mA.BGRewriteAOF(); err != nil {
		t.Fatalf("BGRewriteAOF: %v", err)
	}
	// Writes concurrent with the rewrite must survive (the rewrite
	// backlog) and must not be duplicated.
	exec(t, eA, 0, "INCR", "n")
	exec(t, eA, 0, "RPUSH", "l", "d")
	exec(t, eA, 0, "SET", "postrewrite", "p")
	keys[0] = append(keys[0], "postrewrite")

	// Wait for the rewrite to finish.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var sb strings.Builder
		mA.InfoPersistence(&sb)
		if !strings.Contains(sb.String(), "aof_rewrite_in_progress:1") {
			if !strings.Contains(sb.String(), "aof_last_bgrewrite_status:ok") {
				t.Fatalf("rewrite finished with bad status:\n%s", sb.String())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rewrite still in progress after 10s:\n%s", sb.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	mA.Close()

	shB, eB := newCmdEngine(t)
	mB := NewManager(aofTestConfig(dir), shB, testLogger())
	if err := mB.Start(eB); err != nil {
		t.Fatalf("Start B (replay rewritten AOF): %v", err)
	}
	defer mB.Close()

	compareKeyspaces(t, "A", eA, "B", eB, keys)
	// INCR ran twice in total (once before, once during the rewrite):
	// replay must land exactly on 2 — a duplicated backlog record would
	// leave 3, a lost one 1.
	if v := exec(t, eB, 0, "GET", "n"); string(v.Blob) != "2" {
		t.Errorf("B: GET n = %q, want 2", v.Blob)
	}
	if v := exec(t, eB, 0, "LLEN", "l"); v.Int != 4 {
		t.Errorf("B: LLEN l = %d, want 4", v.Int)
	}
}

// --- 6. torn-record tolerance -------------------------------------------------

func TestAOFTornRecord(t *testing.T) {
	dir := t.TempDir()
	ad := filepath.Join(dir, "appendonlydir")
	if err := os.MkdirAll(ad, 0o755); err != nil {
		t.Fatal(err)
	}
	good := "*6\r\n$9\r\nULTIMAREC\r\n$1\r\n0\r\n$1\r\n1\r\n" +
		"$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	torn := "*6\r\n$9\r\nULTIMAREC\r\n$1\r\n0\r\n$1\r\n2\r\n$3\r\nSET\r\n$1\r\nx\r\n$5\r\nhel" // crash mid-bulk
	if err := os.WriteFile(filepath.Join(ad, "shard-0.aof"), []byte(good+torn), 0o644); err != nil {
		t.Fatal(err)
	}

	type replayed struct {
		db   int
		argv []string
	}
	var got []replayed
	n, _, err := replayAOF(dir, "appendonlydir", func(db int, argv [][]byte) {
		var ss []string
		for _, a := range argv {
			ss = append(ss, string(a))
		}
		got = append(got, replayed{db: db, argv: ss})
	})
	if err != nil {
		t.Fatalf("replayAOF: %v", err)
	}
	if n != 1 {
		t.Fatalf("replayAOF replayed %d commands, want 1", n)
	}
	want := []string{"SET", "k", "v"}
	if got[0].db != 0 || strings.Join(got[0].argv, " ") != strings.Join(want, " ") {
		t.Errorf("replayed %+v, want db=0 argv=%v", got[0], want)
	}
}

// --- 7. Manager.Save under load -------------------------------------------------

func TestManagerSaveUnderLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Dir:           dir,
		DbFilename:    "dump.ultima",
		AppendDirname: "appendonlydir",
		AppendOnly:    false,
		AppendFsync:   "no",
		Save:          "",
	}

	shA, eA := newCmdEngine(t)
	mA := NewManager(cfg, shA, testLogger())
	if err := mA.Start(eA); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mA.Close()

	// Fixed keys that must appear in the snapshot.
	exec(t, eA, 0, "SET", "fixed1", "v1")
	exec(t, eA, 0, "HSET", "fixedh", "f", "v")
	exec(t, eA, 0, "RPUSH", "fixedl", "a", "b")
	exec(t, eA, 1, "SET", "fixeddb1", "x")
	keys := map[int][]string{0: {"fixed1", "fixedh", "fixedl"}, 1: {"fixeddb1"}}

	// Load: a writer hammering the engine while Save holds PauseAll.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		cs := testConnState(eA, 3)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			eA.Execute(cs, [][]byte{[]byte("SET"),
				[]byte(fmt.Sprintf("load%d", i%64)), []byte("v")})
		}
	}()

	if err := mA.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	close(stop)
	<-done

	if mA.LastSave() <= 0 {
		t.Errorf("LastSave = %d, want > 0", mA.LastSave())
	}
	for _, fn := range []string{"dump.ultima", "snapshot.manifest"} {
		if _, err := os.Stat(filepath.Join(dir, fn)); err != nil {
			t.Errorf("expected %s after Save: %v", fn, err)
		}
	}
	if !SnapshotExists(dir, "dump.ultima") {
		t.Error("SnapshotExists = false after Save")
	}

	shB, eB := newCmdEngine(t)
	if _, err := LoadSnapshot(shB, dir, "dump.ultima"); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	compareKeyspaces(t, "saved", eA, "restored", eB, keys)
}
