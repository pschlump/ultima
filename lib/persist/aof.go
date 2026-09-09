package persist

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// aofSet is the per-shard append-only-file collection (MP-AOF-like,
// §13.1): one RESP command stream per shard under dir/appenddirname/,
// plus a JSON manifest.
//
// Record format: one RESP array per record,
//
//	["ULTIMAREC", "<db>", "<seq>", cmd, args...]
//
// where seq comes from a set-wide monotonic counter allocated at capture
// time (i.e. in mutation-completion order, up to goroutine scheduling).
// The seq makes every record self-contained — there are no SELECT records
// — and gives replay a global order across the per-shard logs, which a
// file-at-a-time replay cannot have: a broadcast FLUSHDB sits in every
// log, and naive replay would re-execute it once per log, wiping keys
// restored from logs processed earlier. Broadcast copies share ONE seq;
// replay sorts by seq and executes each seq once.
//
// Concurrency: appends arrive on connection goroutines (command capture)
// and shard goroutines (synthesized DELs from expiry/eviction); each
// log's mutex serializes them. The fsync policy is the manager's,
// applied after the append returns.
type aofSet struct {
	dir  string // base dir; logs live in dir/appenddirname
	name string // appenddirname
	logs []*aofLog
	seq  atomic.Uint64 // record sequence; survives restarts via openAOF's base
}

type aofLog struct {
	mu sync.Mutex
	f  *os.File // nil after closeAll (shutdown); appends then drop
}

// aofRecVerb is the stamp prefix of every on-disk record.
const aofRecVerb = "ULTIMAREC"

// stampedRecord is one parsed AOF record ready for ordered replay.
type stampedRecord struct {
	seq  uint64
	db   int
	argv [][]byte
}

// aofManifest is the JSON sidecar listing the live per-shard logs.
type aofManifest struct {
	Version int      `json:"version"`
	Shards  int      `json:"shards"`
	Files   []string `json:"files"`
}

func (s *aofSet) aofDir() string { return filepath.Join(s.dir, s.name) }

func (s *aofSet) fileName(i int) string { return fmt.Sprintf("shard-%d.aof", i) }

// stamp renders one stamped on-disk record for (db, seq, argv).
func stampRecord(db int, seq uint64, argv [][]byte) [][]byte {
	rec := make([][]byte, 0, len(argv)+3)
	rec = append(rec, []byte(aofRecVerb),
		[]byte(strconv.Itoa(db)),
		[]byte(strconv.FormatUint(seq, 10)))
	return append(rec, argv...)
}

// openAOF creates (or opens for append) the per-shard logs under
// dir/appenddirname and writes the manifest. Existing logs are kept and
// appended to (startup after replay); baseSeq continues the record
// sequence above the highest seq already present, so records written
// after a restart sort after everything replayed.
func openAOF(dir, name string, shards int, baseSeq uint64) (*aofSet, error) {
	s := &aofSet{dir: dir, name: name, logs: make([]*aofLog, shards)}
	s.seq.Store(baseSeq)
	if err := os.MkdirAll(s.aofDir(), 0o755); err != nil {
		return nil, err
	}
	for i := range s.logs {
		f, err := os.OpenFile(filepath.Join(s.aofDir(), s.fileName(i)),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			s.closeAll()
			return nil, err
		}
		s.logs[i] = &aofLog{f: f}
	}
	if err := s.writeManifest(); err != nil {
		s.closeAll()
		return nil, err
	}
	return s, nil
}

func (s *aofSet) writeManifest() error {
	m := aofManifest{Version: 1, Shards: len(s.logs)}
	for i := range s.logs {
		m.Files = append(m.Files, s.fileName(i))
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.aofDir(), "aof.manifest"), b, 0o644)
}

// appendRecord stamps argv with a fresh sequence number and appends it to
// one shard's log.
func (s *aofSet) appendRecord(logIdx int, db int, argv [][]byte) error {
	return s.appendSeq(logIdx, db, s.seq.Add(1), argv)
}

// appendAll broadcasts one record to every shard log (FLUSHDB/FLUSHALL):
// every copy carries the SAME seq, so the merge at replay executes the
// command exactly once, at its global-order position.
func (s *aofSet) appendAll(db int, argv [][]byte) error {
	seq := s.seq.Add(1)
	for i := range s.logs {
		if err := s.appendSeq(i, db, seq, argv); err != nil {
			return err
		}
	}
	return nil
}

func (s *aofSet) appendSeq(logIdx int, db int, seq uint64, argv [][]byte) error {
	l := s.logs[logIdx]
	buf := appendAOFCommand(nil, stampRecord(db, seq, argv))
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil // closing down (Close/SetAppendOnly(false)): drop
	}
	_, err := l.f.Write(buf)
	return err
}

// appendAOFCommand renders argv as one RESP array of bulk strings — the
// on-disk AOF record (Redis writes the same shape for the command part).
func appendAOFCommand(dst []byte, argv [][]byte) []byte {
	dst = resp.AppendArray(dst, len(argv))
	for _, a := range argv {
		dst = resp.AppendBulk(dst, a)
	}
	return dst
}

// fsync flushes every log to disk (the everysec ticker's batch).
func (s *aofSet) fsync() {
	for _, l := range s.logs {
		l.mu.Lock()
		if l.f != nil {
			_ = l.f.Sync()
		}
		l.mu.Unlock()
	}
}

// fsyncOne flushes one log (the appendfsync=always path, after its write).
func (s *aofSet) fsyncOne(i int) {
	l := s.logs[i]
	l.mu.Lock()
	if l.f != nil {
		_ = l.f.Sync()
	}
	l.mu.Unlock()
}

// closeAll fsyncs and closes every log (shutdown / appendonly no).
// Appends after closeAll are dropped (appendSeq's nil check), which
// closes the fsync-vs-close race with the everysec ticker.
func (s *aofSet) closeAll() {
	for _, l := range s.logs {
		if l == nil {
			continue
		}
		l.mu.Lock()
		if l.f != nil {
			_ = l.f.Sync()
			_ = l.f.Close()
			l.f = nil
		}
		l.mu.Unlock()
	}
}

// rewrite replaces every shard log with a minimal command stream
// reproducing the current state (BGREWRITEAOF): SET/RPUSH/SADD/ZADD/HSET
// per key, plus PEXPIREAT for volatile keys, each stamped with a fresh
// seq from the set-wide counter.
//
// Crash-consistency scheme: the caller (Manager) holds shard.Engine.
// PauseAll for the whole dump+swap and has drained every write command
// sitting between its mutation and its AOF capture (commands.Engine.
// PersistQuiesced). With mutations frozen and captures drained, no
// append can interleave with the dump, so the dump alone is the new
// log — there is no rewrite backlog, and a record can never be both
// reflected in the dump and appended (which would duplicate
// non-idempotent effects like INCR/RPUSH on replay). This makes the
// rewrite stop-the-world — the documented divergence from Redis's
// fork-based rewrite (§13.1), traded for exactness without COW.
func (s *aofSet) rewrite(eng *shard.Engine, tok uint64) error {
	nowMs := nowUnixMs()
	for i, l := range s.logs {
		var out []byte
		for db := 0; db < eng.MaxDBs(); db++ {
			eng.DoTok(tok, i, func(sh *shard.Shard) {
				sh.DumpDB(db, nowMs, func(key string, e *shard.Entry) bool {
					for _, argv := range aofStateArgvs(key, e) {
						out = appendAOFCommand(out, stampRecord(db, s.seq.Add(1), argv))
					}
					return true
				})
			})
		}
		l.mu.Lock()
		tmp := filepath.Join(s.aofDir(), s.fileName(i)+".new")
		err := os.WriteFile(tmp, out, 0o644)
		if err == nil {
			err = syncDir(tmp)
		}
		if err == nil && l.f != nil {
			_ = l.f.Close()
		}
		if err == nil {
			err = os.Rename(tmp, filepath.Join(s.aofDir(), s.fileName(i)))
		}
		if err == nil {
			var f *os.File
			f, err = os.OpenFile(filepath.Join(s.aofDir(), s.fileName(i)),
				os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err == nil {
				l.f = f
			}
		}
		l.mu.Unlock()
		if err != nil {
			return fmt.Errorf("persist: rewriting %s: %w", s.fileName(i), err)
		}
	}
	return s.writeManifest()
}

// aofStateArgvs renders one live entry as its minimal replay commands:
// SET / RPUSH / SADD / ZADD / HSET, plus PEXPIREAT for a volatile key.
func aofStateArgvs(key string, e *shard.Entry) [][][]byte {
	kb := []byte(key)
	var cmd [][]byte
	switch e.Type {
	case shard.TypeString:
		cmd = [][]byte{[]byte("SET"), kb, e.Str}
	case shard.TypeList:
		l := e.Obj.(*types.List)
		cmd = [][]byte{[]byte("RPUSH"), kb}
		for _, v := range l.All() {
			cmd = append(cmd, v)
		}
	case shard.TypeSet:
		s := e.Obj.(*types.Set)
		cmd = [][]byte{[]byte("SADD"), kb}
		for _, m := range s.Members() {
			cmd = append(cmd, []byte(m))
		}
	case shard.TypeZSet:
		z := e.Obj.(*types.ZSet)
		cmd = [][]byte{[]byte("ZADD"), kb}
		z.Each(func(member string, score float64) {
			cmd = append(cmd, []byte(strconv.FormatFloat(score, 'g', -1, 64)), []byte(member))
		})
	case shard.TypeHash:
		h := e.Obj.(*types.Hash)
		cmd = [][]byte{[]byte("HSET"), kb}
		h.Each(func(field, value string) {
			cmd = append(cmd, []byte(field), []byte(value))
		})
	}
	out := [][][]byte{cmd}
	if e.ExpireAtMs > 0 {
		out = append(out, [][]byte{[]byte("PEXPIREAT"), kb,
			[]byte(strconv.FormatInt(e.ExpireAtMs, 10))})
	}
	return out
}
