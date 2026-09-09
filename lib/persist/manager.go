package persist

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// Config is the persist section of the server config (Redis-parity
// names, §8): Dir/DbFilename/AppendDirname/AppendOnly/AppendFsync/Save
// plus the Ultima-only Compress (snapshot payload compression, D9).
type Config struct {
	Dir           string // default "./data"
	DbFilename    string // default "dump.ultima"
	AppendDirname string // default "appendonlydir"
	AppendOnly    bool
	AppendFsync   string // "always" | "everysec" | "no"
	Save          string // Redis save rules: "900 1 300 10"; "" = off
	Compress      bool
}

// saveRule is one parsed "seconds changes" pair of a save directive.
type saveRule struct {
	seconds int64
	changes int64
}

// Manager drives snapshot save/load, the per-shard AOF, fsync policy and
// save-rule scheduling. It implements commands.Persister (the capture
// sink the command engine calls for every successful write command) and
// is installed after restore so replayed commands are not re-logged.
type Manager struct {
	cfg    Config
	shards *shard.Engine
	logger *slog.Logger

	cmdEng *commands.Engine

	aofMu sync.Mutex // guards aof swaps (SetAppendOnly)
	aof   *aofSet

	dirty          atomic.Int64 // write records since last successful save
	lastSaveUnix   atomic.Int64
	bgsaveRunning  atomic.Bool
	rewriteRunning atomic.Bool
	loading        atomic.Bool

	lastBgsaveStatus  atomic.Value // string: "ok" | "err: ..."
	lastRewriteStatus atomic.Value // string

	appendOnly atomic.Bool
	fsyncMode  atomic.Value // string: "always" | "everysec" | "no"
	saveStr    atomic.Value // string: current save rules (engine mirrors it)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewManager builds the manager; Start performs restore and opens the AOF.
func NewManager(cfg Config, shards *shard.Engine, logger *slog.Logger) *Manager {
	m := &Manager{
		cfg:    cfg,
		shards: shards,
		logger: logger,
		stopCh: make(chan struct{}),
	}
	m.appendOnly.Store(cfg.AppendOnly)
	m.fsyncMode.Store(cfg.AppendFsync)
	m.saveStr.Store(cfg.Save)
	m.lastBgsaveStatus.Store("ok")
	m.lastRewriteStatus.Store("ok")
	return m
}

// Start restores data synchronously (AOF when appendonly and logs exist,
// else the snapshot), opens the AOF for append when appendonly is on,
// installs the manager as the command engine's persistence sink, and
// starts the autosave/everysec-fsync ticker. It must run before any
// listener starts accepting (§13.1: restore-before-serve).
func (m *Manager) Start(cmdEng *commands.Engine) error {
	m.cmdEng = cmdEng
	m.loading.Store(true)
	restored := ""
	var aofBaseSeq uint64
	if m.cfg.AppendOnly && aofExists(m.cfg.Dir, m.cfg.AppendDirname) {
		n, maxSeq, err := replayAOF(m.cfg.Dir, m.cfg.AppendDirname, m.replayExec)
		if err != nil {
			m.loading.Store(false)
			return fmt.Errorf("persist: AOF replay: %w", err)
		}
		aofBaseSeq = maxSeq // appends continue above everything replayed
		restored = fmt.Sprintf("aof (%d commands)", n)
	} else if SnapshotExists(m.cfg.Dir, m.cfg.DbFilename) {
		man, err := LoadSnapshot(m.shards, m.cfg.Dir, m.cfg.DbFilename)
		if err != nil {
			m.loading.Store(false)
			return fmt.Errorf("persist: snapshot load: %w", err)
		}
		m.lastSaveUnix.Store(man.SavedAt / 1000)
		restored = fmt.Sprintf("snapshot (%d segments)", len(man.Segments))
	}
	m.loading.Store(false)

	if m.cfg.AppendOnly {
		aof, err := openAOF(m.cfg.Dir, m.cfg.AppendDirname, m.shards.ShardCount(), aofBaseSeq)
		if err != nil {
			return fmt.Errorf("persist: opening AOF: %w", err)
		}
		m.aofMu.Lock()
		m.aof = aof
		m.aofMu.Unlock()
	}

	cmdEng.SetPersister(m)
	cmdEng.SetSaveString(m.cfg.Save)
	cmdEng.SetAppendOnlyFlag(m.appendOnly.Load())

	m.wg.Add(1)
	go m.tick()
	if restored != "" {
		m.logger.Info("persist: restored", "source", restored)
	}
	return nil
}

// replayExec feeds one AOF record through the command engine with a
// synthetic authed connection (the replay ConnState, §13.1). Replies are
// discarded; errors are logged (replay continues, as Redis does with
// aof-load-truncated semantics).
func (m *Manager) replayExec(db int, argv [][]byte) {
	cs := &commands.ConnState{Proto: 2, Authed: true, DB: db, Addr: "aof-replay"}
	if v := m.cmdEng.Execute(cs, argv); v.Kind == resp.KindError {
		m.logger.Warn("persist: AOF replay command failed", "argv0", string(argv[0]), "err", v.Str)
	}
}

// --- commands.Persister ------------------------------------------------

// LogCommand is the command engine's capture sink: shardHint is the
// shard owning the rewritten key, or -1 to broadcast (FLUSHDB/FLUSHALL).
// Every successful write bumps the dirty counter (the save-rule and
// rdb_changes_since_last_save input) whether or not the AOF is on.
func (m *Manager) LogCommand(shardHint, db int, argv [][]byte) {
	m.dirty.Add(1)
	m.aofMu.Lock()
	aof := m.aof
	m.aofMu.Unlock()
	if aof == nil {
		return
	}
	var err error
	if shardHint < 0 {
		err = aof.appendAll(db, argv)
	} else {
		err = aof.appendRecord(shardHint, db, argv)
	}
	if err != nil {
		m.logger.Error("persist: AOF append failed", "err", err)
		return
	}
	if m.fsyncMode.Load().(string) == "always" {
		if shardHint < 0 {
			aof.fsync()
		} else {
			aof.fsyncOne(shardHint)
		}
	}
}

// LogKeyGone synthesizes a DEL for a shard-originated key removal
// (expiry sweep, passive expiry, eviction — the OnKeyGone sink, §13.1).
func (m *Manager) LogKeyGone(db int, key string) {
	m.LogCommand(m.shards.ShardIndex([]byte(key)), db, [][]byte{[]byte("DEL"), []byte(key)})
}

// --- SAVE / BGSAVE / LASTSAVE / BGREWRITEAOF ---------------------------

// Save performs a synchronous whole-server snapshot under PauseAll (all
// shards parked → the dump is one global point-in-time).
func (m *Manager) Save() error {
	tok, resume := m.shards.PauseAll()
	defer resume()
	return m.saveLocked(tok)
}

func (m *Manager) saveLocked(tok uint64) error {
	man, err := WriteSnapshotTok(m.shards, tok, m.cfg.Dir, m.cfg.DbFilename, m.cfg.Compress)
	if err != nil {
		m.lastBgsaveStatus.Store("err: " + err.Error())
		return err
	}
	m.lastSaveUnix.Store(man.SavedAt / 1000)
	m.dirty.Store(0)
	m.lastBgsaveStatus.Store("ok")
	return nil
}

// BGSave snapshots in the background: per-shard staggered DoShard tasks,
// each shard a per-shard point-in-time (documented divergence from
// fork-RDB, §13.1). Redis's single-child rule applies: a running AOF
// rewrite rejects BGSAVE (ErrPersistChildActive), a running BGSAVE
// rejects another (commands.ErrPersistBusy).
func (m *Manager) BGSave() error {
	if m.rewriteRunning.Load() {
		return commands.ErrPersistChildActive
	}
	if !m.bgsaveRunning.CompareAndSwap(false, true) {
		return commands.ErrPersistBusy
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer m.bgsaveRunning.Store(false)
		if err := m.saveLocked(0); err != nil {
			m.logger.Error("persist: BGSAVE failed", "err", err)
		}
	}()
	return nil
}

// LastSave is the unix-seconds time of the last successful save (0 = never).
func (m *Manager) LastSave() int64 { return m.lastSaveUnix.Load() }

// BGRewriteAOF rewrites the per-shard logs as a minimal command stream.
// With appendonly off it is a no-op success, matching Redis 7.2.7's
// "Background append only file rewriting started" reply in that state.
// With a rewrite or BGSAVE already running it reports busy (Redis's
// "scheduled" reply).
func (m *Manager) BGRewriteAOF() error {
	if m.bgsaveRunning.Load() {
		return commands.ErrPersistBusy
	}
	m.aofMu.Lock()
	aof := m.aof
	m.aofMu.Unlock()
	if aof == nil {
		return nil
	}
	if !m.rewriteRunning.CompareAndSwap(false, true) {
		return commands.ErrPersistBusy
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer m.rewriteRunning.Store(false)
		// Freeze mutations for the whole dump+swap (the rewrite holds a
		// pause token; see aofSet.rewrite for the crash-consistency
		// argument), then drain write commands caught between their
		// mutation and their AOF capture before cutting over.
		tok, resume := m.shards.PauseAll()
		defer resume()
		for !m.cmdEng.PersistQuiesced() {
			time.Sleep(time.Millisecond)
		}
		if err := aof.rewrite(m.shards, tok); err != nil {
			m.lastRewriteStatus.Store("err: " + err.Error())
			m.logger.Error("persist: BGREWRITEAOF failed", "err", err)
			return
		}
		m.lastRewriteStatus.Store("ok")
	}()
	return nil
}

// Loading reports whether restore is in progress (INFO loading:1).
func (m *Manager) Loading() bool { return m.loading.Load() }

// --- CONFIG-facing knobs -------------------------------------------------

// Dir reports the persistence directory.
func (m *Manager) Dir() string { return m.cfg.Dir }

// DbFilename reports the snapshot file name.
func (m *Manager) DbFilename() string { return m.cfg.DbFilename }

// AppendOnly reports the appendonly flag.
func (m *Manager) AppendOnly() bool { return m.appendOnly.Load() }

// SetAppendOnly opens the AOF (yes) or fsyncs and closes it (no),
// mirroring CONFIG SET appendonly. Turning it on when a snapshot exists
// does not backfill (Redis 7.2 has the same cold-start caveat when the
// AOF directory is empty: existing data is not in the new AOF).
func (m *Manager) SetAppendOnly(on bool) error {
	if on == m.appendOnly.Load() {
		return nil
	}
	m.aofMu.Lock()
	defer m.aofMu.Unlock()
	if on {
		aof, err := openAOF(m.cfg.Dir, m.cfg.AppendDirname, m.shards.ShardCount(),
			maxAOFSeq(m.cfg.Dir, m.cfg.AppendDirname))
		if err != nil {
			return err
		}
		m.aof = aof
	} else {
		if m.aof != nil {
			m.aof.closeAll()
			m.aof = nil
		}
	}
	m.appendOnly.Store(on)
	return nil
}

// AppendFsync reports the fsync policy string.
func (m *Manager) AppendFsync() string { return m.fsyncMode.Load().(string) }

// SetAppendFsync sets the fsync policy; the caller validates the value.
func (m *Manager) SetAppendFsync(v string) { m.fsyncMode.Store(v) }

// SetSaveRules replaces the save-rules string (CONFIG SET save); the
// ticker re-parses it each cycle.
func (m *Manager) SetSaveRules(v string) { m.saveStr.Store(v) }

// --- INFO ------------------------------------------------------------------

// InfoPersistence renders the INFO persistence section fields.
func (m *Manager) InfoPersistence(sb *strings.Builder) {
	loading := 0
	if m.loading.Load() {
		loading = 1
	}
	fmt.Fprintf(sb, "loading:%d\r\n", loading)
	fmt.Fprintf(sb, "rdb_changes_since_last_save:%d\r\n", m.dirty.Load())
	bgsave := 0
	if m.bgsaveRunning.Load() {
		bgsave = 1
	}
	fmt.Fprintf(sb, "rdb_bgsave_in_progress:%d\r\n", bgsave)
	fmt.Fprintf(sb, "rdb_last_save_time:%d\r\n", m.lastSaveUnix.Load())
	fmt.Fprintf(sb, "rdb_last_bgsave_status:%s\r\n", m.lastBgsaveStatus.Load().(string))
	aofEnabled := 0
	if m.appendOnly.Load() {
		aofEnabled = 1
	}
	fmt.Fprintf(sb, "aof_enabled:%d\r\n", aofEnabled)
	rewriting := 0
	if m.rewriteRunning.Load() {
		rewriting = 1
	}
	fmt.Fprintf(sb, "aof_rewrite_in_progress:%d\r\n", rewriting)
	fmt.Fprintf(sb, "aof_last_bgrewrite_status:%s\r\n", m.lastRewriteStatus.Load().(string))
}

// --- scheduling / shutdown -------------------------------------------------

// tick is the once-per-second persist cron: everysec fsync and the
// save-rule checker (Redis serverCron's 100ms checks, relaxed to 1s).
func (m *Manager) tick() {
	defer m.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
		}
		m.aofMu.Lock()
		aof := m.aof
		m.aofMu.Unlock()
		if aof != nil && m.fsyncMode.Load().(string) == "everysec" {
			aof.fsync()
		}
		m.checkSaveRules()
	}
}

// checkSaveRules fires a BGSave when any "seconds changes" rule is met:
// at least `changes` writes since the last save and at least `seconds`
// elapsed since it.
func (m *Manager) checkSaveRules() {
	rules := parseSaveRules(m.saveStr.Load().(string))
	if len(rules) == 0 {
		return
	}
	last := m.lastSaveUnix.Load()
	if last == 0 {
		last = time.Now().Unix() // no save yet: measure from boot
	}
	now := time.Now().Unix()
	dirty := m.dirty.Load()
	for _, r := range rules {
		if dirty >= r.changes && now-last >= r.seconds {
			if err := m.BGSave(); err != nil && !errors.Is(err, commands.ErrPersistBusy) {
				m.logger.Error("persist: auto-save failed", "err", err)
			}
			return
		}
	}
}

// parseSaveRules parses a Redis save string ("900 1 300 10") into rules;
// an empty or malformed string yields none (malformed is rejected at
// CONFIG SET time, so this is defensive).
func parseSaveRules(v string) []saveRule {
	var rules []saveRule
	f := strings.Fields(v)
	for i := 0; i+1 < len(f); i += 2 {
		var secs, changes int64
		if _, err := fmt.Sscanf(f[i], "%d", &secs); err != nil {
			return nil
		}
		if _, err := fmt.Sscanf(f[i+1], "%d", &changes); err != nil {
			return nil
		}
		rules = append(rules, saveRule{seconds: secs, changes: changes})
	}
	return rules
}

// Close stops the ticker, fsyncs and closes the AOF, and — when
// appendonly is off, save rules are configured, and there are unsaved
// writes — takes a final snapshot (the SIGTERM-shutdown save, §13.1).
// Call after the listeners are closed and before shards.Close.
func (m *Manager) Close() {
	close(m.stopCh)
	m.aofMu.Lock()
	if m.aof != nil {
		m.aof.closeAll()
		m.aof = nil
	}
	m.aofMu.Unlock()
	if !m.appendOnly.Load() && len(parseSaveRules(m.saveStr.Load().(string))) > 0 && m.dirty.Load() > 0 {
		if err := m.saveLocked(0); err != nil {
			m.logger.Error("persist: shutdown snapshot failed", "err", err)
		}
	}
	m.wg.Wait()
}

// nowUnixMs is the wall clock in milliseconds (dump TTL filtering).
func nowUnixMs() int64 { return time.Now().UnixMilli() }
