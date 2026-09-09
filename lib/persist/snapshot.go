package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pschlump/ultima/lib/shard"
)

// Manifest is the JSON sidecar (`snapshot.manifest`) describing one
// snapshot file: enough to sanity-check a restore and to feed LASTSAVE.
type Manifest struct {
	Version  int    `json:"version"`
	SavedAt  int64  `json:"saved_at_ms"`
	Shards   int    `json:"shards"`
	DBs      int    `json:"dbs"`
	Compress bool   `json:"compress"`
	File     string `json:"file"`
	Segments []struct {
		DB      int    `json:"db"`
		Shard   int    `json:"shard"`
		Entries int    `json:"entries"`
		CRC     uint64 `json:"crc"`
	} `json:"segments"`
}

// WriteSnapshot dumps the whole keyspace to dir/dbfilename (atomically,
// via a temp file + rename) and writes the manifest sidecar. Each
// (db, shard) segment is serialized inside that shard's goroutine via
// DumpDB, so the segment is a consistent per-shard point-in-time.
//
// Callers wanting whole-server consistency (SAVE) call WriteSnapshotTok
// under shard.Engine.PauseAll; BGSAVE runs this on its own goroutine
// without the pause (per-shard point-in-time — the documented divergence
// from fork-RDB, §13.1).
func WriteSnapshot(eng *shard.Engine, dir, dbfilename string, compress bool) (*Manifest, error) {
	return writeSnapshot(eng, 0, dir, dbfilename, compress)
}

// WriteSnapshotTok is WriteSnapshot inside a PauseAll pause: segment
// dumps must carry the pause token or they deadlock against the parked
// shard goroutines.
func WriteSnapshotTok(eng *shard.Engine, tok uint64, dir, dbfilename string, compress bool) (*Manifest, error) {
	return writeSnapshot(eng, tok, dir, dbfilename, compress)
}

func writeSnapshot(eng *shard.Engine, tok uint64, dir, dbfilename string, compress bool) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp := filepath.Join(dir, dbfilename+".tmp")
	final := filepath.Join(dir, dbfilename)

	now := time.Now()
	m := &Manifest{
		Version:  snapshotVersion,
		SavedAt:  now.UnixMilli(),
		Shards:   eng.ShardCount(),
		DBs:      eng.MaxDBs(),
		Compress: compress,
		File:     dbfilename,
	}

	var out []byte
	out = append(out, snapshotMagic...)
	out = appendU32(out, snapshotVersion)
	out = appendU32(out, uint32(m.Shards))
	out = appendU32(out, uint32(m.DBs))
	out = appendI64(out, m.SavedAt)
	var flags uint32
	if compress {
		flags |= flagCompress
	}
	out = appendU32(out, flags)

	doShard := func(i int, fn func(s *shard.Shard)) {
		if tok != 0 {
			eng.DoTok(tok, i, fn)
			return
		}
		eng.DoShard(i, fn)
	}

	nowMs := now.UnixMilli()
	for db := 0; db < m.DBs; db++ {
		for sh := 0; sh < m.Shards; sh++ {
			var payload []byte
			var n uint32
			doShard(sh, func(s *shard.Shard) {
				s.DumpDB(db, nowMs, func(key string, e *shard.Entry) bool {
					payload = encodeEntry(payload, key, e)
					n++
					return true
				})
			})
			if n == 0 {
				continue // empty segments are omitted
			}
			crc := crcOf(payload)
			body := payload
			if compress {
				body = compressPayload(payload)
			}
			out = appendSegHeader(out, segHeader{
				DB:         uint32(db),
				Shard:      uint32(sh),
				Entries:    n,
				PayloadLen: uint32(len(body)),
				PayloadCRC: crc, // over the UNCOMPRESSED payload
			})
			out = append(out, body...)
			m.Segments = append(m.Segments, struct {
				DB      int    `json:"db"`
				Shard   int    `json:"shard"`
				Entries int    `json:"entries"`
				CRC     uint64 `json:"crc"`
			}{DB: db, Shard: sh, Entries: int(n), CRC: crc})
		}
	}
	out = appendU64(out, crcOf(out))

	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return nil, err
	}
	if err := syncDir(tmp); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "snapshot.manifest"), mb, 0o644); err != nil {
		return nil, err
	}
	return m, nil
}

// LoadSnapshot reads dir/dbfilename, verifies magic, per-segment and
// whole-file CRCs, and stores every entry into eng (rebuilding the
// expiry heap via PushExpire). Entries that expired while the server was
// down are skipped, as Redis's RDB load does. Returns the manifest.
func LoadSnapshot(eng *shard.Engine, dir, dbfilename string) (*Manifest, error) {
	raw, err := os.ReadFile(filepath.Join(dir, dbfilename))
	if err != nil {
		return nil, err
	}
	d := &decoder{b: raw}
	if string(d.take(len(snapshotMagic))) != snapshotMagic {
		return nil, errBadMagic
	}
	if v := d.u32(); v != snapshotVersion {
		return nil, fmt.Errorf("%w: %d", errVersion, v)
	}
	shards := int(d.u32())
	dbs := int(d.u32())
	m := &Manifest{Version: snapshotVersion, SavedAt: d.i64(), Shards: shards, DBs: dbs}
	flags := d.u32()
	m.Compress = flags&flagCompress != 0
	if d.err != nil {
		return nil, d.err
	}

	// The trailer is the last 8 bytes; it covers everything before it.
	if len(raw) < 8 {
		return nil, errShort
	}
	body, trailer := raw[:len(raw)-8], binary.LittleEndian.Uint64(raw[len(raw)-8:])
	if crcOf(body) != trailer {
		return nil, errBadCRC
	}

	nowMs := time.Now().UnixMilli()
	d.off = len(snapshotMagic) + 4 + 4 + 4 + 8 + 4 // past the header
	for d.off < len(body) {
		h := d.segHeader()
		if d.err != nil {
			return nil, d.err
		}
		payloadBytes := d.take(int(h.PayloadLen))
		if d.err != nil {
			return nil, d.err
		}
		payload := payloadBytes
		if m.Compress {
			payload = decompressPayload(payloadBytes)
		}
		if crcOf(payload) != h.PayloadCRC {
			return nil, fmt.Errorf("%w: segment db=%d shard=%d", errBadCRC, h.DB, h.Shard)
		}
		ed := &decoder{b: payload}
		sh := int(h.Shard)
		db := int(h.DB)
		if sh >= eng.ShardCount() || db >= eng.MaxDBs() {
			return nil, fmt.Errorf("persist: segment db=%d shard=%d out of range (engine %d dbs x %d shards)", db, sh, eng.MaxDBs(), eng.ShardCount())
		}
		for i := uint32(0); i < h.Entries; i++ {
			key, e := decodeEntry(ed)
			if ed.err != nil {
				return nil, fmt.Errorf("persist: segment db=%d shard=%d entry %d: %w", db, sh, i, ed.err)
			}
			if e.ExpireAtMs > 0 && e.ExpireAtMs <= nowMs {
				continue // expired while down: not loaded
			}
			eng.DoShard(sh, func(s *shard.Shard) {
				s.Store(db, key, e)
				if e.ExpireAtMs > 0 {
					s.PushExpire(db, key, e)
				}
			})
		}
		m.Segments = append(m.Segments, struct {
			DB      int    `json:"db"`
			Shard   int    `json:"shard"`
			Entries int    `json:"entries"`
			CRC     uint64 `json:"crc"`
		}{DB: db, Shard: sh, Entries: int(h.Entries), CRC: h.PayloadCRC})
	}
	return m, nil
}

// SnapshotExists reports whether dir/dbfilename is present.
func SnapshotExists(dir, dbfilename string) bool {
	_, err := os.Stat(filepath.Join(dir, dbfilename))
	return err == nil
}

// syncDir fsyncs a freshly written file (durability of the bytes before
// the rename).
func syncDir(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
