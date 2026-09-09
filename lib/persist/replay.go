package persist

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Replay-side AOF parsing. The on-disk records are stamped RESP arrays
// (["ULTIMAREC", "<db>", "<seq>", cmd, args...]) written by appendSeq, so
// the parser is strict: no inline commands, no RESP3 types, no partial
// tails (a torn final record from a crash mid-write is tolerated: replay
// stops cleanly at the torn boundary, like Redis's aof-load-truncated).
//
// Replay MERGES the per-shard logs by the records' global sequence
// numbers rather than replaying one file at a time: broadcast commands
// (FLUSHDB/FLUSHALL) land in every log, and a file-at-a-time replay
// would re-execute the flush once per log, wiping keys restored from
// logs processed earlier. Broadcast copies share one seq; the merge
// executes each seq exactly once.

var errTornRecord = errors.New("persist: torn final AOF record (crash mid-write)")

// parseAOFCommand parses one RESP array of bulk strings at b[0:],
// returning the argv and the number of bytes consumed. errTornRecord
// marks a clean prefix truncation (tolerated at end of file); any other
// error is corruption.
func parseAOFCommand(b []byte) (argv [][]byte, consumed int, err error) {
	i := 0
	if len(b) == 0 || b[0] != '*' {
		return nil, 0, fmt.Errorf("persist: AOF record does not start with '*': %q", firstBytes(b))
	}
	i++
	n, ni, err := parseAOFInt(b[i:])
	if err != nil {
		return nil, 0, err
	}
	i += ni
	argv = make([][]byte, 0, n)
	for ; n > 0; n-- {
		if i >= len(b) || b[i] != '$' {
			return nil, 0, fmt.Errorf("persist: AOF bulk does not start with '$': %q", firstBytes(b[i:]))
		}
		i++
		ln, ni, err := parseAOFInt(b[i:])
		if err != nil {
			return nil, 0, err
		}
		i += ni
		if ln < 0 || len(b)-i < ln+2 {
			return nil, 0, errTornRecord
		}
		argv = append(argv, b[i:i+ln])
		i += ln
		if b[i] != '\r' || b[i+1] != '\n' {
			return nil, 0, fmt.Errorf("persist: AOF bulk missing CRLF terminator at offset %d", i)
		}
		i += 2
	}
	return argv, i, nil
}

// parseAOFInt parses "<digits>\r\n" at b[0:], returning the value and
// bytes consumed.
func parseAOFInt(b []byte) (int, int, error) {
	j := 0
	for j < len(b) && b[j] != '\r' {
		j++
	}
	if j+1 >= len(b) {
		return 0, 0, errTornRecord
	}
	if b[j+1] != '\n' {
		return 0, 0, fmt.Errorf("persist: AOF line missing LF: %q", firstBytes(b))
	}
	n, err := strconv.Atoi(string(b[:j]))
	if err != nil {
		return 0, 0, fmt.Errorf("persist: bad AOF number %q", string(b[:j]))
	}
	return n, j + 2, nil
}

func firstBytes(b []byte) string {
	const n = 24
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}

// parseStamped unwraps one on-disk record into (db, seq, command argv).
func parseStamped(fn string, argv [][]byte) (stampedRecord, error) {
	if len(argv) < 4 || string(argv[0]) != aofRecVerb {
		return stampedRecord{}, fmt.Errorf("persist: %s: record missing %s stamp: %q",
			fn, aofRecVerb, firstBytes(argv[0]))
	}
	db, err := strconv.Atoi(string(argv[1]))
	if err != nil {
		return stampedRecord{}, fmt.Errorf("persist: %s: bad record db %q", fn, argv[1])
	}
	seq, err := strconv.ParseUint(string(argv[2]), 10, 64)
	if err != nil {
		return stampedRecord{}, fmt.Errorf("persist: %s: bad record seq %q", fn, argv[2])
	}
	return stampedRecord{seq: seq, db: db, argv: argv[3:]}, nil
}

// collectAOFRecords parses every log in the AOF directory into stamped
// records (unsorted). A torn final record in any log is tolerated.
func collectAOFRecords(dir, name string) ([]stampedRecord, error) {
	s := &aofSet{dir: dir, name: name}
	ad := s.aofDir()
	files, err := aofFiles(ad)
	if err != nil {
		return nil, err
	}
	var recs []stampedRecord
	for _, fn := range files {
		raw, err := os.ReadFile(filepath.Join(ad, fn))
		if err != nil {
			return recs, err
		}
		for off := 0; off < len(raw); {
			argv, n, err := parseAOFCommand(raw[off:])
			if errors.Is(err, errTornRecord) {
				break // clean crash truncation: stop at the boundary
			}
			if err != nil {
				return recs, fmt.Errorf("persist: %s at offset %d: %w", fn, off, err)
			}
			off += n
			rec, err := parseStamped(fn, argv)
			if err != nil {
				return recs, err
			}
			recs = append(recs, rec)
		}
	}
	return recs, nil
}

// replayAOF merges every per-shard log in dir/name by sequence number and
// feeds each command to exec — once per seq, so the broadcast copies of
// FLUSHDB/FLUSHALL execute a single time at their global-order position.
// exec is called on the calling goroutine with (db, argv). Returns the
// number of commands replayed and the highest seq seen (the base for the
// live append sequence, so post-restart records sort after replayed ones).
func replayAOF(dir, name string, exec func(db int, argv [][]byte)) (int, uint64, error) {
	s := &aofSet{dir: dir, name: name}
	if _, err := os.Stat(s.aofDir()); err != nil {
		return 0, 0, nil // no AOF directory: nothing to replay
	}
	recs, err := collectAOFRecords(dir, name)
	if err != nil {
		return 0, 0, err
	}
	sort.SliceStable(recs, func(i, j int) bool { return recs[i].seq < recs[j].seq })
	total := 0
	var maxSeq, lastSeq uint64
	for i, r := range recs {
		if r.seq > maxSeq {
			maxSeq = r.seq
		}
		if i > 0 && r.seq == lastSeq {
			continue // broadcast copy of an already-executed record
		}
		lastSeq = r.seq
		exec(r.db, r.argv)
		total++
	}
	return total, maxSeq, nil
}

// maxAOFSeq scans the AOF directory for the highest record seq present
// (the SetAppendOnly(true) path, when logs exist but nothing was
// replayed this boot).
func maxAOFSeq(dir, name string) uint64 {
	s := &aofSet{dir: dir, name: name}
	if _, err := os.Stat(s.aofDir()); err != nil {
		return 0
	}
	recs, err := collectAOFRecords(dir, name)
	if err != nil {
		return 0
	}
	var hi uint64
	for _, r := range recs {
		if r.seq > hi {
			hi = r.seq
		}
	}
	return hi
}

// aofFiles lists the log files to replay: the manifest's list when
// present, else every shard-*.aof in the directory, in name order.
func aofFiles(ad string) ([]string, error) {
	if raw, err := os.ReadFile(filepath.Join(ad, "aof.manifest")); err == nil {
		var m aofManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("persist: parsing aof.manifest: %w", err)
		}
		return m.Files, nil
	}
	ents, err := os.ReadDir(ad)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range ents {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".aof" {
			files = append(files, e.Name())
		}
	}
	return files, nil
}

// aofExists reports whether an AOF directory with at least one log is
// present (startup restore precedence: appendonly → AOF over snapshot).
func aofExists(dir, name string) bool {
	ad := filepath.Join(dir, name)
	files, err := aofFiles(ad)
	if err != nil {
		return false
	}
	for _, fn := range files {
		if st, err := os.Stat(filepath.Join(ad, fn)); err == nil && st.Size() > 0 {
			return true
		}
	}
	return len(files) > 0
}
