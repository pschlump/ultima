// Package persist implements Ultima's persistence layer (M5c, design doc
// §13.1, decision D9 — own formats, not Redis's RDB/AOF bytes):
//
//   - Snapshot (RDB-equivalent): one file, magic "ULTIMA01", a header,
//     then one segment per (db, shard); CRC-64 per segment and over the
//     whole file. Segment payloads optionally LZW-compressed (pluto lzw,
//     the documented LZF stand-in) behind the snapshot_compress flag.
//   - AOF: per-shard append logs (MP-AOF-like) of RESP-serialized
//     commands plus a JSON manifest; replay feeds argv back through
//     commands.Engine.Execute.
//
// The package imports lib/shard and lib/commands; neither imports back —
// the command engine talks to persistence through a small interface
// (commands.Persister) installed at startup.
package persist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/pschlump/pluto/crc"
	"github.com/pschlump/pluto/quicklist"

	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/types"
)

// Snapshot file layout (all integers little-endian):
//
//	magic    "ULTIMA01" (8 bytes)
//	version  u32 (currently 1)
//	shards   u32   dbs u32   savedAtMs i64   flags u32 (bit0: LZW payloads)
//	segments..., each:
//	  db u32   shard u32   entries u32   payloadLen u32   payloadCRC u64
//	  payload (payloadLen bytes; LZW-compressed when flags bit0 set)
//	trailer  u64 — CRC-64 of every byte before the trailer
//
// Entry payload record:
//
//	keyLen u32, key bytes, expireAtMs i64 (0 = none), typeTag u8, value
//
// Value by typeTag: string = bytes; list/set = u32 count + count ×
// (len u32 + bytes); hash = u32 count + count × (field, value) byte
// pairs; zset = u32 count + count × (member bytes, score f64).
const (
	snapshotMagic   = "ULTIMA01"
	snapshotVersion = 1

	flagCompress = 1 << 0

	tagString = 0
	tagList   = 1
	tagSet    = 2
	tagZSet   = 3
	tagHash   = 4
)

// segHeader is the fixed 24-byte per-segment preamble.
type segHeader struct {
	DB, Shard  uint32
	Entries    uint32
	PayloadLen uint32
	PayloadCRC uint64
}

// --- encoding helpers ------------------------------------------------

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

func appendI64(dst []byte, v int64) []byte { return appendU64(dst, uint64(v)) }

func appendF64(dst []byte, f float64) []byte { return appendU64(dst, math.Float64bits(f)) }

func appendStr(dst []byte, s string) []byte {
	dst = appendU32(dst, uint32(len(s)))
	return append(dst, s...)
}

func appendBytes(dst, b []byte) []byte {
	dst = appendU32(dst, uint32(len(b)))
	return append(dst, b...)
}

func appendSegHeader(dst []byte, h segHeader) []byte {
	dst = appendU32(dst, h.DB)
	dst = appendU32(dst, h.Shard)
	dst = appendU32(dst, h.Entries)
	dst = appendU32(dst, h.PayloadLen)
	return appendU64(dst, h.PayloadCRC)
}

// encodeEntry appends one entry record to dst.
func encodeEntry(dst []byte, key string, e *shard.Entry) []byte {
	dst = appendStr(dst, key)
	dst = appendI64(dst, e.ExpireAtMs)
	switch e.Type {
	case shard.TypeString:
		dst = append(dst, tagString)
		dst = appendBytes(dst, e.Str)
	case shard.TypeList:
		l := e.Obj.(*types.List)
		dst = append(dst, tagList)
		dst = appendU32(dst, uint32(l.Len()))
		for _, v := range l.All() {
			dst = appendBytes(dst, v)
		}
	case shard.TypeSet:
		s := e.Obj.(*types.Set)
		members := s.Members()
		dst = append(dst, tagSet)
		dst = appendU32(dst, uint32(len(members)))
		for _, m := range members {
			dst = appendStr(dst, m)
		}
	case shard.TypeZSet:
		z := e.Obj.(*types.ZSet)
		dst = append(dst, tagZSet)
		dst = appendU32(dst, uint32(z.Len()))
		z.Each(func(member string, score float64) {
			dst = appendStr(dst, member)
			dst = appendF64(dst, score)
		})
	case shard.TypeHash:
		h := e.Obj.(*types.Hash)
		dst = append(dst, tagHash)
		dst = appendU32(dst, uint32(h.Len()))
		h.Each(func(field, value string) {
			dst = appendStr(dst, field)
			dst = appendStr(dst, value)
		})
	}
	return dst
}

// --- decoding helpers ------------------------------------------------

var (
	errShort    = errors.New("persist: truncated snapshot")
	errBadMagic = errors.New("persist: bad magic (not an Ultima snapshot)")
	errBadCRC   = errors.New("persist: CRC mismatch (corrupt snapshot)")
	errBadTag   = errors.New("persist: unknown type tag")
	errVersion  = errors.New("persist: unsupported snapshot version")
)

// decoder is a cursor over a byte slice; the first short read latches
// err and every later call is a no-op returning zero values.
type decoder struct {
	b   []byte
	off int
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if len(d.b)-d.off < n {
		d.err = errShort
		return nil
	}
	b := d.b[d.off : d.off+n]
	d.off += n
	return b
}

func (d *decoder) u32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (d *decoder) u64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (d *decoder) i64() int64 { return int64(d.u64()) }

func (d *decoder) f64() float64 { return math.Float64frombits(d.u64()) }

func (d *decoder) str() string {
	n := d.u32()
	if d.err != nil {
		return ""
	}
	b := d.take(int(n))
	return string(b)
}

func (d *decoder) bytes() []byte {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	return d.take(int(n))
}

func (d *decoder) segHeader() segHeader {
	return segHeader{
		DB:         d.u32(),
		Shard:      d.u32(),
		Entries:    d.u32(),
		PayloadLen: d.u32(),
		PayloadCRC: d.u64(),
	}
}

// decodeEntry decodes one entry record, building the collection value.
func decodeEntry(d *decoder) (key string, e *shard.Entry) {
	key = d.str()
	expireAt := d.i64()
	tag := d.take(1)
	if d.err != nil {
		return "", nil
	}
	e = &shard.Entry{ExpireAtMs: expireAt}
	switch tag[0] {
	case tagString:
		e.Type = shard.TypeString
		e.Str = append([]byte(nil), d.bytes()...)
	case tagList:
		l := types.NewList()
		n := d.u32()
		for ; n > 0 && d.err == nil; n-- {
			l.PushTail(append([]byte(nil), d.bytes()...))
		}
		e.Type = shard.TypeList
		e.Obj = l
	case tagSet:
		s := types.NewSet()
		n := d.u32()
		for ; n > 0 && d.err == nil; n-- {
			s.Add(d.str())
		}
		e.Type = shard.TypeSet
		e.Obj = s
	case tagZSet:
		z := types.NewZSet()
		n := d.u32()
		for ; n > 0 && d.err == nil; n-- {
			m := d.str()
			score := d.f64()
			z.Add(m, score)
		}
		e.Type = shard.TypeZSet
		e.Obj = z
	case tagHash:
		h := types.NewHash()
		n := d.u32()
		for ; n > 0 && d.err == nil; n-- {
			f := d.str()
			v := d.str()
			h.Set(f, v)
		}
		e.Type = shard.TypeHash
		e.Obj = h
	default:
		d.err = fmt.Errorf("%w: %d", errBadTag, tag[0])
		return "", nil
	}
	return key, e
}

// --- compression -----------------------------------------------------

// codec is the payload compressor; quicklist.LZWCodec is the documented
// LZF stand-in (design doc §13.1).
var codec = quicklist.LZWCodec()

func compressPayload(b []byte) []byte { return codec.Compress(b) }

func decompressPayload(b []byte) []byte { return codec.Decompress(b) }

// --- CRC -------------------------------------------------------------

// crcOf is the snapshot/AOF checksum: CRC-64/XZ via pluto (same value
// family Redis uses for its RDB checksum).
func crcOf(b []byte) uint64 { return crc.Checksum64(b, crc.ECMATable) }
