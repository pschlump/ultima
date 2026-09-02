# Pluto Request 08 — Geohash / geo helper package

**Type:** New package `geo/`
**Priority:** P4 — blocks the Redis GEO command family
**Blocks:** GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE,
GEORADIUS (legacy)

## Context

Redis implements GEO on top of a sorted set: each member's score is a 52-bit
interleaved geohash (`note/redis/src/geohash.c`, `geo.c`; GEO sits on zset per
`t_zset.c` usage). Ultima will do the same — once the skip-list range API
(request 01) exists — so what is needed from pluto is the **pure geohash math**:
encoding, decoding, neighbor computation, and radius→score-range translation.
Pure functions, no state; no `_ts` twin needed. Stdlib only.

## Requirements

### 1. API

```go
// 52-bit interleaved geohash, lat in [-85.05112878, 85.05112878],
// lon in [-180, 180] (Redis bounds).
func Encode(lat, lon float64) (uint64, error)   // error = out of range
func Decode(hash uint64) (lat, lon float64)     // center of the cell

// Geohash neighbors: the 8 adjacent cells at the same precision.
func Neighbors(hash uint64) [8]uint64           // N, NE, E, SE, S, SW, W, NW (order documented)

// Area of interest for a radius query: the score ranges to look up in the
// sorted set. Returned ranges partition the cell + its neighbors.
func RangesForRadius(lat, lon, radiusMeters float64) (ranges [][2]uint64, err error)

// Bounding-box variant for GEOSEARCH BYBOX.
func RangesForBox(lat, lon, widthMeters, heightMeters float64) (ranges [][2]uint64, err error)

// Distance between two points, haversine, in meters.
func Distance(lat1, lon1, lat2, lon2 float64) float64

// Unit conversion: m, km, mi, ft (Redis unit set).
func ToMeters(v float64, unit string) (float64, error)
func FromMeters(v float64, unit string) (float64, error)
```

### 2. Correctness requirements

- Encode/Decode must be bit-compatible with Redis's `geohashEncode`/
  `geohashDecode` (52-bit, 26 bits per axis) so that scores Ultima stores are
  meaningful to Redis tooling and differential tests pass.
- Cell edge behavior: decoding returns cell center; encoding a decoded center
  must round-trip to the same hash.
- Radius searches must be conservative-complete: the 3×3 cell block at the
  computed precision must fully cover the circle; Ultima post-filters with
  `Distance` for exactness (document this contract).
- Longitude wraparound at ±180 must be handled in Neighbors and Ranges.

### 3. Edge cases

- Lat/lon out of range → error (Redis rejects these on GEOADD).
- Radius spanning the pole or antimeridian → ranges still complete.
- Zero/negative radius → error or empty, documented.

### 4. Tests

- Bit-exactness vectors taken from real Redis: for a set of known (lat, lon),
  assert `Encode` equals the score Redis stores (generate vectors once with
  `redis-cli GEOADD` + `ZSCORE`, bake into test).
- Round-trip: decode(encode(x)) re-encodes identically, for random points.
- Completeness fuzz: random center + radius; assert every point within radius
  falls inside `RangesForRadius` output (no false negatives; false positives OK).
- Distance accuracy vs known city-pair distances (±0.5%).

## References

- Redis: `note/redis/src/geohash.c`, `geohash_helper.c`, `geo.c`
- Redis command semantics: `note/redis/src/commands/geo*.json`
