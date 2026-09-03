package commands

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// Shared helpers for the P1 collection commands (hashes, lists, sets,
// sorted sets): typed lookup, Redis string2d-compatible float parsing,
// score/lex range bound parsing, and the SCAN-family option tail.

// getColl fetches key's entry expecting type t: (nil, false) when the key
// is missing, (nil, true) on WRONGTYPE.
func getColl(s *shard.Shard, db int, key string, t shard.Type) (*shard.Entry, bool) {
	ent, ok := s.Lookup(db, key)
	if !ok {
		return nil, false
	}
	if ent.Type != t {
		return nil, true
	}
	return ent, false
}

// storeColl replaces key with a collection entry, preserving any TTL
// (Redis: overwriting a key in place keeps its TTL only for in-place
// mutations; store commands like SINTERSTORE drop the TTL... they replace
// the object via dbDelete+setKey, clearing it).
func storeColl(s *shard.Shard, db int, key string, t shard.Type, obj any) {
	s.Store(db, key, &shard.Entry{Type: t, Obj: obj})
}

// --- float parsing (Redis string2d / string2ld via strtod) -----------------

// parseFloat mirrors Redis string2d: C strtod syntax (hex floats,
// "inf"/"infinity" any case, exponents), rejecting NaN, leading/trailing
// junk, ERANGE overflow, and underflow of a nonzero literal all the way
// to zero (Go's ParseFloat no longer reports that as ErrRange, so the
// mantissa is checked explicitly).
func parseFloat(b []byte) (float64, bool) {
	s := string(b)
	if s == "" || strings.ContainsAny(s, "_ \t\n\v\f\r") {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) && isHexFloat(s) {
		// Go requires the p-exponent on hex floats; strtod does not.
		v, err = strconv.ParseFloat(s+"p0", 64)
	}
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			// Overflow (±Inf) and underflow to exactly 0 are invalid;
			// underflow to a subnormal is accepted, as strtod does.
			if math.IsInf(v, 0) || v == 0 {
				return 0, false
			}
			return v, true
		}
		return 0, false
	}
	if math.IsNaN(v) {
		return 0, false
	}
	if v == 0 && nonzeroMantissa(s) {
		return 0, false // underflow to zero (strtod ERANGE)
	}
	return v, true
}

// isHexFloat reports whether s is a hex float literal missing the
// Go-required binary exponent.
func isHexFloat(s string) bool {
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	return (strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X")) &&
		!strings.ContainsAny(s, "pP")
}

// nonzeroMantissa reports whether the literal's mantissa (before any
// exponent) has a nonzero digit.
func nonzeroMantissa(s string) bool {
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i = 1
	}
	hex := false
	if strings.HasPrefix(s[i:], "0x") || strings.HasPrefix(s[i:], "0X") {
		hex = true
		i += 2
	}
	for ; i < len(s); i++ {
		c := s[i]
		if c == 'p' || c == 'P' || (!hex && (c == 'e' || c == 'E')) {
			break
		}
		switch {
		case c >= '1' && c <= '9':
			return true
		case hex && (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'):
			return true
		}
	}
	return false
}

// formatHumanFloat renders a HINCRBYFLOAT result the way Redis ld2string
// does on this platform (long double == double): %f with 17 fractional
// digits, trailing zeros (and a bare dot) trimmed.
func formatHumanFloat(f float64) string {
	s := strconv.FormatFloat(f, 'f', 17, 64)
	if strings.IndexByte(s, '.') >= 0 {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	return s
}

// --- score / lex range bounds ------------------------------------------------

// scoreBound is one end of a BYSCORE range.
type scoreBound struct {
	val  float64
	excl bool
}

// parseScoreBound parses "1.5", "(1.5", "-inf", "+inf" / "inf". The error
// reply is "ERR min or max is not a float".
func parseScoreBound(b []byte) (scoreBound, bool) {
	var sb scoreBound
	if len(b) > 0 && b[0] == '(' {
		sb.excl = true
		b = b[1:]
	}
	v, ok := parseFloat(b)
	if !ok {
		return sb, false
	}
	sb.val = v
	return sb, true
}

// lexBound is one end of a BYLEX range: kind is '-', '+', '[' or '('.
type lexBound struct {
	kind byte
	val  string // for '[' and '('
}

// parseLexBound parses "-", "+", "[member", "(member". The error reply is
// "ERR min or max not valid string range item".
func parseLexBound(b []byte) (lexBound, bool) {
	if len(b) == 0 {
		return lexBound{}, false
	}
	switch b[0] {
	case '-':
		if len(b) == 1 {
			return lexBound{kind: '-'}, true
		}
	case '+':
		if len(b) == 1 {
			return lexBound{kind: '+'}, true
		}
	case '[':
		return lexBound{kind: '[', val: string(b[1:])}, true
	case '(':
		return lexBound{kind: '(', val: string(b[1:])}, true
	}
	return lexBound{}, false
}

var (
	errBadFloatRange = resp.Err("ERR min or max is not a float")
	errBadLexRange   = resp.Err("ERR min or max not valid string range item")
	errNotPositive   = resp.Err("ERR value is out of range, must be positive")
	errBadFloat      = resp.Err("ERR value is not a valid float")
)

// --- SCAN-family option tail (HSCAN/SSCAN/ZSCAN) ----------------------------

// scanOpts parses "MATCH pat" / "COUNT n" pairs starting at args[i].
func scanOpts(args [][]byte, i int) (match []byte, count int64, errV resp.Value, failed bool) {
	count = 10
	for ; i < len(args); i++ {
		switch lowerASCII(args[i]) {
		case "match":
			if i+1 >= len(args) {
				return nil, 0, errSyntax, true
			}
			match = args[i+1]
			i++
		case "count":
			if i+1 >= len(args) {
				return nil, 0, errSyntax, true
			}
			v, ok := parseIntStrict(args[i+1])
			if !ok {
				return nil, 0, errNotInt, true
			}
			if v <= 0 {
				return nil, 0, errSyntax, true
			}
			count = v
			i++
		default:
			return nil, 0, errSyntax, true
		}
	}
	return match, count, resp.Value{}, false
}

// scanWindow applies offset-cursor iteration over items: it takes up to
// count items starting at cursor, filters them with keep, and returns the
// kept items plus the next cursor ("0" at the end). The cursor is opaque
// to clients; only full-iteration termination matters.
func scanWindow[T any](items []T, cursor uint64, count int64, keep func(T) bool) ([]T, string) {
	if cursor > uint64(len(items)) {
		return nil, "0"
	}
	end := cursor + uint64(count)
	if end >= uint64(len(items)) {
		end = uint64(len(items))
	}
	out := make([]T, 0, end-cursor)
	for _, it := range items[cursor:end] {
		if keep(it) {
			out = append(out, it)
		}
	}
	if end >= uint64(len(items)) {
		return out, "0"
	}
	return out, uintToStr(end)
}
