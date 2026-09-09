package commands

import (
	"strings"
	"testing"
)

// TestGlobMatchRedisSemantics pins GlobMatch to Redis 7.2.7 stringmatchlen
// (note/redis/src/util.c) for the cases where the two agree — which is
// everything except the unterminated-class and star-vs-empty corners
// covered by TestGlobMatchRedisDivergences. Every expectation here was
// verified against the verbatim C function.
func TestGlobMatchRedisSemantics(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		// '*' — any run, including empty.
		{"*", "a", true},
		{"*", " ", true},
		{"**", "ab", true}, // consecutive stars collapse
		{"a*", "a", true},
		{"a*", "ab", true},
		{"a*", "b", false},
		{"*a", "ba", true},
		{"*a", "b", false},
		{"a*b", "ab", true},
		{"a*b", "acb", true},
		{"a*b", "a", false},
		{"a*b", "ac", false},
		{"*a*", "ba", true},
		{"*a*", "bb", false},
		// '?' — exactly one byte.
		{"?", "a", true},
		{"??", "a", false},
		{"a?c", "abc", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		// Classes.
		{"[abc]", "b", true},
		{"[abc]", "d", false},
		{"[^abc]", "d", true},
		{"[^abc]", "b", false},
		// '^' is the only negation byte; '!' is a literal member
		// (fnmatch-style [!abc] is NOT Redis semantics).
		{"[!abc]", "!", true},
		{"[!abc]", "a", true},
		{"[!abc]", "d", false},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hello", false},
		{"h[^e]llo", "hallo", true},
		// Ranges; reversed endpoints are swapped, like Redis.
		{"[a-z]", "m", true},
		{"[a-z]", "A", false}, // case sensitive
		{"[z-a]", "m", true},
		{"[a-c]x", "bx", true},
		{"[a-c]x", "dx", false},
		// '-' as first/last member is literal.
		{"[-a]", "-", true},
		{"[-a]", "a", true},
		{"[-a]", "b", false},
		{"[a-c-]", "-", true},
		// Empty class never matches; negated empty class matches
		// any single byte.
		{"[]", "]", false},
		{"[]", "", false},
		{"[^]", "x", true},
		{"[^]", "", false},
		// Escapes: '\x' is the literal byte x, including for the
		// glob metacharacters; a trailing lone '\' is literal too.
		{"a\\*b", "a*b", true},
		{"a\\*b", "axb", false},
		{"a\\?", "a?", true},
		{"\\*", "*", true},
		{"\\*", "x", false},
		{"\\", "\\", true},
		{"a\\", "a\\", true},
		{"[\\]]", "]", true},
		{"[\\]]", "\\", false},
		// Unterminated classes never match in GlobMatch; the cases
		// where Redis agrees are the non-matching ones.
		{"[a", "b", false},
		{"[a", "", false},
		{"[ab", "ab", false},
		{"[", "a", false},
		// Byte-oriented: '?' is one byte, not one rune.
		{"h?llo", "héllo", false}, // é is 2 bytes
		{"h??llo", "héllo", true},
		{"*", "héllo", true},
		// High bytes pass through byte-wise.
		{"\xff", "\xff", true},
		{"?", "\xc3", true},
		{"[\x80-\xff]", "\xc3", true},
		{"[\x80-\xff]", "z", false},
		// Empty pattern / empty string.
		{"", "", true},
		{"", "a", false},
		// The __keyspace@/__keyevent@ channels M5a publishes on.
		{"__keyevent@0__:*", "__keyevent@0__:expired", true},
		{"__keyevent@0__:*", "__keyevent@1__:expired", false},
		{"__keyspace@*__:mykey", "__keyspace@12__:mykey", true},
	}
	for _, tc := range cases {
		if got := GlobMatch([]byte(tc.pat), []byte(tc.s)); got != tc.want {
			t.Errorf("GlobMatch(%q,%q) = %v, want %v", tc.pat, tc.s, got, tc.want)
		}
	}
}

// TestGlobMatchRedisDivergences encodes the corners where GlobMatch
// deliberately/actually differs from Redis 7.2.7 stringmatchlen; each
// case documents the true Redis result (verified against the verbatim C)
// so the divergence is a choice visible in the test, not a silent bug.
func TestGlobMatchRedisDivergences(t *testing.T) {
	cases := []struct {
		pat, s string
		got    bool // GlobMatch's actual result
		redis  bool // stringmatchlen 7.2.7
	}{
		// stringmatchlen's main loop is `while(patternLen && stringLen)`,
		// so a star-only pattern never matches the empty string. (KEYS *
		// still returns empty-named keys, but via the `allkeys` shortcut
		// in keysCommand, not the matcher.) GlobMatch reports match.
		{"*", "", true, false},
		{"**", "", true, false},
		{"***", "", true, false},
		// Redis treats a class that runs to the end of the pattern as if
		// it were closed (its inner loop backs up one byte and the main
		// loop tail consumes it), so `[a` ≡ `[a]`. GlobMatch returns
		// false for any unterminated class.
		{"[a", "a", false, true},
		{"[ab", "a", false, true},
		{"[^a", "b", false, true},
		// Same via a range whose endpoint is ']': Redis's range branch
		// consumes the ']' as the endpoint and then the class "closes"
		// at pattern end, making `[a-]` a one-byte class over the
		// (swapped) range ']'..'a'. GlobMatch hits end-of-pattern
		// mid-class and fails.
		{"[a-]", "a", false, true},
		{"[a-]", "]", false, true},
		{"[a-]x", "x", false, true},
		// Same via an escaped ']': Redis's `[\]` is a class matching
		// "]"; GlobMatch fails on the unterminated class.
		{"[\\]", "]", false, true},
		{"[a\\]", "a", false, true},
		{"[a\\]", "]", false, true},
	}
	for _, tc := range cases {
		if got := GlobMatch([]byte(tc.pat), []byte(tc.s)); got != tc.got {
			t.Errorf("GlobMatch(%q,%q) = %v, want %v (divergence case; Redis=%v)",
				tc.pat, tc.s, got, tc.got, tc.redis)
		}
	}
}

// TestGlobMatchDeepNesting documents that GlobMatch has no equivalent of
// stringmatchlen's nesting > 1000 abuse guard: Redis answers "no match"
// for this pattern purely because of the guard, GlobMatch answers true.
// (Deep star recursion is also a potential CPU/stack DoS vector that the
// guard exists to bound.)
func TestGlobMatchDeepNesting(t *testing.T) {
	pat := strings.Repeat("*a", 1100)
	s := strings.Repeat("a", 1100)
	if !GlobMatch([]byte(pat), []byte(s)) {
		t.Errorf("GlobMatch(1100x\"*a\", 1100x\"a\") = false, want true (Redis: false via nesting guard)")
	}
}
