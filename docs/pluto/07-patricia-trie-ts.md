# Pluto Request 07 — Thread-safe Patricia trie + glob/prefix enhancements

**Type:** New package `patricia_trie_ts/`; additive changes to `patricia_trie/`
**Priority:** P3
**Blocks:** efficient keyspace prefix operations, stream ID indexing,
potential radix-tree keyspace encoding

## Context

Ultima's keyspace may use a radix structure for prefix-heavy workloads (Redis 7+
keeps its keyspace in dicts but streams use `rax.c`; Valkey/KeyDB variants use
radix trees for the keyspace). Pluto has `patricia_trie/` (crit-bit trie with
path compression, `KeysWithPrefix`, `All`/`Backward`) but no thread-safe twin,
and it lacks the glob matching that `trie`/`tst` offer (`KeysThatMatch`).

## Requirements

### 1. `patricia_trie_ts` — standard `_ts` twin

- Identical API to `patricia_trie`, internal `sync.RWMutex`, snapshot
  iterators, `Lock()`/`Unlock()` + `Nl*` variants (same pattern as
  `trie_ts`/`tst_ts`).

### 2. Additive API on both packages

Verify which of these already exist; add the missing ones:

```go
func (t *PatriciaTrie[T]) LongestPrefixOf(key string) (string, T, bool)
func (t *PatriciaTrie[T]) KeysThatMatch(pattern string) iter.Seq2[string, T] // glob: * ? [...] \x
func (t *PatriciaTrie[T]) Delete(key string) bool
```

- `KeysThatMatch` glob semantics must match Redis's `stringmatchlen`
  (`note/redis/src/util.c`): `*`, `?`, `[abc]`, `[a-z]`, `[^...]`, backslash
  escaping. This is the same semantics the KEYS command uses, so exactness
  matters — port the C algorithm's behavior including edge cases
  (`\*`, `[` unterminated treated literally, etc.).
- All iterators range-over-func, early-break safe.

### 3. Performance

- `LongestPrefixOf`, `KeysWithPrefix`: O(key length + results).
- `KeysThatMatch`: document expected cost (full descent with pruning where the
  pattern's literal prefix allows).
- Benchmark: 1M random 16-byte keys; measure Insert/Search/KeysWithPrefix
  vs `tst` and `trie` for comparison table in the package docs.

### 4. Edge cases

- Empty pattern / empty key.
- Pattern matching nothing / everything (`*`).
- Binary-safe keys (any bytes except none — document whether NUL is allowed;
  Redis keys are binary-safe).
- Delete of prefix-shared nodes: path re-compression correctness (this is the
  classic crit-bit Delete bug farm — test heavily).

### 5. Tests

- Oracle tests vs `map[string]T` + brute-force glob (reference implementation of
  the glob in the test file, ported from `stringmatchlen`).
- Randomized insert/delete stress with re-compression checks (trie invariants:
  no unary internal nodes after Delete).
- `_ts` twin: race-detector concurrent test.

## References

- Pluto: `patricia_trie/`, `trie_ts/`, `tst_ts/` (twin pattern)
- Redis: `note/redis/src/rax.c` (design ideas), `note/redis/src/util.c`
  (`stringmatchlen`)
