package shard

// DumpDB calls fn for every live entry of db owned by this shard (table
// stripe s.id == shard id), in bucket order, until fn returns false or
// the stripe is exhausted. Passively expired entries (ExpireAtMs <= now)
// are skipped, matching Redis's "expired keys are not saved" RDB rule.
//
// Call only from inside this shard's goroutine (Engine.DoShard): the
// stripe walk then holds the shard's own stripe read lock while no
// mutation of this shard's data can interleave, so the dump is a
// consistent per-shard point-in-time with no pause (M5c, §13.1).
func (s *Shard) DumpDB(db int, now int64, fn func(key string, e *Entry) bool) {
	if db < 0 || db >= len(s.eng.dbs) {
		return
	}
	s.tab(db).StripeWalk(s.id, func(_ int, it item) bool {
		if it.E.ExpireAtMs > 0 && it.E.ExpireAtMs <= now {
			return true // expired but not yet swept: not saved
		}
		return fn(it.Key, it.E)
	})
}
