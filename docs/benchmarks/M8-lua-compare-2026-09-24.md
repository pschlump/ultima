# Lua backend comparison — 2026-09-24

Experiment branch `gopher_lua_original`: original pure-Go
gopher-lua (vendored third_party/gopher-lua + pure-Go host shim)
vs `main`'s wasm backend (gopher-lua compiled to wasm on wazero,
R3 per-script VM pool). Same 4 short chunks as bin/bench-m8.sh,
-n 50000 per -c 50 leg (-c 1 leg: 5000).

| Config | Leg | rps | peak RSS MB |
|---|---|---|---|
| main-wasm-pool1 | EVAL return 1  -c50 | 2017.11 | 1737 |
| main-wasm-pool1 | EVAL script+SET  -c50 | 5784.36 | 1788 |
| main-wasm-pool1 | EVAL script+GET  -c50 | 4777.37 | 1847 |
| main-wasm-pool1 | EVAL 1k-iter loop  -c50 | 1084.53 | 1833 |
| main-wasm-pool1 | EVAL return 1  -c 1 | 1769.91 | 1793 |
| main-wasm-pool1 | END-OF-RUN RSS |   | 1790 |
| main-wasm-pool1 | idle RSS at startup |   | 28 |
| main-wasm-norecycle | EVAL return 1  -c50 | 9954.21 | 1600 |
| main-wasm-norecycle | EVAL script+SET  -c50 | 9595.09 | 1782 |
| main-wasm-norecycle | EVAL script+GET  -c50 | 8235.88 | 1826 |
| main-wasm-norecycle | EVAL 1k-iter loop  -c50 | 1119.77 | 1829 |
| main-wasm-norecycle | EVAL return 1  -c 1 | 13440.86 | 1782 |
| main-wasm-norecycle | END-OF-RUN RSS |   | 1781 |
| main-wasm-norecycle | idle RSS at startup |   | 27 |
| orig-purego-pool1 | EVAL return 1  -c50 | 19762.85 | 66 |
| orig-purego-pool1 | EVAL script+SET  -c50 | 18248.18 | 67 |
| orig-purego-pool1 | EVAL script+GET  -c50 | 18946.57 | 68 |
| orig-purego-pool1 | EVAL 1k-iter loop  -c50 | 13231.01 | 69 |
| orig-purego-pool1 | EVAL return 1  -c 1 | 14836.79 | 69 |
| orig-purego-pool1 | END-OF-RUN RSS |   | 69 |
| orig-purego-pool1 | idle RSS at startup |   | 27 |
| orig-purego-pool0 | EVAL return 1  -c50 | 16005.12 | 65 |
| orig-purego-pool0 | EVAL script+SET  -c50 | 15370.43 | 65 |
| orig-purego-pool0 | EVAL script+GET  -c50 | 15556.94 | 65 |
| orig-purego-pool0 | EVAL 1k-iter loop  -c50 | 12001.92 | 66 |
| orig-purego-pool0 | EVAL return 1  -c 1 | 8620.69 | 66 |
| orig-purego-pool0 | END-OF-RUN RSS |   | 66 |
| orig-purego-pool0 | idle RSS at startup |   | 27 |
| redis-7.2.7 | EVAL return 1  -c50 | 189393.94 | 0 |
| redis-7.2.7 | EVAL script+SET  -c50 | 173611.12 | 0 |
| redis-7.2.7 | EVAL script+GET  -c50 | 183150.19 | 0 |
| redis-7.2.7 | EVAL 1k-iter loop  -c50 | 96339.12 | 0 |
| redis-7.2.7 | EVAL return 1  -c 1 | 13927.58 | 0 |

## Analysis

Machine: darwin/arm64. Harness: `bin/bench-lua-compare.sh` (RSS sampled
at 20 Hz via `ps` during each leg). `main-wasm-pool1` is the shipping
default (pool size 1, recycle after 100 runs / 75% heap watermark);
`main-wasm-norecycle` disables recycling to isolate the wasm backend's
best case; `orig-purego-pool1/pool0` are this branch with the pool on/off.

**Speed — pure-Go wins every leg:**

| Leg -c50 | wasm (shipping) | wasm (no recycle) | pure-Go pool1 | pure-Go vs wasm shipping |
|---|---|---|---|---|
| EVAL return 1 | 2,017 | 9,954 | 19,763 | **9.8x** |
| EVAL script+SET | 5,784 | 9,595 | 18,248 | **3.2x** |
| EVAL script+GET | 4,777 | 8,236 | 18,947 | **4.0x** |
| EVAL 1k-iter loop | 1,085 | 1,120 | 13,231 | **12.2x** |
| EVAL return 1 -c1 | 1,770 | 13,441 | 14,837 | **8.4x** |

- The shipping wasm numbers are dominated by the recycling policy: with
  `script_vm_recycle_runs=100`, one pooled VM in 100 pays the ~44 ms
  wazero instantiation, an amortized ~440 µs/run floor. Disabling
  recycling lifts wasm to ~8-13k rps — but is unsafe there (guest GC is
  stopped, heaps grow monotonically), while on the pure-Go backend
  recycling is a cheap fresh `lua.LState`.
- Even against wasm's best case, pure-Go is ~2x faster on the
  redis.call legs and ~12x on the CPU loop (the "wasm string tax" +
  interpreter-crossing overhead).
- At -c 1 the pure-Go branch (14.8k rps) edges past Redis 7.2.7 itself
  (13.9k rps). At -c 50 Redis's ~190k remains out of reach for both
  backends — that ceiling is PauseAll serialization (S3), not the Lua
  engine.

**Memory — the bigger win:**

| Config | idle RSS | peak RSS | end RSS |
|---|---|---|---|
| main-wasm-pool1 | 28 MB | ~1,793 MB | 1,790 MB |
| main-wasm-norecycle | 27 MB | ~1,829 MB | 1,781 MB |
| orig-purego-pool1 | 27 MB | **69 MB** | 69 MB |
| orig-purego-pool0 | 27 MB | **66 MB** | 66 MB |

The wasm backend holds ~1.8 GB after ~250k short script runs regardless
of the recycling policy (each VM owns a wazero runtime + linear memory;
the Go GC does not reclaim them promptly). The pure-Go backend plateaus
at ~68 MB — **~26x less** — and is flat from the first leg on.

**Pool necessity on this branch:** pooling still helps (+23% at -c 50,
+72% at -c 1) because a fresh LState costs ~40 µs of stdlib setup, but
pool0 is now a viable configuration — unlike the wasm backend, where
pool0 pays 44 ms per run (~200 rps).

## Correctness gates on this branch (all green)

`go test ./...` (incl. `lib/scripting`, `lib/commands`, M8 integration:
BUSY/SCRIPT KILL/UNKILLABLE/hard deadline/memory cap), the byte-exact
differential suite vs live redis-server 7.2.7 (`tests/differential`,
M8 corpus included), cli-matrix (1846 passed / 0 failed), and the
vendored module's own upstream tests. Known divergences from the wasm
backend: coroutines now work (closer to Redis), the memory budget is
enforced by approximate monotonic accounting (concat/rep/table/closure
allocation sites), and `table.insert` into a readonly table bypasses the
guard (not in any gate; see third_party/gopher-lua/host docs).
