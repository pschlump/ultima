# M8 Benchmark Report — 2026-09-23

Milestone M8e: EVAL/EVALSHA throughput vs Redis 7.2.7
(docs/m8-detailed-plan.md). Ultima runs scripts through the R3
per-script VM pool (lib/scripting/pool.go): a hit re-runs on the
bound wasm image (~16-31 µs per-run floor) instead of repaying the
~44 ms wazero instantiation of the pre-M8e S4 fresh-VM lifecycle
(BENCH_M8_POOL_SIZE=0 reproduces that as the control). Redis
compiles once and re-runs the cached function in its shared PUC
state. Guest GC is stopped, so pooled VMs are recycled on run
count and a heap watermark (script_vm_recycle_runs/_pct,
note/m8e-pool.md). What remains vs Redis: the wasm string tax
(~10-20x interp on darwin/arm64 wazero, the worst-case host) and
PauseAll serialization (S3).

## Environment

- Machine: Apple M4 Max, 14 cores, 36 GB RAM (darwin/arm64)
- Ultima: `ultima-server 6830032-dirty (commit 683003230cfeb1e5680e8d7a84c6bd8c8f498891, branch main, built 2026-09-23T17:58:58Z for darwin/arm64)`
- Redis: `Redis server v=7.2.7 sha=00000000:0 malloc=libc bits=64 build=7f24f11dd7e42c58` (via REDIS_BIN=redis-server; `--save '' --appendonly no`)
- Driver: `redis-benchmark 7.2.7`, -n 5000 per run (-c 1 rows: 500)
- Ultima VM pool: script_vm_pool_size=0 (0 = pre-M8e fresh-VM-per-run)

## Results (requests/sec, higher is better)

| Benchmark | Ultima | Redis 7.2.7 |
|---|---|---|
| EVAL return 1,            -c 50 | 203.87 | 116279.06 |
| EVALSHA return 1,         -c 50 | 199.05 | 128205.12 |
| EVAL script+SET,          -c 50 | 198.40 | 121951.22 |
| EVALSHA script+SET,       -c 50 | 198.10 | 111111.11 |
| EVAL script+GET,          -c 50 | 192.52 | 119047.62 |
| EVAL 1k-iter loop,        -c 50 | 187.20 | 83333.34 |
| EVAL return 1,            -c  1 | 22.10 | 10638.30 |
| EVALSHA return 1,         -c  1 | 22.10 | 10638.30 |

Note: redis-benchmark exits on the first error reply; a missing
number means the run errored (see /tmp/ultima-bench-m8.log).
