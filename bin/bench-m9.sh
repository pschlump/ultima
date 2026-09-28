#!/usr/bin/env bash
# bench-m9.sh — M9a hardened benchmark harness (docs/m9-detailed-plan.md):
# the canonical M9 target-workload matrix (design doc §1.2: GET/SET-heavy
# mixes over RESP, redis-benchmark, 8+ cores). Improvements over bench.sh:
# every row is the MEDIAN of BENCH_REPS repetitions with min–max spread,
# SET/GET rows sweep payload sizes (-d), memtier mixed-workload legs run
# when memtier_benchmark is on PATH (§14.2), and pprof CPU/heap/goroutine
# snapshots of Ultima under load are captured next to the report.
#
# Report: docs/benchmarks/M9-<tag>-<date>.md (tag defaults to "baseline").
# Knobs: BENCH_REPS (3), BENCH_REQUESTS (300000), BENCH_PAYLOADS ("3 64
# 1024"), BENCH_MEMTIER=0, BENCH_MEMTIER_SECS (15), BENCH_PROFILE=0,
# BENCH_M9_TAG, BENCH_ULTIMA_PORT/BENCH_REDIS_PORT/BENCH_HTTP_PORT.
# M9c knobs: BENCH_SHARDS (shard_count written into the bench config;
# default 0 = 4×GOMAXPROCS rounded up to a power of two), BENCH_GOMAXPROCS
# (env passthrough to ultima-server; default ncpu), BENCH_DRIVERS (1; N
# parallel redis-benchmark processes per row, throughputs summed — breaks
# the ~1-1.9M ops/s single-driver ceiling), BENCH_CONNS_SWEEP (1; 0 skips
# the -c 100/-c 400 regression-curve rows).
# Battery gate (ported from bench-m8.sh, same BENCH_M8_BATTERY_* knobs):
# between rows the battery is checked with the sibling battery-check tool
# — below BENCH_M8_BATTERY_LOW (50) the run pauses until the charge
# recovers to BENCH_M8_BATTERY_HIGH (60). No tool or no battery = off.
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_BIN="${REDIS_BIN:-redis-server}"
BENCH_BIN="${BENCH_BIN:-redis-benchmark}"
UPORT="${BENCH_ULTIMA_PORT:-7379}"
RPORT="${BENCH_REDIS_PORT:-7380}"
HPORT="${BENCH_HTTP_PORT:-7381}"
REQUESTS="${BENCH_REQUESTS:-300000}"
REPS="${BENCH_REPS:-3}"
PAYLOADS="${BENCH_PAYLOADS:-3 64 1024}"
DRIVERS="${BENCH_DRIVERS:-1}"
TAG="${BENCH_M9_TAG:-baseline}"

# Battery gate setup (see header comment; identical semantics to bench-m8.sh).
BATTERY_LOW="${BENCH_M8_BATTERY_LOW:-50}"
BATTERY_HIGH="${BENCH_M8_BATTERY_HIGH:-60}"
BATTERY_POLL="${BENCH_M8_BATTERY_POLL:-60}"
BATTERY_CHECK="${BENCH_M8_BATTERY_CHECK:-}"
if [[ -z "$BATTERY_CHECK" ]]; then
	if [[ -x ../battery-check/battery-check ]]; then
		BATTERY_CHECK=../battery-check/battery-check
	elif command -v battery-check >/dev/null 2>&1; then
		BATTERY_CHECK=battery-check
	fi
fi

# battery_gate blocks while the battery is below BATTERY_LOW until it
# recovers to BATTERY_HIGH. Tool errors (exit 2) disable gating rather
# than stall the run; exit 1 = below level.
battery_gate() {
	[[ -n "$BATTERY_CHECK" ]] && [[ -x "$BATTERY_CHECK" || -n "$(command -v "$BATTERY_CHECK" 2>/dev/null)" ]] || return 0
	local out rc
	if out=$("$BATTERY_CHECK" --level "$BATTERY_LOW" 2>&1); then
		return 0
	else
		rc=$? # must be captured in the else: after fi, $? is the if's own status (0)
	fi
	if (( rc != 1 )); then
		echo "battery-check failed (rc=$rc): $out — battery gating disabled" >&2
		BATTERY_CHECK=""
		return 0
	fi
	echo "$(date +%H:%M:%S) battery low: $out — pausing until ${BATTERY_HIGH}%"
	while :; do
		sleep "$BATTERY_POLL"
		if out=$("$BATTERY_CHECK" --level "$BATTERY_HIGH" 2>&1); then
			echo "$(date +%H:%M:%S) battery recovered: $out — resuming"
			return 0
		else
			rc=$?
		fi
		if (( rc != 1 )); then
			echo "battery-check failed (rc=$rc): $out — resuming without gating" >&2
			BATTERY_CHECK=""
			return 0
		fi
	done
}
REPORT="${BENCH_M9_REPORT:-docs/benchmarks/M9-${TAG}-$(date +%Y-%m-%d).md}"
PDIR="docs/benchmarks/profiles/$(date +%Y-%m-%d)-${TAG}"

for p in "$UPORT" "$RPORT" "$HPORT"; do
	if nc -z 127.0.0.1 "$p" 2>/dev/null; then
		echo "port $p is in use; set BENCH_ULTIMA_PORT/BENCH_REDIS_PORT/BENCH_HTTP_PORT" >&2
		exit 1
	fi
done

go build -ldflags "$(sh bin/gen-build-stamp.sh)" -o ./ultima-server ./cmd/ultima-server

CFG=$(mktemp /tmp/ultima-bench-m9-XXXX.json)
DDIR=$(mktemp -d /tmp/ultima-bench-m9-data-XXXX)
# pprof_enabled: the M9a profile-capture endpoint (server.pprof_enabled).
# shard_count 0 = the default resolution (4×GOMAXPROCS, power of two).
SHARDS="${BENCH_SHARDS:-0}"
cat > "$CFG" <<EOF
{"server":{"resp_addr":"127.0.0.1:$UPORT","grpc_addr":"127.0.0.1:0","http_addr":"127.0.0.1:$HPORT","pprof_enabled":true,"log_level":"error","shard_count":$SHARDS},"persist":{"dir":"$DDIR","save":""}}
EOF

# Resolved shard count for the report preamble (mirrors
# shard.ResolveShardCount: 4×GOMAXPROCS rounded up to a power of two).
GOMAXPROCS_VAL="${BENCH_GOMAXPROCS:-$(sysctl -n hw.ncpu)}"
if [[ "$SHARDS" != "0" ]]; then
	RESOLVED_SHARDS="$SHARDS"
else
	p=1
	while (( p < 4 * GOMAXPROCS_VAL )); do p=$((p * 2)); done
	RESOLVED_SHARDS=$p
fi

GOMAXPROCS="$GOMAXPROCS_VAL" ./ultima-server --cfg "$CFG" 2>/tmp/ultima-bench-m9.log &
UPID=$!
"$REDIS_BIN" --port "$RPORT" --save '' --appendonly no --logfile /tmp/redis-bench-m9.log --daemonize yes
cleanup() {
	kill "$UPID" 2>/dev/null || true
	redis-cli -p "$RPORT" SHUTDOWN NOSAVE 2>/dev/null || true
	rm -f "$CFG"
	rm -rf "$DDIR"
}
trap cleanup EXIT

wait_port() {
	for _ in $(seq 1 100); do
		nc -z 127.0.0.1 "$1" 2>/dev/null && return 0
		sleep 0.05
	done
	echo "server on port $1 did not start" >&2
	exit 1
}
wait_port "$UPORT"
wait_port "$RPORT"
wait_port "$HPORT"

# rps port args... → median/min/max requests-per-sec over REPS runs.
# With BENCH_DRIVERS > 1 each rep runs that many redis-benchmark
# processes concurrently against the same server and sums their
# throughputs — a single driver process caps ~1-1.9M ops/s itself, which
# would hide the server's real ceiling (M9c).
rps() {
	local port="$1"
	shift
	# One rep: DRIVERS concurrent benchmarks, throughput summed. Retried
	# with backoff when a driver yields nothing — with DRIVERS>1 plus the
	# -c 200/-c 400 rows, TIME_WAIT accumulation can exhaust the ephemeral
	# port range ("Can't assign requested address"); a pause lets it drain.
	run_rep() {
		local attempt
		for attempt in 1 2 3; do
			local -a pids=()
			for d in $(seq 1 "$DRIVERS"); do
				"$BENCH_BIN" -p "$port" "$@" 2>"/tmp/bench-m9-drv-err-$$-$d" \
					| grep -E 'throughput summary: [0-9.]+ requests per second' \
					| awk '{print $3}' > "/tmp/bench-m9-drv-$$-$d" &
				pids+=($!)
			done
			for p in "${pids[@]}"; do wait "$p"; done
			total=0
			local empty=0
			for d in $(seq 1 "$DRIVERS"); do
				v=$(cat "/tmp/bench-m9-drv-$$-$d" 2>/dev/null)
				if [[ -z "$v" ]]; then
					empty=1
					if (( attempt == 3 )); then
						echo "rps: empty driver output (port $port, driver $d, args: $*); driver stderr:" >&2
						head -3 "/tmp/bench-m9-drv-err-$$-$d" >&2
					fi
					v=0
				fi
				total=$(awk "BEGIN{print $total + $v}")
				rm -f "/tmp/bench-m9-drv-$$-$d" "/tmp/bench-m9-drv-err-$$-$d"
			done
			if (( empty == 0 )); then
				return 0
			fi
			echo "rps: port $port args [$*] attempt $attempt failed (driver connect error); backing off 15s" >&2
			sleep 15
		done
	}
	local vals=""
	local _rep d p v total
	for _rep in $(seq 1 "$REPS"); do
		run_rep "$@"
		vals="$vals $total"
	done
	# shellcheck disable=SC2086
	printf '%s\n' $vals | sort -n | awk '{a[NR]=$1} END{if(NR%2) med=a[(NR+1)/2]; else med=(a[NR/2]+a[NR/2+1])/2; printf "%.2f %.2f %.2f", med, a[1], a[NR]}'
}

declare -a ROWS
run_pair() {
	local name="$1"; shift
	battery_gate
	local u r
	u=$(rps "$UPORT" "$@")
	r=$(rps "$RPORT" "$@")
	ROWS+=("$name|$u|$r")
}

seed() { # seed port payload — make GET rows hit a real value of size $2
	local payload
	payload=$(head -c "$2" /dev/zero | tr '\0' 'x')
	redis-cli -p "$1" SET key:000000000000 "$payload" >/dev/null
}

run_pair "PING inline, -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t ping_inline
run_pair "PING mbulk,  -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t ping_mbulk
run_pair "PING mbulk,  -c 50 -P  1" -c 50 -P 1 -n "$REQUESTS" -t ping_mbulk

for d in $PAYLOADS; do
	seed "$UPORT" "$d"; seed "$RPORT" "$d"
	run_pair "SET -d $d, -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t set -d "$d"
	run_pair "GET -d $d, -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t get -d "$d"
	run_pair "SET -d $d, -c 50 -P  1" -c 50 -P 1 -n "$REQUESTS" -t set -d "$d"
	run_pair "GET -d $d, -c 50 -P  1" -c 50 -P 1 -n "$REQUESTS" -t get -d "$d"
	run_pair "SET -d $d, -c 200 -P 16" -c 200 -P 16 -n "$REQUESTS" -t set -d "$d"
	run_pair "GET -d $d, -c 200 -P 16" -c 200 -P 16 -n "$REQUESTS" -t get -d "$d"
done

# M9c regression-curve rows: the ratio worsens from -c50 to -c200; -c100
# and -c400 fill in the curve. Skip with BENCH_CONNS_SWEEP=0.
if [[ "${BENCH_CONNS_SWEEP:-1}" != "0" ]]; then
	d="${PAYLOADS%% *}" # first payload keeps the sweep cheap
	seed "$UPORT" "$d"; seed "$RPORT" "$d"
	run_pair "SET -d $d, -c 100 -P 16" -c 100 -P 16 -n "$REQUESTS" -t set -d "$d"
	run_pair "GET -d $d, -c 100 -P 16" -c 100 -P 16 -n "$REQUESTS" -t get -d "$d"
	run_pair "SET -d $d, -c 400 -P 16" -c 400 -P 16 -n "$REQUESTS" -t set -d "$d"
	run_pair "GET -d $d, -c 400 -P 16" -c 400 -P 16 -n "$REQUESTS" -t get -d "$d"
fi

# Randomized-key rows (M9b): shard parallelism is the design's lever; the
# fixed-key rows above measure the single-shard path. SET populates the
# -r keyspace, so the paired GET rows are hits.
run_pair "SET -d 64 -r 1M, -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t set -d 64 -r 1000000
run_pair "GET -d 64 -r 1M, -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t get -d 64 -r 1000000
run_pair "SET -d 64 -r 1M, -c 200 -P 16" -c 200 -P 16 -n "$REQUESTS" -t set -d 64 -r 1000000
run_pair "GET -d 64 -r 1M, -c 200 -P 16" -c 200 -P 16 -n "$REQUESTS" -t get -d 64 -r 1000000

# memtier mixed-workload legs (§14.2 "redis-benchmark + memtier"); skipped
# gracefully when memtier_benchmark is not installed.
declare -a MT_ROWS
MEMTIER_NOTE="memtier_benchmark not found on PATH; mixed-workload legs skipped."
if [[ "${BENCH_MEMTIER:-1}" != "0" ]] && command -v memtier_benchmark >/dev/null 2>&1; then
	MT_SECS="${BENCH_MEMTIER_SECS:-15}"
	mt() { # mt port ratio → Totals ops/sec
		memtier_benchmark -s 127.0.0.1 -p "$1" --ratio="$2" --test-time="$MT_SECS" \
			--clients=50 --threads=4 --pipeline=16 --key-pattern=R:R --hide-histogram 2>/dev/null \
			| awk '/^Totals/ {print $2}' | tail -1
	}
	for ratio in 1:1 1:10; do
		u=$(mt "$UPORT" "$ratio")
		r=$(mt "$RPORT" "$ratio")
		MT_ROWS+=("SET:GET $ratio, -c 50 -P 16, ${MT_SECS}s|$u|$r")
	done
	MEMTIER_NOTE="memtier_benchmark $(memtier_benchmark --version 2>&1 | head -1), --test-time=$MT_SECS --clients=50 --threads=4 --pipeline=16 --key-pattern=R:R."
fi

# pprof capture (M9a): Ultima under sustained SET load; skip with
# BENCH_PROFILE=0. Requires pprof_enabled in the bench config above.
PROFILE_NOTE="profile capture disabled (BENCH_PROFILE=0)."
if [[ "${BENCH_PROFILE:-1}" != "0" ]]; then
	mkdir -p "$PDIR"
	"$BENCH_BIN" -p "$UPORT" -c 50 -P 16 -n 50000000 -t set -d 64 -r 1000000 >/dev/null 2>&1 &
	LOADPID=$!
	sleep 0.5
	curl -s "http://127.0.0.1:$HPORT/debug/pprof/profile?seconds=10" > "$PDIR/cpu.pprof" || true
	curl -s "http://127.0.0.1:$HPORT/debug/pprof/heap" > "$PDIR/heap.pprof" || true
	curl -s "http://127.0.0.1:$HPORT/debug/pprof/goroutine?debug=1" > "$PDIR/goroutine.txt" || true
	# M9c: contention + scheduler evidence (enabled under pprof_enabled in
	# cmd/ultima-server/startup.go).
	curl -s "http://127.0.0.1:$HPORT/debug/pprof/mutex" > "$PDIR/mutex.pprof" || true
	curl -s "http://127.0.0.1:$HPORT/debug/pprof/block" > "$PDIR/block.pprof" || true
	curl -s "http://127.0.0.1:$HPORT/debug/pprof/trace?seconds=5" > "$PDIR/trace.out" || true
	kill "$LOADPID" 2>/dev/null || true
	wait "$LOADPID" 2>/dev/null || true
	if [[ -s "$PDIR/cpu.pprof" ]]; then
		PROFILE_NOTE="captured under SET -c 50 -P 16 -d 64 load: \`$PDIR/{cpu.pprof,heap.pprof,mutex.pprof,block.pprof,goroutine.txt,trace.out}\` (view: \`go tool pprof -http=:0 $PDIR/cpu.pprof\`, \`go tool trace $PDIR/trace.out\`)."
	else
		PROFILE_NOTE="capture attempted but $PDIR/cpu.pprof is empty — is pprof_enabled on?"
	fi
fi

HW_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
HW_CORES=$(sysctl -n hw.ncpu)
MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
UV=$(./ultima-server --version 2>/dev/null || echo unknown)
RV=$("$REDIS_BIN" --version | head -1)
BV=$("$BENCH_BIN" --version 2>/dev/null | head -1 || true)

ratio_of() { # $1 ultima median, $2 redis median
	if [[ -n "$1" && -n "$2" && "$2" != "0" && "$2" != "0.00" ]]; then
		awk "BEGIN{printf \"%.2f\", $1/$2}"
	else
		echo "n/a"
	fi
}

mkdir -p docs/benchmarks
{
	echo "# M9 Benchmark Report (${TAG}) — $(date +%Y-%m-%d)"
	echo
	echo "M9 harness (docs/m9-detailed-plan.md M9a): canonical target-workload"
	echo "matrix (§1.2 GET/SET-heavy over RESP). Every cell is the median of"
	echo "$REPS runs (min–max in parentheses). M9 gate: every GET/SET row ≥4×"
	echo "Redis, median of ≥3 runs."
	echo
	echo "## Environment"
	echo
	echo "- Machine: $HW_MODEL, $HW_CORES cores, ${MEM_GB} GB RAM ($(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m))"
	echo "- Ultima: \`$UV\` (shards: $RESOLVED_SHARDS, GOMAXPROCS: $GOMAXPROCS_VAL)"
	echo "- Redis: \`$RV\` (via REDIS_BIN=$REDIS_BIN; \`--save '' --appendonly no\`)"
	echo "- Driver: \`$BV\`, -n $REQUESTS per run, $REPS reps, $DRIVERS parallel driver(s) per rep"
	echo "- memtier: $MEMTIER_NOTE"
	echo "- pprof: $PROFILE_NOTE"
	echo "- Ultima config: \`{\"server\":{\"resp_addr\":\"127.0.0.1:$UPORT\",\"http_addr\":\"127.0.0.1:$HPORT\",\"pprof_enabled\":true,\"log_level\":\"error\",\"shard_count\":$SHARDS},\"persist\":{\"dir\":\"<tmp>\",\"save\":\"\"}\}\`"
	echo
	echo "## Target-workload matrix (requests/sec, median of $REPS)"
	echo
	echo "| Workload | Ultima (min–max) | Redis 7.2.7 (min–max) | Ultima/Redis |"
	echo "|---|---|---|---|"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r name u r <<< "$row"
		read -r umed umin umax <<< "$u"
		read -r rmed rmin rmax <<< "$r"
		echo "| $name | ${umed:-err} (${umin}–${umax}) | ${rmed:-err} (${rmin}–${rmax}) | $(ratio_of "$umed" "$rmed")x |"
	done
	if [[ ${#MT_ROWS[@]} -gt 0 ]]; then
		echo
		echo "## memtier mixed workloads (ops/sec, single ${MT_SECS}s leg)"
		echo
		echo "| Workload | Ultima | Redis 7.2.7 | Ultima/Redis |"
		echo "|---|---|---|---|"
		for row in "${MT_ROWS[@]}"; do
			IFS='|' read -r name u r <<< "$row"
			echo "| $name | ${u:-err} | ${r:-err} | $(ratio_of "$u" "$r")x |"
		done
	fi
	echo
	echo "Both servers ran on the same loopback interface, sequentially, with"
	echo "no other benchmark load. GET rows are pre-seeded hits on a single"
	echo "key of the stated payload size; PING rows carry no payload."
} > "$REPORT"

echo "wrote $REPORT"
cat "$REPORT"
