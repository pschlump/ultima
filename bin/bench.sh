#!/usr/bin/env bash
# bench.sh — M1 benchmark sweep: Ultima vs local redis-server, same
# machine (design doc §14.2 make bench, §14.3 #5). Writes the report to
# docs/benchmarks/M1-<date>.md, then chains into bin/bench-pubsub.sh for
# the M3 pub/sub benchmark (skip with BENCH_PUBSUB=0) and bin/bench-m5.sh
# for the M5 maxmemory soak (skip with BENCH_M5=0).
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_BIN="${REDIS_BIN:-redis-server}"
BENCH_BIN="${BENCH_BIN:-redis-benchmark}"
UPORT="${BENCH_ULTIMA_PORT:-7379}"
RPORT="${BENCH_REDIS_PORT:-7380}"
REQUESTS="${BENCH_REQUESTS:-300000}"
REPORT="docs/benchmarks/M1-$(date +%Y-%m-%d).md"

for p in "$UPORT" "$RPORT"; do
	if nc -z 127.0.0.1 "$p" 2>/dev/null; then
		echo "port $p is in use; set BENCH_ULTIMA_PORT/BENCH_REDIS_PORT" >&2
		exit 1
	fi
done

go build -ldflags "$(sh bin/gen-build-stamp.sh)" -o ./ultima-server ./cmd/ultima-server

CFG=$(mktemp /tmp/ultima-bench-XXXX.json)
cat > "$CFG" <<EOF
{"server":{"resp_addr":"127.0.0.1:$UPORT","grpc_addr":"127.0.0.1:0","http_addr":"127.0.0.1:0","log_level":"error"}}
EOF

./ultima-server --cfg "$CFG" 2>/tmp/ultima-bench.log &
UPID=$!
"$REDIS_BIN" --port "$RPORT" --save '' --appendonly no --logfile /tmp/redis-bench.log --daemonize yes
cleanup() {
	kill "$UPID" 2>/dev/null || true
	redis-cli -p "$RPORT" SHUTDOWN NOSAVE 2>/dev/null || true
	rm -f "$CFG"
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

# extract "N requests per second" (mean) for a run
rps() {
	local port="$1"
	shift
	"$BENCH_BIN" -p "$port" "$@" 2>/dev/null | grep -E 'throughput summary: [0-9.]+ requests per second' | awk '{print $3}' | paste -sd' ' -
}

declare -a ROWS
run_pair() {
	local name="$1"; shift
	local u r
	u=$(rps "$UPORT" "$@")
	r=$(rps "$RPORT" "$@")
	ROWS+=("$name|$u|$r")
}

run_pair "PING inline,  -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t ping_inline
run_pair "PING mbulk,   -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t ping_mbulk
run_pair "PING mbulk,   -c 50 -P  1" -c 50 -P 1 -n "$REQUESTS" -t ping_mbulk
run_pair "SET,          -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t set
run_pair "GET,          -c 50 -P 16" -c 50 -P 16 -n "$REQUESTS" -t get
run_pair "SET,          -c 50 -P  1" -c 50 -P 1 -n "$REQUESTS" -t set
run_pair "GET,          -c 50 -P  1" -c 50 -P 1 -n "$REQUESTS" -t get
run_pair "SET,          -c 200 -P 16" -c 200 -P 16 -n "$REQUESTS" -t set
run_pair "GET,          -c 200 -P 16" -c 200 -P 16 -n "$REQUESTS" -t get

HW_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
HW_CORES=$(sysctl -n hw.ncpu)
MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
UV=$(./ultima-server --version 2>/dev/null || echo unknown)
RV=$("$REDIS_BIN" --version | head -1)
BV=$("$BENCH_BIN" --version 2>/dev/null | head -1 || true)

mkdir -p docs/benchmarks
{
	echo "# M1 Benchmark Report — $(date +%Y-%m-%d)"
	echo
	echo "Milestone M1 exit criterion: first benchmark report vs Redis (design doc §14.4)."
	echo
	echo "## Environment"
	echo
	echo "- Machine: $HW_MODEL, $HW_CORES cores, ${MEM_GB} GB RAM ($(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m))"
	echo "- Ultima: \`$UV\` (shards: 4×GOMAXPROCS, power of two)"
	echo "- Redis: \`$RV\` (via REDIS_BIN=$REDIS_BIN; \`--save '' --appendonly no\`)"
	echo "- Driver: \`$BV\`, -n $REQUESTS per run"
	echo "- Ultima config: \`{\"server\":{\"resp_addr\":\"127.0.0.1:$UPORT\",\"log_level\":\"error\"}}\`"
	echo
	echo "## Headline numbers (requests/sec, higher is better)"
	echo
	echo "| Workload | Ultima | Redis 7.2.7 | Ultima/Redis |"
	echo "|---|---|---|---|"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r name u r <<< "$row"
		ur="${u%% *}"; rr="${r%% *}"
		if [[ -n "$ur" && -n "$rr" && "$rr" != "0" ]]; then
			ratio=$(awk "BEGIN{printf \"%.2f\", $ur/$rr}")
		else
			ratio="n/a"
		fi
		echo "| $name | ${ur:-err} | ${rr:-err} | ${ratio}x |"
	done
	echo
	echo "Numbers are single-run means from redis-benchmark's summary output;"
	echo "rerun \`make bench\` for a fresh sample. Both servers ran on the same"
	echo "loopback interface, sequentially, with no other benchmark load."
} > "$REPORT"

echo "wrote $REPORT"
cat "$REPORT"

# M3 pub/sub section (design doc §14.4 "redis-benchmark pub/sub"). Skip
# with BENCH_PUBSUB=0. Servers from the M1 sweep are torn down first so
# the pub/sub run gets an idle machine and the same ports. The M5
# maxmemory soak (bin/bench-m5.sh) chains after it; skip with BENCH_M5=0.
if [[ "${BENCH_PUBSUB:-1}" != "0" ]]; then
	cleanup
	trap - EXIT
	echo "--- M3 pub/sub section ---"
	sh bin/bench-pubsub.sh
fi
if [[ "${BENCH_M5:-1}" != "0" ]]; then
	echo "--- M5 maxmemory soak section ---"
	sh bin/bench-m5.sh
fi
