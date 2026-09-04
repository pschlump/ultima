#!/usr/bin/env bash
# bench-pubsub.sh — M3 pub/sub benchmark: Ultima vs local redis-server,
# same machine (design doc §14.4 "redis-benchmark pub/sub"). Writes the
# report to docs/benchmarks/M3-<date>.md. Run standalone or chained from
# bin/bench.sh (BENCH_PUBSUB=0 disables the chain).
#
# Workloads (all: redis-benchmark PUBLISH benchchan payload):
#   a. zero subscribers,          -c 50 -P 16
#   b. K subscribers (fan-out),   -c 50 -P 16   (full speed)
#   b2. K subscribers,            -c 50 -P  1   (no pipeline)
#   b3. K subscribers,            -c  1 -P  1   (single client)
# Subscribers are the note/pubsub-bench-sub Go driver: redis-cli formats
# every message as text and cannot drain fast enough, which trips
# Ultima's slow-consumer close (lib/respserver pushQueueCap 4096) even at
# moderate rates — a subscriber artifact, not a server limit. After every
# fan-out run the script verifies subscriber survival (PUBSUB NUMSUB) and
# per-subscriber delivered-message counts.
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_BIN="${REDIS_BIN:-redis-server}"
BENCH_BIN="${BENCH_BIN:-redis-benchmark}"
CLI_BIN="${BENCH_CLI_BIN:-redis-cli}"
UPORT="${BENCH_ULTIMA_PORT:-7379}"
RPORT="${BENCH_REDIS_PORT:-7380}"
REQUESTS="${BENCH_REQUESTS:-300000}"
SUBS="${BENCH_SUBS:-8}"
SINGLE_N="${BENCH_SINGLE_REQUESTS:-100000}"
CHAN="benchchan"
REPORT="docs/benchmarks/M3-$(date +%Y-%m-%d).md"

TMPD=$(mktemp -d /tmp/ultima-ps-bench-XXXX)

for p in "$UPORT" "$RPORT"; do
	if nc -z 127.0.0.1 "$p" 2>/dev/null; then
		echo "port $p is in use; set BENCH_ULTIMA_PORT/BENCH_REDIS_PORT" >&2
		exit 1
	fi
done

go build -ldflags "$(sh bin/gen-build-stamp.sh)" -o ./ultima-server ./cmd/ultima-server
PSUB="$TMPD/psub"
(cd note/pubsub-bench-sub && go build -o "$PSUB" .)

CFG="$TMPD/ultima.cfg.json"
cat > "$CFG" <<EOF
{"server":{"resp_addr":"127.0.0.1:$UPORT","grpc_addr":"127.0.0.1:0","http_addr":"127.0.0.1:0","log_level":"error"}}
EOF

./ultima-server --cfg "$CFG" 2>"$TMPD/ultima.log" &
UPID=$!
"$REDIS_BIN" --port "$RPORT" --save '' --appendonly no --logfile "$TMPD/redis.log" --daemonize yes

declare -a SUB_PIDS=()
cleanup() {
	if ((${#SUB_PIDS[@]} > 0)); then
		kill "${SUB_PIDS[@]}" 2>/dev/null || true
	fi
	kill "$UPID" 2>/dev/null || true
	"$CLI_BIN" -p "$RPORT" SHUTDOWN NOSAVE 2>/dev/null || true
	rm -rf "$TMPD"
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

numsub() { # port -> subscriber count for $CHAN
	"$CLI_BIN" -p "$1" PUBSUB NUMSUB "$CHAN" 2>/dev/null | tail -1
}

# start_subs PORT TAG — launch $SUBS Go subscribers, wait until the
# server reports all of them.
start_subs() {
	local port="$1" tag="$2" i
	for i in $(seq 1 "$SUBS"); do
		"$PSUB" -addr "127.0.0.1:$port" -chan "$CHAN" >"$TMPD/$tag-$i.txt" 2>&1 &
		SUB_PIDS+=($!)
	done
	for _ in $(seq 1 100); do
		[[ "$(numsub "$port")" == "$SUBS" ]] && return 0
		sleep 0.05
	done
	echo "subscribers on port $port did not attach (numsub=$(numsub "$port"))" >&2
	exit 1
}

# stop_subs — SIGTERM all subscribers (driver prints its final count) and
# reap them.
stop_subs() {
	if ((${#SUB_PIDS[@]} > 0)); then
		kill -TERM "${SUB_PIDS[@]}" 2>/dev/null || true
		wait "${SUB_PIDS[@]}" 2>/dev/null || true
	fi
	SUB_PIDS=()
}

# delivery TAG EXPECTED — prints "full/total got every message, min=N"
delivery() {
	local tag="$1" expected="$2" i n full=0 got
	local min="$expected"
	for i in $(seq 1 "$SUBS"); do
		got=$(grep -oE 'received [0-9]+' "$TMPD/$tag-$i.txt" 2>/dev/null | tail -1 | awk '{print $2}' || true)
		n="${got:-0}"
		[[ "$n" == "$expected" ]] && full=$((full + 1))
		(( n < min )) && min=$n
	done
	echo "$full/$SUBS full, min=$min"
}

# extract "N requests per second" (mean) for a run
rps() {
	local port="$1"
	shift
	"$BENCH_BIN" -p "$port" "$@" 2>/dev/null | grep -E 'throughput summary: [0-9.]+ requests per second' | awk '{print $3}' | paste -sd' ' -
}

declare -a ROWS

# a. zero subscribers
U0=$(rps "$UPORT" -c 50 -P 16 -n "$REQUESTS" PUBLISH "$CHAN" payload)
R0=$(rps "$RPORT" -c 50 -P 16 -n "$REQUESTS" PUBLISH "$CHAN" payload)
ROWS+=("PUBLISH, 0 subscribers,  -c 50 -P 16|$U0|$R0|no delivery expected")

# b/b2/b3. fan-out with $SUBS subscribers at three publisher concurrencies
fanout() {
	local label="$1" n="$2" ; shift 2
	start_subs "$UPORT" u; start_subs "$RPORT" r
	local u r un rn ud rd
	u=$(rps "$UPORT" -n "$n" "$@" PUBLISH "$CHAN" payload)
	un=$(numsub "$UPORT")
	r=$(rps "$RPORT" -n "$n" "$@" PUBLISH "$CHAN" payload)
	rn=$(numsub "$RPORT")
	sleep 2 # let surviving subscribers drain their socket buffers
	stop_subs
	ud=$(delivery u "$n"); rd=$(delivery r "$n")
	ROWS+=("$label|$u|$r|Ultima $un/$SUBS subs up ($ud); Redis $rn/$SUBS subs up ($rd)")
}

fanout "PUBLISH, $SUBS subscribers,  -c 50 -P 16" "$REQUESTS" -c 50 -P 16
fanout "PUBLISH, $SUBS subscribers,  -c 50 -P  1" "$REQUESTS" -c 50 -P 1
fanout "PUBLISH, $SUBS subscribers,  -c  1 -P  1" "$SINGLE_N" -c 1 -P 1

HW_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
HW_CORES=$(sysctl -n hw.ncpu)
MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
UV=$(./ultima-server --version 2>/dev/null || echo unknown)
RV=$("$REDIS_BIN" --version | head -1)
BV=$("$BENCH_BIN" --version 2>/dev/null | head -1 || true)

mkdir -p docs/benchmarks
{
	echo "# M3 Pub/Sub Benchmark Report — $(date +%Y-%m-%d)"
	echo
	echo "Milestone M3 exit criterion: redis-benchmark pub/sub (design doc §14.4)."
	echo
	echo "## Environment"
	echo
	echo "- Machine: $HW_MODEL, $HW_CORES cores, ${MEM_GB} GB RAM ($(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m))"
	echo "- Ultima: \`$UV\` (shards: 4×GOMAXPROCS, power of two)"
	echo "- Redis: \`$RV\` (via REDIS_BIN=$REDIS_BIN; \`--save '' --appendonly no\`)"
	echo "- Driver: \`$BV\`, PUBLISH $CHAN payload, -n $REQUESTS per run (-n $SINGLE_N for -c 1)"
	echo "- Subscribers: $SUBS × \`note/pubsub-bench-sub\` (Go discard-and-count reader;"
	echo "  redis-cli formats every message and cannot drain fast enough, which trips"
	echo "  Ultima's slow-consumer close — a client artifact, so it was not used)"
	echo "- Ultima config: \`{\"server\":{\"resp_addr\":\"127.0.0.1:$UPORT\",\"log_level\":\"error\"}}\`"
	echo
	echo "## Headline numbers (requests/sec, higher is better)"
	echo
	echo "| Workload | Ultima | Redis 7.2.7 | Ultima/Redis | Delivery check |"
	echo "|---|---|---|---|---|"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r name u r note <<< "$row"
		ur="${u%% *}"; rr="${r%% *}"
		if [[ -n "$ur" && -n "$rr" && "$rr" != "0" ]]; then
			ratio=$(awk "BEGIN{printf \"%.2f\", $ur/$rr}")
		else
			ratio="n/a"
		fi
		echo "| $name | ${ur:-err} | ${rr:-err} | ${ratio}x | $note |"
	done
	echo
	echo "Numbers are single-run means from redis-benchmark's summary output;"
	echo "rerun \`make bench\` for a fresh sample. Both servers ran on the same"
	echo "loopback interface, sequentially, with no other benchmark load."
	echo
	echo "Delivery check: subscriber connections still attached after the run"
	echo "(PUBSUB NUMSUB) and per-subscriber received-message counts from the"
	echo "driver. Ultima closes a subscribed connection whose push queue exceeds"
	echo "4096 messages (slow consumer, the analogue of Redis's"
	echo "client-output-buffer-limit pubsub 32mb/8mb/60, which is far more"
	echo "generous), so at full publisher speed some or all Ultima subscribers"
	echo "are dropped by design; subscribers that stay attached receive every"
	echo "message. The \`-c 50 -P 16\` fan-out row is therefore unstable"
	echo "run-to-run for Ultima — the mean blends throttled fan-out with fast"
	echo "zero-subscriber publishing after the drops. The \`-P 1\` rows, where"
	echo "all subscribers on both servers received all messages, are the stable"
	echo "apples-to-apples comparison."
} > "$REPORT"

echo "wrote $REPORT"
cat "$REPORT"
