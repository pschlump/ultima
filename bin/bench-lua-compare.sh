#!/usr/bin/env bash
# bench-lua-compare.sh — gopher_lua_original experiment harness: runs the
# 4 short Lua chunks of bin/bench-m8.sh (EVAL return 1 / script+SET /
# script+GET / 1k-iter loop) against several ultima-server binaries and
# captures throughput (redis-benchmark rps) AND server memory (peak RSS
# sampled at 20 Hz during each leg, plus idle and end-of-run RSS).
#
# Configs are "label|binary|pool_size[|extra script config JSON]" triples
# in BENCH_CONFIGS (default:
# the main-branch wasm build with its shipping VM pool, and this branch's
# pure-Go build with the pool on and off). redis-server 7.2.7 is measured
# too, as the shared reference point both binaries were benched against.
#
# Knobs: BENCH_REQUESTS (default 50000 per -c 50 leg), BENCH_PORT (7479),
# REDIS_BIN, BENCH_BIN, REPORT (docs/benchmarks/M8-lua-compare-<date>.md).
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_BIN="${REDIS_BIN:-redis-server}"
BENCH_BIN="${BENCH_BIN:-redis-benchmark}"
PORT="${BENCH_PORT:-7479}"
RPORT=7478
N="${BENCH_REQUESTS:-50000}"
N1=$((N / 10)) # -c 1 legs
REPORT="${REPORT:-docs/benchmarks/M8-lua-compare-$(date +%Y-%m-%d).md}"

CHUNK1='return 1'
CHUNK2="return redis.call('set',KEYS[1],ARGV[1])"
CHUNK3="return redis.call('get',KEYS[1])"
CHUNK4='local i=0 while i<1000 do i=i+1 end return i'

CONFIGS=("${BENCH_CONFIGS[@]:-}")
if [[ ${#CONFIGS[@]} -eq 0 || -z "${CONFIGS[0]}" ]]; then
	CONFIGS=(
		"main-wasm-pool1|/tmp/ultima-server-main|1"
		"main-wasm-norecycle|/tmp/ultima-server-main|1|,\"script_vm_recycle_runs\":0,\"script_vm_recycle_pct\":0"
		"orig-purego-pool1|/tmp/ultima-server-orig|1"
		"orig-purego-pool0|/tmp/ultima-server-orig|0"
	)
fi

for p in "$PORT" "$RPORT"; do
	if nc -z 127.0.0.1 "$p" 2>/dev/null; then
		echo "port $p in use; set BENCH_PORT" >&2
		exit 1
	fi
done

redis_pid=""
srv_pid=""
cleanup() {
	[[ -n "$srv_pid" ]] && kill "$srv_pid" 2>/dev/null || true
	[[ -n "$redis_pid" ]] && redis-cli -p "$RPORT" SHUTDOWN NOSAVE 2>/dev/null || true
	srv_pid=""; redis_pid=""
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

rss_kb() { ps -o rss= -p "$1" 2>/dev/null | tr -d ' '; }

# peak_rss <pid> <outfile> — samples at 20 Hz until killed, keeping the
# running max in the file (written every iteration so a kill mid-loop
# still leaves the latest peak behind).
peak_rss() {
	local pid="$1" out="$2" cur max=0
	echo 0 >"$out"
	while kill -0 "$pid" 2>/dev/null; do
		cur=$(rss_kb "$pid")
		if [[ -n "$cur" && "$cur" -gt "$max" ]]; then
			max=$cur
			echo "$max" >"$out"
		fi
		sleep 0.05
	done
}

declare -a ROWS

# run_legs <label> <port> <pid-to-sample(or empty)>
run_legs() {
	local label="$1" port="$2" pid="$3"
	local leg rps peak rssfile
	_run_one() {
		local legname="$1"; shift
		rssfile=$(mktemp /tmp/rss-XXXX)
		if [[ -n "$pid" ]]; then peak_rss "$pid" "$rssfile" & sampler=$!; fi
		rps=$("$BENCH_BIN" -p "$port" "$@" 2>/dev/null |
			grep -E 'throughput summary: [0-9.]+ requests per second' | awk '{print $3}')
		if [[ -n "$pid" ]]; then kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null || true; fi
		peak=$(cat "$rssfile"); rm -f "$rssfile"
		ROWS+=("$label|$legname|${rps:-err}|$(( ${peak:-0} / 1024 ))")
		printf '%-18s %-28s %10s rps  peak RSS %5d MB\n' "$label" "$legname" "${rps:-err}" "$(( ${peak:-0} / 1024 ))"
	}
	# warm-up (pool fill, caches) — not measured
	"$BENCH_BIN" -p "$port" -c 50 -n 5000 EVAL "$CHUNK1" 0 >/dev/null 2>&1 || true
	_run_one "EVAL return 1  -c50" -c 50 -n "$N" EVAL "$CHUNK1" 0
	_run_one "EVAL script+SET  -c50" -c 50 -n "$N" EVAL "$CHUNK2" 1 "bench:lc:key:__rand_int__" v
	_run_one "EVAL script+GET  -c50" -c 50 -n "$N" EVAL "$CHUNK3" 1 "bench:lc:key:__rand_int__"
	_run_one "EVAL 1k-iter loop  -c50" -c 50 -n "$N" EVAL "$CHUNK4" 0
	_run_one "EVAL return 1  -c 1" -c 1 -n "$N1" EVAL "$CHUNK1" 0
}

for cfg in "${CONFIGS[@]}"; do
	IFS='|' read -r label bin pool extra <<<"$cfg"
	CFGF=$(mktemp /tmp/ultima-lc-XXXX.json)
	cat >"$CFGF" <<EOF
{"server":{"resp_addr":"127.0.0.1:$PORT","grpc_addr":"127.0.0.1:0","http_addr":"127.0.0.1:0","log_level":"error"},"script":{"script_vm_pool_size":$pool$extra}}
EOF
	"$bin" --cfg "$CFGF" 2>/tmp/ultima-lc.log &
	srv_pid=$!
	wait_port "$PORT"
	idle=$(rss_kb "$srv_pid")
	echo "=== $label ($bin pool=$pool) idle RSS $((idle / 1024)) MB"
	run_legs "$label" "$PORT" "$srv_pid"
	end=$(rss_kb "$srv_pid")
	ROWS+=("$label|END-OF-RUN RSS| |$((end / 1024))")
	ROWS+=("$label|idle RSS at startup| |$((idle / 1024))")
	echo "=== $label end RSS $((end / 1024)) MB"
	kill "$srv_pid" 2>/dev/null || true
	wait "$srv_pid" 2>/dev/null || true
	srv_pid=""
	rm -f "$CFGF"
done

# redis-server reference
"$REDIS_BIN" --port "$RPORT" --save '' --appendonly no --logfile /tmp/redis-lc.log --daemonize yes
redis_pid=1
wait_port "$RPORT"
echo "=== redis 7.2.7 reference"
run_legs "redis-7.2.7" "$RPORT" ""
redis-cli -p "$RPORT" SHUTDOWN NOSAVE 2>/dev/null || true
redis_pid=""

mkdir -p docs/benchmarks
{
	echo "# Lua backend comparison — $(date +%Y-%m-%d)"
	echo
	echo "Experiment branch \`gopher_lua_original\`: original pure-Go"
	echo "gopher-lua (vendored third_party/gopher-lua + pure-Go host shim)"
	echo "vs \`main\`'s wasm backend (gopher-lua compiled to wasm on wazero,"
	echo "R3 per-script VM pool). Same 4 short chunks as bin/bench-m8.sh,"
	echo "-n $N per -c 50 leg (-c 1 leg: $N1)."
	echo
	echo "| Config | Leg | rps | peak RSS MB |"
	echo "|---|---|---|---|"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r label leg rps rss <<<"$row"
		echo "| $label | $leg | ${rps:-} | $rss |"
	done
} >"$REPORT"
echo "wrote $REPORT"
