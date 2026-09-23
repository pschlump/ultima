#!/usr/bin/env bash
# bench-m8.sh — M8 Lua scripting benchmark: EVAL/EVALSHA throughput of
# Ultima (fresh gopher-lua wasm VM per script, S4) vs local redis-server
# 7.2.7 (PUC Lua, cached scripts in one shared state). Chained from
# bin/bench.sh unless BENCH_M8=0; writes docs/benchmarks/M8-<date>.md.
#
# Knobs: BENCH_M8_REQUESTS (default 5000 — Ultima's fresh-VM EVAL runs
# ~200/s at -c 50, so 20k requests would be ~2 min per row), BENCH_ULTIMA_PORT,
# BENCH_REDIS_PORT (shared with bench.sh), REDIS_BIN, BENCH_BIN.
# BENCH_M8_UNTIL=HH:MM (local) turns the run into a soak: the sweep repeats
# until that wall-clock time (checked between legs — the in-flight leg
# finishes, nothing new starts after the deadline) and the report gains one
# results table per sweep.
#
# Battery gate: between legs the battery is checked with the sibling
# battery-check tool — below BENCH_M8_BATTERY_LOW (50) the soak pauses until
# the charge recovers to BENCH_M8_BATTERY_HIGH (60), for laptops whose load
# outruns the AC adapter. Knobs: BENCH_M8_BATTERY_CHECK (tool path; default
# ../battery-check/battery-check, else $PATH), BENCH_M8_BATTERY_LOW/HIGH,
# BENCH_M8_BATTERY_POLL (seconds between re-checks, 60). No tool found or no
# battery present = gating silently off.
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_BIN="${REDIS_BIN:-redis-server}"
BENCH_BIN="${BENCH_BIN:-redis-benchmark}"
UPORT="${BENCH_ULTIMA_PORT:-7379}"
RPORT="${BENCH_REDIS_PORT:-7380}"
REQUESTS="${BENCH_M8_REQUESTS:-5000}"
REPORT="docs/benchmarks/M8-$(date +%Y-%m-%d).md"

UNTIL="${BENCH_M8_UNTIL:-}"
DEADLINE=0
if [[ -n "$UNTIL" ]]; then
	if DEADLINE=$(date -j -f "%H:%M" "$UNTIL" +%s 2>/dev/null); then
		:
	elif DEADLINE=$(date -d "today $UNTIL" +%s 2>/dev/null); then
		:
	else
		echo "BENCH_M8_UNTIL must be HH:MM (local time), got '$UNTIL'" >&2
		exit 1
	fi
	if (( $(date +%s) >= DEADLINE )); then
		echo "BENCH_M8_UNTIL=$UNTIL is already in the past" >&2
		exit 1
	fi
	echo "soak mode: sweeping until $UNTIL local"
fi

# Battery gate setup (see header comment).
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
# recovers to BATTERY_HIGH (or the soak deadline passes). Tool errors
# (exit 2) disable gating rather than stall the run; exit 1 = below level.
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
		if (( DEADLINE > 0 )) && (( $(date +%s) >= DEADLINE )); then
			return 0
		fi
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

for p in "$UPORT" "$RPORT"; do
	if nc -z 127.0.0.1 "$p" 2>/dev/null; then
		echo "port $p is in use; set BENCH_ULTIMA_PORT/BENCH_REDIS_PORT" >&2
		exit 1
	fi
done

go build -ldflags "$(sh bin/gen-build-stamp.sh)" -o ./ultima-server ./cmd/ultima-server

CFG=$(mktemp /tmp/ultima-bench-m8-XXXX.json)
cat > "$CFG" <<EOF
{"server":{"resp_addr":"127.0.0.1:$UPORT","grpc_addr":"127.0.0.1:0","http_addr":"127.0.0.1:0","log_level":"error"}}
EOF

./ultima-server --cfg "$CFG" 2>/tmp/ultima-bench-m8.log &
UPID=$!
"$REDIS_BIN" --port "$RPORT" --save '' --appendonly no --logfile /tmp/redis-bench-m8.log --daemonize yes
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

# EVALSHA leg needs the script cached on both servers (content-addressed:
# one sha for both).
SHA1=$(redis-cli -p "$UPORT" SCRIPT LOAD "return 1")
redis-cli -p "$RPORT" SCRIPT LOAD "return 1" >/dev/null
SHA2=$(redis-cli -p "$UPORT" SCRIPT LOAD "return redis.call('set',KEYS[1],ARGV[1])")
redis-cli -p "$RPORT" SCRIPT LOAD "return redis.call('set',KEYS[1],ARGV[1])" >/dev/null

declare -a ROWS
run_pair() {
	local name="$1"; shift
	if (( DEADLINE > 0 )) && (( $(date +%s) >= DEADLINE )); then
		ROWS+=("$name|skipped: past $UNTIL|skipped: past $UNTIL")
		return
	fi
	battery_gate
	local u r
	u=$(rps "$UPORT" "$@")
	r=$(rps "$RPORT" "$@")
	ROWS+=("$name|$u|$r")
}

run_sweep() {
	run_pair "EVAL return 1,            -c 50" -c 50 -n "$REQUESTS" EVAL "return 1" 0
	run_pair "EVALSHA return 1,         -c 50" -c 50 -n "$REQUESTS" EVALSHA "$SHA1" 0
	run_pair "EVAL script+SET,          -c 50" -c 50 -n "$REQUESTS" EVAL "return redis.call('set',KEYS[1],ARGV[1])" 1 "bench:m8:key:__rand_int__" v
	run_pair "EVALSHA script+SET,       -c 50" -c 50 -n "$REQUESTS" EVALSHA "$SHA2" 1 "bench:m8:key:__rand_int__" v
	run_pair "EVAL script+GET,          -c 50" -c 50 -n "$REQUESTS" EVAL "return redis.call('get',KEYS[1])" 1 "bench:m8:key:__rand_int__"
	run_pair "EVAL 1k-iter loop,        -c 50" -c 50 -n "$REQUESTS" EVAL "local i=0 while i<1000 do i=i+1 end return i" 0
	run_pair "EVAL return 1,            -c  1" -c 1 -n "$((REQUESTS / 10))" EVAL "return 1" 0
	run_pair "EVALSHA return 1,         -c  1" -c 1 -n "$((REQUESTS / 10))" EVALSHA "$SHA1" 0
}

HW_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
HW_CORES=$(sysctl -n hw.ncpu)
MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
UV=$(./ultima-server --version 2>/dev/null || echo unknown)
RV=$("$REDIS_BIN" --version | head -1)
BV=$("$BENCH_BIN" --version 2>/dev/null | head -1 || true)

report_header() {
	echo "# M8 Benchmark Report — $(date +%Y-%m-%d)$1"
	echo
	echo "Milestone M8e: EVAL/EVALSHA throughput vs Redis 7.2.7"
	echo "(docs/m8-detailed-plan.md). Ultima runs every script in a FRESH"
	echo "gopher-lua wasm VM (decision S4: wazero instantiation + stdlib open"
	echo "per run, no shared state); Redis compiles once and re-runs the"
	echo "cached function in its shared PUC state. Every row's gap is the"
	echo "VM-lifecycle cost (~43 ms/run — EVALSHA ≈ EVAL confirms it is"
	echo "instantiation, not compile); at 1k loop iterations R3 (wazero's"
	echo "interpreter backend on darwin/arm64) barely registers next to it"
	echo "and dominates only for CPU-heavy scripts. A per-script VM pool is"
	echo "the sketched mitigation (docs/m8-detailed-plan.md M8e)."
	echo
	echo "## Environment"
	echo
	echo "- Machine: $HW_MODEL, $HW_CORES cores, ${MEM_GB} GB RAM ($(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m))"
	echo "- Ultima: \`$UV\`"
	echo "- Redis: \`$RV\` (via REDIS_BIN=$REDIS_BIN; \`--save '' --appendonly no\`)"
	echo "- Driver: \`$BV\`, -n $REQUESTS per run (-c 1 rows: $((REQUESTS / 10)))"
}

report_table() {
	echo "| Benchmark | Ultima | Redis 7.2.7 |"
	echo "|---|---|---|"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r name u r <<<"$row"
		echo "| $name | ${u:-n/a} | ${r:-n/a} |"
	done
}

report_note() {
	echo "Note: redis-benchmark exits on the first error reply; a missing"
	echo "number means the run errored (see /tmp/ultima-bench-m8.log)."
}

mkdir -p docs/benchmarks

if (( DEADLINE > 0 )); then
	{
		report_header " (soak until $UNTIL)"
		echo
		echo "Soak mode (BENCH_M8_UNTIL=$UNTIL): the sweep repeated until the"
		echo "deadline; the deadline is checked between legs, so a leg in flight"
		echo "at $UNTIL finished but no new one started. One table per sweep."
		echo
	} > "$REPORT"
	n=0
	while (( $(date +%s) < DEADLINE )); do
		n=$((n+1))
		ROWS=()
		run_sweep
		{
			echo "## Sweep $n — finished $(date +%H:%M:%S) (requests/sec, higher is better)"
			echo
			report_table
			echo
		} >> "$REPORT"
		echo "sweep $n done at $(date +%H:%M:%S) (deadline $UNTIL)"
	done
	{
		report_note
	} >> "$REPORT"
	echo "soak stopped at $(date +%H:%M:%S) after $n sweep(s); wrote $REPORT"
else
	ROWS=()
	run_sweep
	{
		report_header ""
		echo
		echo "## Results (requests/sec, higher is better)"
		echo
		report_table
		echo
		report_note
	} > "$REPORT"

	echo "wrote $REPORT"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r name u r <<<"$row"
		printf '%s ultima=%s redis=%s\n' "$name" "${u:-n/a}" "${r:-n/a}"
	done
fi
