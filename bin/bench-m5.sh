#!/usr/bin/env bash
# bench-m5.sh — M5 maxmemory soak: sustained write load against a bounded
# maxmemory with each of the 8 Redis eviction policies, on Ultima and on a
# local redis-server, same machine (design doc §14.4 M5 exit criterion,
# docs/m5-detailed-plan.md M5d). Asserts bounded memory + sustained
# throughput on the Ultima side and writes the report to
# docs/benchmarks/M5-<date>.md in the M1/M3 format. Run standalone or
# chained from bin/bench.sh (BENCH_M5=0 disables the chain).
#
# Method: with maxmemory fixed, drive SET key:__rand_int__ <payload> over a
# random keyspace ~20x larger than fits in memory, so nearly every request
# inserts a fresh key and forces eviction at steady state (volatile-* runs
# add PX so the keys are evictable). After each run the script checks
# INFO used_memory stays within 5% of maxmemory, evicted_keys advanced
# (eviction policies), and a probe SET gets the Redis OOM error under
# noeviction vs OK under the evicting policies. Absolute memory numbers are
# NOT compared against Redis (Ultima reports the keyspace estimate, Redis
# reports process memory — D10).
#
# Tunables: BENCH_M5_REQUESTS, BENCH_M5_MAXMEM_MB, BENCH_M5_DATASIZE,
# BENCH_M5_KEYSPACE, BENCH_M5_TTL_MS, plus the shared REDIS_BIN / BENCH_BIN
# / BENCH_CLI_BIN / BENCH_ULTIMA_PORT / BENCH_REDIS_PORT.
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_BIN="${REDIS_BIN:-redis-server}"
BENCH_BIN="${BENCH_BIN:-redis-benchmark}"
CLI_BIN="${BENCH_CLI_BIN:-redis-cli}"
UPORT="${BENCH_ULTIMA_PORT:-7379}"
RPORT="${BENCH_REDIS_PORT:-7380}"
REQUESTS="${BENCH_M5_REQUESTS:-2000000}"
MAXMEM_MB="${BENCH_M5_MAXMEM_MB:-64}"
DATASIZE="${BENCH_M5_DATASIZE:-1024}"
KEYSPACE="${BENCH_M5_KEYSPACE:-1000000}"
TTL_MS="${BENCH_M5_TTL_MS:-600000}"
MAXMEM_BYTES=$((MAXMEM_MB * 1024 * 1024))
REPORT="docs/benchmarks/M5-$(date +%Y-%m-%d).md"

POLICIES=(noeviction allkeys-lru allkeys-lfu allkeys-random volatile-lru volatile-lfu volatile-random volatile-ttl)

TMPD=$(mktemp -d /tmp/ultima-m5-soak-XXXX)

for p in "$UPORT" "$RPORT"; do
	if nc -z 127.0.0.1 "$p" 2>/dev/null; then
		echo "port $p is in use; set BENCH_ULTIMA_PORT/BENCH_REDIS_PORT" >&2
		exit 1
	fi
done

go build -ldflags "$(sh bin/gen-build-stamp.sh)" -o ./ultima-server ./cmd/ultima-server

CFG="$TMPD/ultima.cfg.json"
cat > "$CFG" <<EOF
{"server":{"resp_addr":"127.0.0.1:$UPORT","grpc_addr":"127.0.0.1:0","http_addr":"127.0.0.1:0","log_level":"error"},"persist":{"dir":"$TMPD/data","save":""}}
EOF

./ultima-server --cfg "$CFG" 2>"$TMPD/ultima.log" &
UPID=$!
"$REDIS_BIN" --port "$RPORT" --save '' --appendonly no --maxmemory 0 --logfile "$TMPD/redis.log" --daemonize yes

cleanup() {
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

PAYLOAD=$(head -c "$DATASIZE" /dev/zero | tr '\0' 'x')

cli() { # PORT args...
	local port="$1"
	shift
	"$CLI_BIN" -p "$port" "$@" 2>/dev/null
}

info_field() { # PORT SECTION FIELD -> integer value
	cli "$1" INFO "$2" | grep -E "^$3:" | cut -d: -f2 | tr -d '\r'
}

# extract "N requests per second" (mean) from one benchmark output file
rps_of() {
	grep -E 'throughput summary: [0-9.]+ requests per second' "$1" | awk '{print $3}' | tail -1
}

mb_of() { # bytes -> MB with one decimal
	awk "BEGIN{printf \"%.1f\", $1/1048576}"
}

# soak PORT POLICY — echoes "rps|used_bytes|evicted_delta|probe"
soak() {
	local port="$1" policy="$2"
	local extra=""
	case "$policy" in
	volatile-*) extra="PX $TTL_MS" ;;
	esac
	cli "$port" CONFIG SET maxmemory "$MAXMEM_BYTES" >/dev/null
	cli "$port" CONFIG SET maxmemory-policy "$policy" >/dev/null
	cli "$port" FLUSHALL >/dev/null
	local ev0 out err rps used ev1 probe
	ev0=$(info_field "$port" stats evicted_keys)
	out="$TMPD/bench-$port-$policy.out"
	err="$TMPD/bench-$port-$policy.err"
	"$BENCH_BIN" -p "$port" -c 50 -P 16 -n "$REQUESTS" -r "$KEYSPACE" \
		SET "key:__rand_int__" "$PAYLOAD" $extra >"$out" 2>"$err" || true
	rps=$(rps_of "$out" || true)
	sleep 0.2 # let in-flight evictions settle before sampling INFO
	used=$(info_field "$port" memory used_memory)
	ev1=$(info_field "$port" stats evicted_keys)
	probe=$(cli "$port" SET "soakprobe:$policy" v || true)
	rm -f "$out" "$err"
	echo "${rps:-err}|${used:-0}|$(( ${ev1:-0} - ${ev0:-0} ))|${probe:-err}"
}

declare -a ROWS
GATE="ok"
note_fail() { GATE="FAIL: $1"; echo "GATE FAILURE: $1" >&2; }

for policy in "${POLICIES[@]}"; do
	echo "--- policy $policy ---"
	# Read into a variable FIRST: "IFS='|' read ... <<< \"$(soak ...)\"
	# would leak the pipe-only IFS into soak's command substitution, where
	# unquoted $extra then fails to word-split (PX 600000 arrives as one
	# argument -> ERR syntax error on both servers).
	uline=$(soak "$UPORT" "$policy" || echo "err|0|0|err")
	rline=$(soak "$RPORT" "$policy" || echo "err|0|0|err")
	IFS='|' read -r urps uused uev uprobe <<< "$uline"
	IFS='|' read -r rrps rused rev rprobe <<< "$rline"

	uratio="n/a"
	if [[ "$urps" =~ ^[0-9.]+$ && "$rrps" =~ ^[0-9.]+$ && "$rrps" != "0" ]]; then
		uratio=$(awk -v u="$urps" -v r="$rrps" 'BEGIN{printf "%.2fx", u/r}')
	fi

	# Ultima gate: bounded memory (5% slack for cross-connection in-flight
	# tasks; eviction runs after each shard task against the per-shard
	# quota), evictions happen under the evicting policies, probe verdict.
	ubounded=$(awk -v u="${uused:-0}" -v m="$MAXMEM_BYTES" 'BEGIN{print (u > 0 && u <= m*1.05) ? "yes" : "NO"}')
	if [[ "$policy" == noeviction ]]; then
		ucheck="bounded=$ubounded, probe OOM"
		[[ "$urps" =~ ^[0-9.]+$ ]] || urps="n/a (OOM-bound)"
		[[ "$rrps" =~ ^[0-9.]+$ ]] || rrps="n/a (OOM-bound)"
		[[ "$ubounded" == yes ]] || note_fail "$policy: used_memory $uused > maxmemory $MAXMEM_BYTES"
		[[ "$uprobe" == OOM* ]] || note_fail "$policy: probe SET reply '$uprobe' (want OOM)"
		[[ "$uev" == 0 ]] || note_fail "$policy: evicted_keys=$uev under noeviction"
	else
		ucheck="bounded=$ubounded, evicted $uev, probe OK"
		[[ "$ubounded" == yes ]] || note_fail "$policy: used_memory $uused > maxmemory $MAXMEM_BYTES"
		(( uev > 0 )) || note_fail "$policy: evicted_keys did not advance"
		[[ "$uprobe" == OK ]] || note_fail "$policy: probe SET reply '$uprobe' (want OK)"
		[[ "$urps" =~ ^[0-9.]+$ ]] || note_fail "$policy: no throughput summary (run failed)"
	fi
	# Redis is reported, not gated: used_memory is process memory and can
	# legitimately sit above maxmemory between eviction passes (and under
	# noeviction it lands back below maxmemory once the benchmark's
	# in-flight input buffers drain, so the probe can return OK there).
	rbounded=$(awk -v u="${rused:-0}" -v m="$MAXMEM_BYTES" 'BEGIN{print (u <= m*1.30) ? "yes" : "no"}')

	ROWS+=("$policy|$urps|$rrps|$uratio|$(mb_of "${uused:-0}") / $uev|$(mb_of "${rused:-0}") / $rev|$ucheck|${rbounded}|$rprobe")
done

HW_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
HW_CORES=$(sysctl -n hw.ncpu)
MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
UV=$(./ultima-server --version 2>/dev/null || echo unknown)
RV=$("$REDIS_BIN" --version | head -1)
BV=$("$BENCH_BIN" --version 2>/dev/null | head -1 || true)

mkdir -p docs/benchmarks
{
	echo "# M5 Maxmemory Soak Report — $(date +%Y-%m-%d)"
	echo
	echo "Milestone M5 exit criterion: maxmemory soak (design doc §14.4,"
	echo "docs/m5-detailed-plan.md M5d)."
	echo
	echo "## Environment"
	echo
	echo "- Machine: $HW_MODEL, $HW_CORES cores, ${MEM_GB} GB RAM ($(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m))"
	echo "- Ultima: \`$UV\` (shards: 4×GOMAXPROCS, power of two; eviction quota per shard = maxmemory/shardCount)"
	echo "- Redis: \`$RV\` (via REDIS_BIN=$REDIS_BIN; \`--save '' --appendonly no\`)"
	echo "- Driver: \`$BV\`"
	echo "- Soak: maxmemory ${MAXMEM_MB} MB, \`SET key:__rand_int__ <${DATASIZE}-byte value>\`"
	echo "  over a $KEYSPACE-key random space (\`-r\`, ~20x what fits), -c 50 -P 16,"
	echo "  -n $REQUESTS per run; volatile-* policies add \`PX $TTL_MS\` so keys are"
	echo "  evictable"
	echo "- Ultima config: \`{\"server\":{\"resp_addr\":\"127.0.0.1:$UPORT\",\"log_level\":\"error\"},\"persist\":{\"save\":\"\"\}}\`,"
	echo "  \`maxmemory\`/\`maxmemory-policy\` set live via CONFIG per run"
	echo
	echo "## Headline numbers (requests/sec, higher is better)"
	echo
	echo "| Policy | Ultima rps | Redis rps | Ultima/Redis | Ultima used MB / evicted | Redis used MB / evicted | Ultima check |"
	echo "|---|---|---|---|---|---|---|"
	for row in "${ROWS[@]}"; do
		IFS='|' read -r policy urps rrps uratio umem rmem ucheck _rbounded _rprobe <<< "$row"
		echo "| $policy | ${urps:-err} | ${rrps:-err} | ${uratio} | $umem | $rmem | $ucheck |"
	done
	echo
	echo "Numbers are single-run means from redis-benchmark's summary output;"
	echo "rerun \`make bench-m5\` for a fresh sample. Both servers ran on the same"
	echo "loopback interface, sequentially, with no other benchmark load."
	echo
	echo "used MB is INFO \`used_memory\` after the run; \"evicted\" is the"
	echo "\`evicted_keys\` delta over the run. The two used_memory columns are not"
	echo "comparable to each other: Ultima reports the keyspace estimate the"
	echo "maxmemory gate enforces (bounded by construction, D10), Redis reports"
	echo "process memory including allocator overhead, so Redis rows legitimately"
	echo "sit above the ${MAXMEM_MB} MB limit. The Ultima check column is the soak"
	echo "assertion: used_memory within 5% of maxmemory, evicted_keys advancing"
	echo "under the evicting policies, and a post-run probe SET returning Redis's"
	echo "byte-exact OOM error under noeviction vs OK under the evicting policies"
	echo "(noeviction rps is n/a: once full, every write is rejected and"
	echo "redis-benchmark prints no throughput summary for an all-error run)."
} > "$REPORT"

echo "wrote $REPORT"
cat "$REPORT"

if [[ "$GATE" != ok ]]; then
	echo "$GATE" >&2
	exit 1
fi
echo "soak gate: OK (Ultima memory bounded under every policy)"
