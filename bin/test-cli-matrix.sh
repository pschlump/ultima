#!/usr/bin/env bash
# test-cli-matrix.sh — run the CLI command matrix (tests/cli-matrix/cases/*.txt)
# against ultima-server through redis-cli, ultima-cli, ultima-ws-cli and
# ultima-grpc-cli, in both security modes (noauth / auth), and optionally
# validate the expectations themselves against a real redis-server (-R).
#
# Usage:
#   sh bin/test-cli-matrix.sh [options]
#     -t TARGET   run only this target (repeatable; one of:
#                 redis-cli | ultima-cli | ultima-ws-cli | ultima-grpc-cli |
#                 redis | redis3)   default: the four ultima targets
#     -m MODE     noauth | auth | both (default: both)
#     -f REGEX    run only cases/specials whose name matches REGEX
#     -R          also validate expectations against a real redis-server
#                 (targets redis + redis3, i.e. RESP2 and RESP3)
#     -v          verbose: show full diffs for failures
#
# Env knobs: MATRIX_PORT_BASE (default 16379; resp=base, grpc=base+1,
# http=base+2, real redis=base+10), REDIS_CLI, REDIS_SERVER, ULTIMA_SERVER,
# ULTIMA_CLI, ULTIMA_WS_CLI, ULTIMA_GRPC_CLI, CASES_DIR.
set -u
cd "$(dirname "$0")/.."

PORT_BASE=${MATRIX_PORT_BASE:-16379}
REDIS_CLI=${REDIS_CLI:-redis-cli}
REDIS_SERVER=${REDIS_SERVER:-redis-server}
SRV=${ULTIMA_SERVER:-./ultima-server}
UCLI=${ULTIMA_CLI:-./ultima-cli}
UWCLI=${ULTIMA_WS_CLI:-./ultima-ws-cli}
UGCLI=${ULTIMA_GRPC_CLI:-./ultima-grpc-cli}
CASES_DIR=${CASES_DIR:-tests/cli-matrix/cases}

AUTH_PASS=matrix-pass
ADMIN_USER=admin
ADMIN_PASS=matrix-admin-pass

TARGETS=""
MODES="noauth auth"
FILTER="."
WITH_REDIS=0
VERBOSE=0

while getopts "t:m:f:Rv" o; do
	case "$o" in
	t) TARGETS="$TARGETS $OPTARG" ;;
	m) [ "$OPTARG" = both ] && MODES="noauth auth" || MODES="$OPTARG" ;;
	f) FILTER=$OPTARG ;;
	R) WITH_REDIS=1 ;;
	v) VERBOSE=1 ;;
	*) echo "usage: $0 [-t target]... [-m noauth|auth|both] [-f regex] [-R] [-v]" >&2; exit 2 ;;
	esac
done
[ -z "$TARGETS" ] && TARGETS=" redis-cli ultima-cli ultima-ws-cli ultima-grpc-cli"
# shellcheck disable=SC2086
set -- $TARGETS
TARGETS="$*"

TIMEOUT=$(command -v timeout || command -v gtimeout || true)
[ -n "$TIMEOUT" ] || { echo "error: need GNU timeout (brew install coreutils)" >&2; exit 2; }

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cli-matrix.XXXXXX")
SRV_PID=""
REDIS_PID=""
cleanup() {
	[ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null
	[ -n "$REDIS_PID" ] && "$REDIS_CLI" -p "$((PORT_BASE + 10))" shutdown nosave 2>/dev/null
	[ -n "$SRV_PID" ] && wait "$SRV_PID" 2>/dev/null
	rm -rf "$WORK"
}
trap cleanup EXIT

PASS=0 FAIL=0 SKIP=0
FAILED=""

P_RESP=$PORT_BASE
P_GRPC=$((PORT_BASE + 1))
P_HTTP=$((PORT_BASE + 2))
P_REDIS=$((PORT_BASE + 10))
export P_RESP P_GRPC P_HTTP P_REDIS AUTH_PASS ADMIN_USER ADMIN_PASS
export REDIS_CLI UCLI UWCLI UGCLI

# client.sh is the executable form of run_client/run_client_oneshot so that
# `timeout` (which needs a real program, not a shell function) can wrap the
# streaming specials.
cat >"$WORK/client.sh" <<'EOS'
#!/bin/sh
# $1=client|oneshot $2=target $3=mode [args...]
what=$1 target=$2 mode=$3
shift 3
d3=""
[ "$target" = redis3 ] && d3="-3"
extra=${CLI_EXTRA:-}
case "$target" in
redis-cli)
	if [ "$mode" = auth ]; then
		REDISCLI_AUTH=$AUTH_PASS "$REDIS_CLI" --no-raw -p "$P_RESP" "$@" 2>&1
	else
		"$REDIS_CLI" --no-raw -p "$P_RESP" "$@" 2>&1
	fi
	;;
ultima-cli)
	if [ "$mode" = auth ]; then
		"$UCLI" -p "$P_RESP" -a "$AUTH_PASS" "$@" 2>&1
	else
		"$UCLI" -p "$P_RESP" "$@" 2>&1
	fi
	;;
ultima-ws-cli)
	if [ "$mode" = auth ]; then
		"$UWCLI" $extra --addr "127.0.0.1:$P_HTTP" --user "$ADMIN_USER" --pass "$ADMIN_PASS" "$@" 2>&1
	else
		"$UWCLI" $extra --addr "127.0.0.1:$P_HTTP" "$@" 2>&1
	fi
	;;
ultima-grpc-cli)
	if [ "$mode" = auth ]; then
		"$UGCLI" --addr "127.0.0.1:$P_GRPC" --http-addr "127.0.0.1:$P_HTTP" \
			--user "$ADMIN_USER" --pass "$ADMIN_PASS" "$@" 2>&1
	else
		"$UGCLI" --addr "127.0.0.1:$P_GRPC" "$@" 2>&1
	fi
	;;
redis | redis3)
	if [ "$mode" = auth ]; then
		REDISCLI_AUTH=$AUTH_PASS "$REDIS_CLI" --no-raw $d3 -p "$P_REDIS" "$@" 2>&1
	else
		"$REDIS_CLI" --no-raw $d3 -p "$P_REDIS" "$@" 2>&1
	fi
	;;
*) echo "unknown target $target" >&2; exit 2 ;;
esac
EOS
chmod +x "$WORK/client.sh"

# ---------------------------------------------------------------- servers ---

start_ultima() { # mode
	local mode=$1
	local cfg="$WORK/ultima-$mode.cfg.json"
	cat >"$cfg" <<-EOF
	{
	  "server": {
	    "resp_addr": "127.0.0.1:$P_RESP",
	    "grpc_addr": "127.0.0.1:$P_GRPC",
	    "http_addr": "127.0.0.1:$P_HTTP",
	    "shard_count": 0,
	    "log_level": "warn"
	  },
	  "persist": {
	    "dir": "$WORK/data-$mode",
	    "appendonly": false,
	    "save": ""
	  }
	}
	EOF
	if [ "$mode" = auth ]; then
		sh bin/gen-jwt-keys.sh "$WORK/keys" >/dev/null 2>&1
		python3 - "$cfg" <<-EOF
		import json, sys
		p = sys.argv[1]
		c = json.load(open(p))
		c["server"]["requirepass"] = "$AUTH_PASS"
		c["auth"] = {
		  "enabled": True,
		  "jwt_private_key_file": "$WORK/keys/ultima-jwt.pem",
		  "jwt_public_key_file": "$WORK/keys/ultima-jwt.pub",
		  "bootstrap_admin_password": "$ADMIN_PASS",
		  "accounts_file": "$WORK/data-auth/accounts.json",
		}
		json.dump(c, open(p, "w"), indent=2)
		EOF
	fi
	"$SRV" --cfg "$cfg" >"$WORK/server-$mode.log" 2>&1 &
	SRV_PID=$!
	wait_resp "$P_RESP" "$mode" || {
		echo "error: ultima-server failed to start (mode $mode); log:" >&2
		cat "$WORK/server-$mode.log" >&2
		exit 2
	}
}

stop_ultima() {
	[ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
	SRV_PID=""
}

wait_resp() { # port mode
	local i
	for i in $(seq 1 50); do
		if [ "$2" = auth ]; then
			REDISCLI_AUTH=$AUTH_PASS "$REDIS_CLI" -p "$1" PING 2>/dev/null | grep -q PONG && return 0
		else
			"$REDIS_CLI" -p "$1" PING 2>/dev/null | grep -q PONG && return 0
		fi
		sleep 0.1
	done
	return 1
}

start_redis() { # mode
	local rp=""
	[ "$1" = auth ] && rp="--requirepass $AUTH_PASS"
	"$REDIS_SERVER" --port "$P_REDIS" --save '' --appendonly no \
		--dir "$WORK" --daemonize yes $rp --logfile "$WORK/redis-$1.log"
	REDIS_PID=1
	wait_resp "$P_REDIS" "$1" || { echo "error: redis-server failed to start" >&2; exit 2; }
}

stop_redis() {
	if [ "$CUR_MODE" = auth ]; then
		REDISCLI_AUTH=$AUTH_PASS "$REDIS_CLI" -p "$P_REDIS" shutdown nosave 2>/dev/null
	else
		"$REDIS_CLI" -p "$P_REDIS" shutdown nosave 2>/dev/null
	fi
	REDIS_PID=""
	sleep 0.2
}

# ---------------------------------------------------------------- clients ---

# run_client TARGET MODE < input > output ; merges stderr into stdout.
run_client() {
	sh "$WORK/client.sh" client "$1" "$2"
}

# run_client_oneshot TARGET MODE ARGS... — one command per process.
run_client_oneshot() {
	local target=$1 mode=$2
	shift 2
	sh "$WORK/client.sh" oneshot "$target" "$mode" "$@"
}

target_class() { # -> resp2|resp3
	case "$1" in
	redis3 | ultima-ws-cli | ultima-grpc-cli) echo resp3 ;;
	*) echo resp2 ;;
	esac
}

control_flush() { # server(ultima|redis) mode
	local port=$P_RESP
	[ "$1" = redis ] && port=$P_REDIS
	if [ "$2" = auth ]; then
		REDISCLI_AUTH=$AUTH_PASS "$REDIS_CLI" -p "$port" FLUSHALL >/dev/null 2>&1
	else
		"$REDIS_CLI" -p "$port" FLUSHALL >/dev/null 2>&1
	fi
}

# ---------------------------------------------------------------- parsing ---

# Parse all case files into $WORK/parsed/NNN.{meta,in,exp,exp3}.
parse_cases() {
	mkdir -p "$WORK/parsed"
	local n=0 f line state="" cur="" s3=0
	for f in "$CASES_DIR"/*.txt; do
		while IFS= read -r line || [ -n "$line" ]; do
			case "$line" in
			'== case: '*)
				n=$((n + 1))
				cur=$(printf '%03d' "$n")
				state=in
				s3=0
				printf 'name=%s\ntype=exact\ntargets=all\nmodes=all\n' "${line#'== case: '}" >"$WORK/parsed/$cur.meta"
				: >"$WORK/parsed/$cur.in"
				: >"$WORK/parsed/$cur.exp"
				continue
				;;
			'== type: '*) [ -n "$cur" ] && sed -i '' "s/^type=.*/type=${line#'== type: '}/" "$WORK/parsed/$cur.meta" && continue ;;
			'== targets: '*) [ -n "$cur" ] && sed -i '' "s/^targets=.*/targets=${line#'== targets: '}/" "$WORK/parsed/$cur.meta" && continue ;;
			'== modes: '*) [ -n "$cur" ] && sed -i '' "s/^modes=.*/modes=${line#'== modes: '}/" "$WORK/parsed/$cur.meta" && continue ;;
			'== resp3: skip') [ -n "$cur" ] && echo "resp3=skip" >>"$WORK/parsed/$cur.meta" && continue ;;
			'== resp3') [ -n "$cur" ] && state=wait3 && continue ;;
			'== end') cur="" state="" && continue ;;
			'#'*) continue ;; # comments are safe anywhere (INFO is not exercised via cases)
			--) if [ "$state" = in ]; then state=exp; elif [ "$state" = wait3 ]; then state=exp3; fi; continue ;;
			esac
			[ -z "$cur" ] && continue
			case "$state" in
			in) printf '%s\n' "$line" >>"$WORK/parsed/$cur.in" ;;
			exp) printf '%s\n' "$line" >>"$WORK/parsed/$cur.exp" ;;
			exp3) s3=1; printf '%s\n' "$line" >>"$WORK/parsed/$cur.exp3" ;;
			esac
		done <"$f"
		cur="" state="" # cases never span files; a trailing open case ends here
	done
	# Blank separator lines before the next == case: header would otherwise
	# land at the end of the preceding block; strip trailing blank lines.
	local pf
	for pf in "$WORK"/parsed/*.in "$WORK"/parsed/*.exp "$WORK"/parsed/*.exp3; do
		[ -e "$pf" ] || continue
		awk '{ lines[NR]=$0; if (NF) last=NR } END { for (i=1; i<=last; i++) print lines[i] }' \
			"$pf" >"$pf.strip" && mv "$pf.strip" "$pf"
	done
	echo "$n"
}

meta_get() { grep "^$2=" "$WORK/parsed/$1.meta" | head -1 | cut -d= -f2-; }

# ------------------------------------------------------------- comparison ---

compare_case() { # type expected actual -> 0 ok
	local type=$1 exp=$2 act=$3
	case "$type" in
	exact) cmp -s "$exp" "$act" ;;
	sorted)
		sort "$exp" >"$WORK/s.e"
		sort "$act" >"$WORK/s.a"
		cmp -s "$WORK/s.e" "$WORK/s.a"
		;;
	match) match_lines "$exp" "$act" 0 ;;
	smatch) match_lines "$exp" "$act" 1 ;;
	*) echo "bad type $type" >&2; return 2 ;;
	esac
}

# match_lines: every line of actual must fully match the extended regex on
# the same line of expected; line counts must agree. dosort=1 sorts both
# first (for unordered replies with nondeterministic values).
match_lines() { # expected(regex lines) actual dosort
	local exp=$1 act=$2
	if [ "$3" = 1 ]; then
		sort "$exp" >"$WORK/m.e"
		sort "$act" >"$WORK/m.a"
		exp="$WORK/m.e"
		act="$WORK/m.a"
	fi
	awk 'NR==FNR { re[FNR]=$0; n=FNR; next }
	     { if (FNR>n || $0 !~ "^(" re[FNR] ")$") exit 1 }
	     END { if (NR==0 && n>0) exit 1; if (n>0 && FNR!=n) exit 1 }' "$exp" "$act"
}

show_diff() { # type expected actual
	local type=$1 exp=$2 act=$3
	case "$type" in
	sorted | smatch)
		sort "$exp" >"$WORK/d.e"
		sort "$act" >"$WORK/d.a"
		diff -u "$WORK/d.e" "$WORK/d.a" | head -30
		;;
	*) diff -u "$exp" "$act" | head -30 ;;
	esac
}

# ------------------------------------------------------------------ suite ---

CUR_MODE=noauth

record() { # result name
	case "$1" in
	pass) PASS=$((PASS + 1)) ;;
	skip) SKIP=$((SKIP + 1)) ;;
	fail)
		FAIL=$((FAIL + 1))
		FAILED="$FAILED\n  [$CUR_TARGET/$CUR_MODE] $2"
		printf 'FAIL [%s/%s] %s\n' "$CUR_TARGET" "$CUR_MODE" "$2"
		;;
	esac
}

run_suite() { # target server(ultima|redis)
	local target=$1 server=$2 class
	class=$(target_class "$target")
	local cf name type targets modes resp3
	for cf in "$WORK"/parsed/*.meta; do
		local base=${cf%.meta}
		name=$(meta_get "${base##*/}" name)
		type=$(meta_get "${base##*/}" type)
		targets=$(meta_get "${base##*/}" targets)
		modes=$(meta_get "${base##*/}" modes)
		resp3=$(meta_get "${base##*/}" resp3)
		echo "$name" | grep -Eq "$FILTER" || continue
		# targets is a comma-separated constraint list: resp2/resp3 gate the
		# protocol class, no-redis skips the real-redis validation targets.
		local skip=0 tok
		for tok in $(echo "$targets" | tr ',' ' '); do
			case "$tok" in
			all) ;;
			no-redis) [ "$server" = redis ] && skip=1 ;;
			resp2) [ "$class" = resp3 ] && skip=1 ;;
			resp3) [ "$class" = resp2 ] && skip=1 ;;
			*) echo "bad targets token $tok in $name" >&2; exit 2 ;;
			esac
		done
		[ "$skip" = 1 ] && { record skip "$name"; continue; }
		case "$modes" in
		all) ;;
		auth-only) [ "$CUR_MODE" = noauth ] && { record skip "$name"; continue; } ;;
		noauth-only) [ "$CUR_MODE" = auth ] && { record skip "$name"; continue; } ;;
		esac
		local exp="$base.exp"
		if [ "$class" = resp3 ]; then
			if [ "$resp3" = skip ]; then record skip "$name"; continue; fi
			[ -f "$base.exp3" ] && exp="$base.exp3"
		fi
		control_flush "$server" "$CUR_MODE"
		# Timeout guard: a case that blocks (SUBSCRIBE/MONITOR/blocking pop on
		# an empty key) must fail, not hang the run.
		"$TIMEOUT" 30 sh "$WORK/client.sh" client "$target" "$CUR_MODE" <"$base.in" >"$WORK/actual" 2>&1
		if compare_case "$type" "$exp" "$WORK/actual"; then
			record pass "$name"
		else
			record fail "$name"
			if [ "$VERBOSE" = 1 ]; then
				echo "--- expected ($type) vs actual:"
				show_diff "$type" "$exp" "$WORK/actual"
				echo "---"
			fi
		fi
	done
}

# --------------------------------------------------------------- specials ---

# special_client wraps the client for the streaming specials, with a timeout
# (streaming verbs block until SIGINT). ws-cli runs sessionless here: a §9.4
# session would retain its subscriptions for the replay window after the
# subscriber disconnects and pollute later targets' empty-state assertions.
special_client() { # target mode < input > output
	local se=""
	[ "$1" = ultima-ws-cli ] && se="--no-session"
	CLI_EXTRA="$se" "$TIMEOUT" -s INT 6 sh "$WORK/client.sh" client "$1" "$2"
}

# Streaming pub/sub delivery: background subscriber + foreground publisher.
# Channels are suffixed per target: a §9.4 session from an earlier target's
# subscriber is retained server-side for ws_replay_buffer_ms after its
# disconnect and would otherwise still count as a receiver.
special_pubsub() { # target
	local target=$1 name=special:pubsub-delivery
	echo "$name" | grep -Eq "$FILTER" || return 0
	local ch="mx.news.$target"
	local sub="$WORK/sub.out" pub="$WORK/pub.out"
	{ printf 'SUBSCRIBE %s\n' "$ch"; sleep 2.5; } | special_client "$target" "$CUR_MODE" >"$sub" 2>&1 &
	local spid=$!
	sleep 0.8
	printf 'PUBLISH %s hello-matrix\n' "$ch" | run_client "$target" "$CUR_MODE" >"$pub" 2>&1
	wait "$spid" 2>/dev/null
	local ok=1
	grep -q '"message"' "$sub" && grep -q "\"$ch\"" "$sub" && grep -q '"hello-matrix"' "$sub" || ok=0
	grep -qx '(integer) 1' "$pub" || ok=0
	if [ "$ok" = 1 ]; then record pass "$name"; else
		record fail "$name"
		[ "$VERBOSE" = 1 ] && { echo "--- subscriber:"; cat "$sub"; echo "--- publisher:"; cat "$pub"; }
	fi
}

special_psubscribe() { # target
	local target=$1 name=special:psubscribe-delivery
	echo "$name" | grep -Eq "$FILTER" || return 0
	local pat="mx.p.$target.*" topic="mx.p.$target.topic"
	local sub="$WORK/psub.out"
	{ printf 'PSUBSCRIBE %s\n' "$pat"; sleep 2.5; } | special_client "$target" "$CUR_MODE" >"$sub" 2>&1 &
	local spid=$!
	sleep 0.8
	printf 'PUBLISH %s pload\n' "$topic" | run_client "$target" "$CUR_MODE" >/dev/null 2>&1
	wait "$spid" 2>/dev/null
	if grep -q '"pmessage"' "$sub" && grep -q "\"$topic\"" "$sub" && grep -q '"pload"' "$sub"; then
		record pass "$name"
	else
		record fail "$name"
		[ "$VERBOSE" = 1 ] && { echo "--- subscriber:"; cat "$sub"; }
	fi
}

# Blocking pop parked on an empty list, woken by a push on another connection.
special_blpop() { # target
	local target=$1 name=special:blpop-wakeup
	echo "$name" | grep -Eq "$FILTER" || return 0
	control_flush ultima "$CUR_MODE"
	local blk="$WORK/blk.out"
	{ printf 'BLPOP mx.blk 3\n'; sleep 2.5; } | special_client "$target" "$CUR_MODE" >"$blk" 2>&1 &
	local bpid=$!
	sleep 0.8
	printf 'RPUSH mx.blk woke\n' | run_client "$target" "$CUR_MODE" >/dev/null 2>&1
	wait "$bpid" 2>/dev/null
	if grep -q '"mx.blk"' "$blk" && grep -q '"woke"' "$blk"; then
		record pass "$name"
	else
		record fail "$name"
		[ "$VERBOSE" = 1 ] && { echo "--- blocked client:"; cat "$blk"; }
	fi
}

# MONITOR sees another connection's command.
special_monitor() { # target
	local target=$1 name=special:monitor-stream
	echo "$name" | grep -Eq "$FILTER" || return 0
	local mon="$WORK/mon.out"
	{ printf 'MONITOR\n'; sleep 2.5; } | special_client "$target" "$CUR_MODE" >"$mon" 2>&1 &
	local mpid=$!
	sleep 0.8
	if [ "$CUR_MODE" = auth ]; then
		REDISCLI_AUTH=$AUTH_PASS "$REDIS_CLI" -p "$P_RESP" SET mx.mon.k v1 >/dev/null 2>&1
	else
		"$REDIS_CLI" -p "$P_RESP" SET mx.mon.k v1 >/dev/null 2>&1
	fi
	wait "$mpid" 2>/dev/null
	if grep -q '"SET"' "$mon" && grep -q 'mx.mon.k' "$mon"; then
		record pass "$name"
	else
		record fail "$name"
		[ "$VERBOSE" = 1 ] && { echo "--- monitor:"; cat "$mon"; }
	fi
}

# QUIT one-shot (the REPL intercepts lowercase quit/exit, so this also
# verifies the server-side QUIT path).
special_quit() { # target server
	local target=$1 server=$2 name=special:quit
	echo "$name" | grep -Eq "$FILTER" || return 0
	local out
	out=$(run_client_oneshot "$target" "$CUR_MODE" QUIT)
	if [ "$out" = OK ]; then record pass "$name"; else
		record fail "$name"
		[ "$VERBOSE" = 1 ] && echo "--- got: $out"
	fi
}

# Auth-mode-only: an unauthenticated RESP connection is gated with NOAUTH,
# and a wrong password gets WRONGPASS. redis-cli runs commands without a
# password; ultima-cli authenticates during its connect handshake, so the
# wrong-password case surfaces as a connect failure there (both contain the
# server error text).
special_noauth_gate() { # target
	local target=$1 name=special:noauth-gate
	[ "$CUR_MODE" = auth ] || return 0
	echo "$name" | grep -Eq "$FILTER" || return 0
	local out bad brc=0
	case "$target" in
	redis-cli)
		out=$(env -u REDISCLI_AUTH "$REDIS_CLI" --no-raw -p "$P_RESP" GET x 2>&1)
		bad=$(env -u REDISCLI_AUTH "$REDIS_CLI" --no-raw -p "$P_RESP" AUTH wrongpw 2>&1)
		;;
	ultima-cli)
		out=$("$UCLI" -p "$P_RESP" -a wrongpw GET x 2>&1)
		brc=$?
		bad=$out
		;;
	*) return 0 ;;
	esac
	case "$out" in *NOAUTH* | *WRONGPASS*) : ;; *)
		record fail "$name"
		[ "$VERBOSE" = 1 ] && echo "--- noauth: $out"
		return 0
		;;
	esac
	case "$bad" in *WRONGPASS*) record pass "$name" ;; *)
		record fail "$name"
		[ "$VERBOSE" = 1 ] && echo "--- wrongpass: $bad (rc=$brc)"
		;;
	esac
}

# Auth-mode-only: a garbage JWT is rejected at the WS upgrade / gRPC call.
special_bad_token() { # target
	local target=$1 name=special:bad-token
	[ "$CUR_MODE" = auth ] || return 0
	echo "$name" | grep -Eq "$FILTER" || return 0
	local rc=0 out
	case "$target" in
	ultima-ws-cli) out=$("$UWCLI" --addr "127.0.0.1:$P_HTTP" --token garbage.token.here PING 2>&1) || rc=$? ;;
	ultima-grpc-cli) out=$("$UGCLI" --addr "127.0.0.1:$P_GRPC" --token garbage.token.here PING 2>&1) || rc=$? ;;
	*) return 0 ;;
	esac
	if [ "$rc" != 0 ]; then record pass "$name"; else
		record fail "$name"
		[ "$VERBOSE" = 1 ] && echo "--- unexpected success: $out"
	fi
}

run_specials() { # target server
	local target=$1 server=$2
	special_quit "$target" "$server"
	[ "$server" = redis ] && return 0 # streaming specials run against ultima only
	special_pubsub "$target"
	special_psubscribe "$target"
	special_blpop "$target"
	special_monitor "$target"
	special_noauth_gate "$target"
	special_bad_token "$target"
}

# ------------------------------------------------------------------- main ---

for bin in "$REDIS_CLI"; do command -v "$bin" >/dev/null || { echo "error: $bin not found" >&2; exit 2; }; done

NCASES=$(parse_cases)
echo "cli-matrix: $NCASES cases from $CASES_DIR; targets: $TARGETS; modes: $MODES$([ $WITH_REDIS = 1 ] && echo '; +real-redis validation')"

is_ultima_target() { case " $TARGETS " in *" $1 "*) return 0 ;; esac; return 1; }

for CUR_MODE in $MODES; do
	NEED_ULTIMA=0
	for t in $TARGETS; do is_ultima_target "$t" 2>/dev/null; case "$t" in redis | redis3) ;; *) NEED_ULTIMA=1 ;; esac; done
	if [ "$NEED_ULTIMA" = 1 ]; then
		echo "--- starting ultima-server (mode $CUR_MODE, resp :$P_RESP grpc :$P_GRPC http :$P_HTTP)"
		start_ultima "$CUR_MODE"
		for CUR_TARGET in $TARGETS; do
			case "$CUR_TARGET" in redis | redis3) continue ;; esac
			echo "=== target $CUR_TARGET (mode $CUR_MODE)"
			run_suite "$CUR_TARGET" ultima
			run_specials "$CUR_TARGET" ultima
		done
		stop_ultima
	fi
	if [ "$WITH_REDIS" = 1 ]; then
		RUN_R=0
		for t in $TARGETS; do case "$t" in redis | redis3) RUN_R=1 ;; esac; done
		[ "$RUN_R" = 0 ] && { TARGETS="$TARGETS redis redis3"; RUN_R=1; }
		if [ "$RUN_R" = 1 ]; then
			echo "--- starting real redis-server (mode $CUR_MODE, port $P_REDIS)"
			start_redis "$CUR_MODE"
			for CUR_TARGET in $TARGETS; do
				case "$CUR_TARGET" in redis | redis3) ;; *) continue ;; esac
				echo "=== target $CUR_TARGET (mode $CUR_MODE) [validation]"
				run_suite "$CUR_TARGET" redis
				special_quit "$CUR_TARGET" redis
			done
			stop_redis
		fi
	fi
done

echo
echo "cli-matrix: $PASS passed, $FAIL failed, $SKIP skipped"
[ "$FAIL" -gt 0 ] && printf 'failed:%b\n' "$FAILED"
exit "$([ "$FAIL" -gt 0 ] && echo 1 || echo 0)"
