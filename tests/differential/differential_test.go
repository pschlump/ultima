package differential

import (
	"os"
	"testing"
)

// TestDifferentialP0 runs the full P0 script table against Ultima and a
// real redis-server (REDIS_BIN, default "redis-server") and diffs every
// reply, including error strings.
func TestDifferentialP0(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, p0Scripts, "")
}

// TestDifferentialP1 runs the M2 collection script table (hashes, lists,
// sets, sorted sets) against Ultima and a real redis-server and diffs
// every reply, including error strings.
func TestDifferentialP1(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, p1Scripts, "")
}

// TestDifferentialAuth runs the requirepass script table against a
// password-protected pair of servers.
func TestDifferentialAuth(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, authScripts, "diff-secret-pw")
}

var authScripts = []script{
	{"requirepass-flow", []step{
		cmd("RECONNECT"), // fresh unauthenticated connections on both sides
		cmd("PING"),
		cmd("GET", "x"),
		cmd("SET", "x", "1"),
		cmd("AUTH", "wrong"),
		cmd("AUTH", "default", "wrong"),
		cmd("AUTH", "nouser", "diff-secret-pw"),
		cmd("AUTH", "diff-secret-pw"),
		cmd("PING"),
		cmd("SET", "x", "1"),
		cmd("GET", "x"),
		cmd("RECONNECT"),
		cmd("AUTH", "default", "diff-secret-pw"),
		cmd("GET", "x"),
		cmd("RECONNECT"),
		cmdM(mHello, "HELLO", "3", "AUTH", "default", "diff-secret-pw"),
		cmd("GET", "x"),
		cmd("RECONNECT"),
		cmd("HELLO", "3", "AUTH", "default", "wrong"),
	}},
}
