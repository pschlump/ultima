// M8d soak: a long-running mixed-corpus hammer over the scripting engine
// (the "overnight soak" of docs/m8-detailed-plan.md). In-process server,
// N workers cycling through writes, reads, cache hits, conversion-heavy
// returns, pcall/guard/compile/read-only error classes, and occasional
// CPU-spin scripts, while a control connection randomizes
// lua-time-limit. RSS of the process is sampled; the end-of-run RSS must
// stay within a slack band of the post-warmup sample (leak trip-wire).
//
// Skipped unless SOAK=1. Knobs: SOAK_SECONDS (default 120), SOAK_WORKERS
// (default 8).
//
//	SOAK=1 SOAK_SECONDS=28800 go test ./tests -run TestM8Soak -timeout 24h
package tests

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/scripting"
)

// soakRSSKB samples the process RSS via ps (getrusage's maxrss is a
// monotone high-water mark — useless for leak detection).
func soakRSSKB(t *testing.T) int64 {
	t.Helper()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatalf("ps rss: %v", err)
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatalf("ps rss parse %q: %v", out, err)
	}
	return kb
}

func TestM8Soak(t *testing.T) {
	if os.Getenv("SOAK") != "1" {
		t.Skip("SOAK=1 not set")
	}
	secs, _ := strconv.Atoi(os.Getenv("SOAK_SECONDS"))
	if secs <= 0 {
		secs = 120
	}
	workers, _ := strconv.Atoi(os.Getenv("SOAK_WORKERS"))
	if workers <= 0 {
		workers = 8
	}
	addr, _ := m8RESP(t, scripting.Config{LuaTimeLimitMs: 5000, HardDeadlineMs: 30000})
	deadline := time.Now().Add(time.Duration(secs) * time.Second)

	// preload one script for the EVALSHA leg
	ctrl := m3Dial(t, addr)
	ctrl.send(t, "SCRIPT", "LOAD", "return redis.call('incr','soak:ctr')")
	loadReply := ctrl.recv(t) // "+<sha>" — m3Client renders blob/sha raw
	sha := strings.TrimPrefix(loadReply, "+")

	type stats struct {
		replies, errors int64
	}
	var wg sync.WaitGroup
	all := make([]*stats, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		all[w] = &stats{}
		go func(w int) {
			defer wg.Done()
			st := all[w]
			c, err := m3DialE(addr)
			if err != nil {
				t.Errorf("worker %d dial: %v", w, err)
				return
			}
			defer func() { _ = c.conn.Close() }()
			i := 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("soak:%d:%d", w, i%97)
				var args []string
				var want string // expected reply prefix for error legs
				switch i % 10 {
				case 0: // write
					args = []string{"EVAL", "return redis.call('set',KEYS[1],ARGV[1])", "1", key, "v"}
				case 1: // read
					args = []string{"EVAL", "return redis.call('get',KEYS[1])", "1", key}
				case 2: // cache hit (sha from SCRIPT LOAD is raw text)
					args = []string{"EVALSHA", sha, "0"}
				case 3: // conversion-heavy return
					args = []string{"EVAL", "return {1,2,{3,'four'}}", "0"}
				case 4: // pcall error class
					args = []string{"EVAL", "return redis.pcall('get')", "0"}
					want = "-ERR Wrong number of args calling Redis command from script"
				case 5: // guard: undefined global read
					args = []string{"EVAL", "return undefined_global", "0"}
					want = "-ERR user_script:1: Script attempted to access nonexistent global variable 'undefined_global'"
				case 6: // guard: readonly write
					args = []string{"EVAL", "g = 5", "0"}
					want = "-ERR user_script:1: Attempt to modify a readonly table"
				case 7: // compile error (uncached)
					args = []string{"EVAL", "return 1 +", "0"}
					want = "-ERR Error compiling script (new function):"
				case 8: // read-only rejection
					args = []string{"EVAL_RO", "return redis.call('set','x','y')", "0"}
					want = "-ERR Write commands are not allowed from read-only scripts."
				case 9: // short CPU spin (deadline flag hygiene); a deadline
					// kill under a randomized 100 ms soft limit is legal
					args = []string{"EVAL", "local i=0 while i<50000 do i=i+1 end return i", "0"}
				}
				i++
				if err := c.sendE(args...); err != nil {
					return // server shutting down at test end
				}
				r, err := c.recvE()
				if err != nil {
					return
				}
				st.replies++
				if strings.HasPrefix(r, "-") {
					st.errors++
				}
				// every reply must be in its leg's class; a wrong error
				// text is a regression, not load
				if want != "" && !strings.HasPrefix(r, want) {
					t.Errorf("worker %d leg %d: reply %q, want prefix %q", w, (i-1)%10, r, want)
					return
				}
				if want == "" && strings.HasPrefix(r, "-") &&
					!strings.HasPrefix(r, "-ERR Script killed") {
					t.Errorf("worker %d leg %d: unexpected error %q", w, (i-1)%10, r)
					return
				}
			}
		}(w)
	}

	// control: randomize lua-time-limit, sample RSS
	var samples []int64
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		limit := []string{"100", "500", "5000"}[len(samples)%3]
		ctrl.send(t, "CONFIG", "SET", "lua-time-limit", limit)
		if got := ctrl.recv(t); got != "+OK" {
			t.Fatalf("CONFIG SET lua-time-limit = %q", got)
		}
		samples = append(samples, soakRSSKB(t))
		t.Logf("soak: %d/%d s, rss=%dKB", len(samples)*10, secs, samples[len(samples)-1])
	}
	ctrl.send(t, "CONFIG", "SET", "lua-time-limit", "5000")
	ctrl.recv(t)
	wg.Wait()

	var replies, errors int64
	for _, st := range all {
		replies += st.replies
		errors += st.errors
	}
	t.Logf("soak done: %d replies (%d expected-class errors), rss samples %v", replies, errors, samples)
	if replies == 0 {
		t.Fatal("no replies")
	}
	// leak trip-wire: the tail sample must stay within a slack band of
	// the post-warmup sample (GC scheduling noise rides under the band)
	if len(samples) >= 3 {
		warm := samples[1]
		last := samples[len(samples)-1]
		if last > warm+warm/2+256*1024 {
			t.Errorf("RSS grew past the slack band: warmup %dKB, tail %dKB", warm, last)
		}
	}
}
