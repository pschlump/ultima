// M4 exit criterion: TypeScript client round-trip against the WebSocket
// front-end (design doc §6.3). Boots wsTestServer and drives the
// protobuf-es client script in tests/ts-roundtrip under bun.
package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestTSRoundTrip(t *testing.T) {
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("bun not on PATH; install bun to run the TypeScript round-trip test")
	}
	// `go test` runs with the package directory (tests/) as cwd.
	dir, err := filepath.Abs("ts-roundtrip")
	if err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dir, "node_modules")); err != nil || !st.IsDir() {
		t.Skip("tests/ts-roundtrip/node_modules missing; run `bun install` in tests/ts-roundtrip")
	}

	// Static gate first when tsc is available: the generated gen/ts
	// bindings and the client script must be strict-clean.
	if tsc, err := exec.LookPath("tsc"); err == nil {
		tc := exec.Command(tsc, "--noEmit", "-p", "tsconfig.json")
		tc.Dir = dir
		if out, err := tc.CombinedOutput(); err != nil {
			t.Fatalf("tsc typecheck failed: %v\n%s", err, out)
		}
	} else {
		t.Log("tsc not on PATH; skipping the typecheck step")
	}

	_, wsURL := wsTestServer(t)

	cmd := exec.Command(bun, "roundtrip.ts")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "WS_URL="+wsURL)
	out, err := cmd.CombinedOutput()
	t.Logf("bun roundtrip.ts output:\n%s", out)
	if err != nil {
		t.Fatalf("TypeScript round-trip failed: %v", err)
	}
}
