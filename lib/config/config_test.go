package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ultima.cfg.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFromFileAppliesDefaults(t *testing.T) {
	var cfg Config
	if err := FromFile(writeCfg(t, `{}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.RespAddr != ":6379" {
		t.Errorf("resp_addr = %q, want :6379", cfg.Server.RespAddr)
	}
	if cfg.Server.GrpcAddr != ":6380" {
		t.Errorf("grpc_addr = %q, want :6380", cfg.Server.GrpcAddr)
	}
	if cfg.Server.HTTPAddr != ":6381" {
		t.Errorf("http_addr = %q, want :6381", cfg.Server.HTTPAddr)
	}
	if cfg.Server.ShardCount != 0 {
		t.Errorf("shard_count = %d, want 0", cfg.Server.ShardCount)
	}
	if cfg.Server.LogLevel != "info" {
		t.Errorf("log_level = %q, want info", cfg.Server.LogLevel)
	}
}

func TestFromFileFileOverridesDefaults(t *testing.T) {
	var cfg Config
	err := FromFile(writeCfg(t, `{
		"server": {
			"resp_addr": "127.0.0.1:16379",
			"shard_count": 32,
			"log_level": "debug"
		}
	}`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.RespAddr != "127.0.0.1:16379" {
		t.Errorf("resp_addr = %q, want 127.0.0.1:16379", cfg.Server.RespAddr)
	}
	if cfg.Server.ShardCount != 32 {
		t.Errorf("shard_count = %d, want 32", cfg.Server.ShardCount)
	}
	if cfg.Server.LogLevel != "debug" {
		t.Errorf("log_level = %q, want debug", cfg.Server.LogLevel)
	}
	// Untouched fields keep their defaults.
	if cfg.Server.GrpcAddr != ":6380" {
		t.Errorf("grpc_addr = %q, want default :6380", cfg.Server.GrpcAddr)
	}
}

func TestFromFileEnvSubstitution(t *testing.T) {
	t.Setenv("ultima_test_password", "s3cr3t")
	var cfg Config
	err := FromFile(writeCfg(t, `{
		"server": { "requirepass": "$ENV$ultima_test_password" }
	}`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.RequirePass != "s3cr3t" {
		t.Errorf("requirepass = %q, want s3cr3t", cfg.Server.RequirePass)
	}
}

func TestFromFileEnvSubstitutionUnsetVar(t *testing.T) {
	var cfg Config
	err := FromFile(writeCfg(t, `{
		"server": { "requirepass": "$ENV$ultima_test_definitely_unset" }
	}`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.RequirePass != "" {
		t.Errorf("requirepass = %q, want empty for unset env var", cfg.Server.RequirePass)
	}
}

func TestFromFileMissingFile(t *testing.T) {
	var cfg Config
	if err := FromFile(filepath.Join(t.TempDir(), "nope.json"), &cfg); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestCheckStartupPostureWarnsOnWideOpenRESP(t *testing.T) {
	var cfg Config
	if err := SetDefaults(&cfg); err != nil {
		t.Fatal(err)
	}
	// Default ":6379" binds all interfaces with no auth/TLS: must not panic,
	// and bindsAllInterfaces must agree it is wide open.
	if !bindsAllInterfaces(cfg.Server.RespAddr) {
		t.Errorf("bindsAllInterfaces(%q) = false, want true", cfg.Server.RespAddr)
	}
	cfg.CheckStartupPosture(slog.Default())

	cfg.Server.RespAddr = "127.0.0.1:6379"
	if bindsAllInterfaces(cfg.Server.RespAddr) {
		t.Error("bindsAllInterfaces(127.0.0.1:6379) = true, want false")
	}
	cfg.CheckStartupPosture(slog.Default())
}

func TestLogLevelValue(t *testing.T) {
	var cfg Config
	if err := SetDefaults(&cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.LogLevelValue(); got != slog.LevelInfo {
		t.Errorf("default level = %v, want info", got)
	}
	cfg.Server.LogLevel = "DEBUG"
	if got := cfg.LogLevelValue(); got != slog.LevelDebug {
		t.Errorf("level = %v, want debug", got)
	}
	cfg.Server.LogLevel = "bogus"
	if got := cfg.LogLevelValue(); got != slog.LevelInfo {
		t.Errorf("bogus level = %v, want info fallback", got)
	}
}
