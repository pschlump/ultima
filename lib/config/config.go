// Package config loads Ultima's JSON configuration, modeled on the exsms
// pattern (design doc §8): `default:"..."` struct tags applied via
// reflection before unmarshal (so the file overrides defaults), and
// `$ENV$NAME` substitution in string values.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Config is the root configuration. Groups are nested structs so
// SetDefaults recursion reaches every field.
type Config struct {
	Server  ServerConfig  `json:"server"`
	Persist PersistConfig `json:"persist"`
	Auth    AuthConfig    `json:"auth"`
	Debug   DebugConfig   `json:"debug"`
}

// PersistConfig is the persistence section (M5c, §13.1): Redis-parity
// names (dir, dbfilename, appenddirname, appendonly, appendfsync, save)
// plus the Ultima-only snapshot_compress (D9 own formats).
type PersistConfig struct {
	Dir              string `json:"dir" default:"./data"`
	DbFilename       string `json:"dbfilename" default:"dump.rdb"`
	AppendDirname    string `json:"appenddirname" default:"appendonlydir"`
	AppendOnly       bool   `json:"appendonly" default:"false"`
	AppendFsync      string `json:"appendfsync" default:"everysec"` // always | everysec | no
	Save             string `json:"save" default:"3600 1 300 100 60 10000"`
	SnapshotCompress bool   `json:"snapshot_compress" default:"false"`
}

// ServerConfig holds the three listener addresses and server knobs.
type ServerConfig struct {
	RespAddr string `json:"resp_addr" default:":6379"`
	GrpcAddr string `json:"grpc_addr" default:":6380"`
	HTTPAddr string `json:"http_addr" default:":6381"`
	// HTTPAddrs is a comma-separated list of ADDITIONAL HTTP listen
	// addresses bound alongside http_addr (the HTTP/WS surface listens on
	// all of them at once). Entries may be full host:port ("[::1]:6381")
	// or bare IPs/hostnames ("192.168.1.143", "::1"), which inherit
	// http_addr's port. An entry whose IP is not on any local interface
	// (e.g. a laptop that moved networks) is skipped with a startup
	// warning; other bind failures are fatal.
	HTTPAddrs   string `json:"http_addrs" default:""`
	ShardCount  int    `json:"shard_count" default:"0"`   // 0 = 4×GOMAXPROCS, power of two
	MaxDBs      int    `json:"max_dbs" default:"16"`      // logical DBs for SELECT (§13.3)
	MaxMemoryMB int    `json:"max_memory_mb" default:"0"` // 0 = unlimited (CONFIG maxmemory)
	LogLevel    string `json:"log_level" default:"info"`
	RequirePass string `json:"requirepass" default:""`
	RespTLS     bool   `json:"resp_tls_enabled" default:"false"`
	// NotifyKeyspaceEvents mirrors Redis's notify-keyspace-events (M5a);
	// "" = off. Validated at startup against the Redis class letters.
	NotifyKeyspaceEvents string `json:"notify_keyspace_events" default:""`
	// MaxMemoryPolicy mirrors Redis's maxmemory-policy (M5b): one of the
	// eight Redis policy names; validated at startup.
	MaxMemoryPolicy string `json:"maxmemory_policy" default:"noeviction"`
	// MetricsAllow is the comma-separated CIDR/IP allowlist guarding
	// GET /metrics (M6c, §10.1); parsed into []*net.IPNet at startup,
	// bare IPs become /32 or /128.
	MetricsAllow string `json:"metrics_allow" default:"127.0.0.0/8,::1"`
	// WSOriginAllow is the comma-separated Origin allowlist for the
	// /ws/v1 upgrade (M6d, §10.2), consulted only when auth.enabled:
	// entries match a full Origin ("https://admin.example.com") or its
	// host, "*" allows any origin. Same-origin upgrades and requests
	// without an Origin header always pass.
	WSOriginAllow string `json:"ws_origin_allow" default:""`
}

// HTTPListenAddrs resolves every address the HTTP surface binds: http_addr
// first, then each http_addrs entry, with bare IPs/hostnames (no port)
// inheriting http_addr's port.
func (c ServerConfig) HTTPListenAddrs() ([]string, error) {
	_, port, err := net.SplitHostPort(c.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("http_addr %q: %w", c.HTTPAddr, err)
	}
	out := []string{c.HTTPAddr}
	for _, part := range strings.Split(c.HTTPAddrs, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(part); err == nil {
			out = append(out, part)
			continue
		}
		host := strings.TrimPrefix(strings.TrimSuffix(part, "]"), "[")
		if ip, err := netip.ParseAddr(host); err == nil {
			out = append(out, net.JoinHostPort(ip.String(), port))
			continue
		}
		out = append(out, net.JoinHostPort(host, port))
	}
	return out, nil
}

// DebugConfig holds feature flags (§8 debug.enabled map).
type DebugConfig struct {
	Enabled map[string]bool `json:"enabled"`
}

// AuthConfig is the M6a account/login section (design doc §8/§9, D20).
// When Enabled is false every surface stays open and none of the other
// fields are consulted. Durations are strings parsed with
// time.ParseDuration at startup (SetDefaults has no duration kind).
type AuthConfig struct {
	Enabled bool `json:"enabled" default:"false"`
	// Ed25519 key pair (PEM: PKCS#8 private, PKIX/SPKI public), generated
	// by bin/gen-jwt-keys.sh. Both must exist and parse when Enabled.
	JwtPrivateKeyFile string `json:"jwt_private_key_file" default:"./keys/ultima-jwt.pem"`
	JwtPublicKeyFile  string `json:"jwt_public_key_file" default:"./keys/ultima-jwt.pub"`
	AccessTokenTTL    string `json:"access_token_ttl" default:"15m"`
	RefreshTokenTTL   string `json:"refresh_token_ttl" default:"720h"`
	TotpIssuer        string `json:"totp_issuer" default:"Ultima"`
	TotpSkew          int    `json:"totp_skew" default:"1"`
	// AccountsFile defaults to <persist.dir>/accounts.json when empty.
	AccountsFile string `json:"accounts_file" default:""`
	// BootstrapAdminPassword seeds the built-in admin account on first
	// boot (no accounts file yet); empty = generate a random password and
	// log it once. Carries "$ENV$NAME" via the usual substitution.
	BootstrapAdminPassword string `json:"bootstrap_admin_password" default:""`
	// WS session replay-buffer bounds (§9.4) — consumed by M6b.
	WSReplayBufferMs      int `json:"ws_replay_buffer_ms" default:"30000"`
	WSReplayBufferMaxMsgs int `json:"ws_replay_buffer_max_msgs" default:"10000"`
}

var envRefRe = regexp.MustCompile(`\$ENV\$([A-Za-z_][A-Za-z0-9_]*)`)

// FromFile applies struct-tag defaults, then loads the JSON file at path
// over them (file values win), expanding `$ENV$NAME` references in the
// file's string values from the process environment.
func FromFile(path string, cfg *Config) error {
	if err := SetDefaults(cfg); err != nil {
		return fmt.Errorf("config: applying defaults: %w", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: reading %s: %w", path, err)
	}
	raw = substituteEnvRefs(raw)
	if err := json.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("config: parsing %s: %w", path, err)
	}
	return nil
}

// substituteEnvRefs replaces each `$ENV$NAME` token with the value of
// environment variable NAME (empty string if unset).
func substituteEnvRefs(raw []byte) []byte {
	return envRefRe.ReplaceAllFunc(raw, func(m []byte) []byte {
		name := envRefRe.FindSubmatch(m)[1]
		return []byte(os.Getenv(string(name)))
	})
}

// SetDefaults walks cfg (a pointer to struct) and sets any zero-valued
// field that carries a `default:"..."` tag. Nested structs are recursed.
// Supported field kinds: string, bool, all int/uint widths, float32/64.
func SetDefaults(cfg any) error {
	v := reflect.ValueOf(cfg)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("config: SetDefaults requires a pointer to struct, got %T", cfg)
	}
	return setDefaults(v.Elem())
}

func setDefaults(v reflect.Value) error {
	t := v.Type()
	for i := range v.NumField() {
		f := v.Field(i)
		tag := t.Field(i).Tag.Get("default")
		switch {
		case f.Kind() == reflect.Struct:
			if err := setDefaults(f); err != nil {
				return err
			}
		case tag == "" || !f.IsZero():
			// no default declared, or caller already set a value
		default:
			if err := setField(f, t.Field(i).Name, tag); err != nil {
				return err
			}
		}
	}
	return nil
}

func setField(f reflect.Value, name, tag string) error {
	switch f.Kind() {
	case reflect.String:
		f.SetString(tag)
	case reflect.Bool:
		b, err := strconv.ParseBool(tag)
		if err != nil {
			return fmt.Errorf("config: field %s: bad default %q: %w", name, tag, err)
		}
		f.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(tag, 10, 64)
		if err != nil {
			return fmt.Errorf("config: field %s: bad default %q: %w", name, tag, err)
		}
		f.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(tag, 10, 64)
		if err != nil {
			return fmt.Errorf("config: field %s: bad default %q: %w", name, tag, err)
		}
		f.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(tag, 64)
		if err != nil {
			return fmt.Errorf("config: field %s: bad default %q: %w", name, tag, err)
		}
		f.SetFloat(n)
	default:
		return fmt.Errorf("config: field %s: default tag on unsupported kind %s", name, f.Kind())
	}
	return nil
}

// CheckStartupPosture warns (and later milestones will refuse) on obviously
// unsafe configurations, in the spirit of exsms's checkStartupPosture
// (design doc §8). For M0 it logs warnings and never fails startup.
func (c *Config) CheckStartupPosture(logger *slog.Logger) {
	if bindsAllInterfaces(c.Server.RespAddr) && c.Server.RequirePass == "" && !c.Server.RespTLS {
		logger.Warn("unsafe startup posture: RESP listener is bound to all interfaces with no requirepass and no TLS",
			"resp_addr", c.Server.RespAddr)
	}
	switch strings.ToLower(c.Server.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		logger.Warn("unknown log_level, falling back to info", "log_level", c.Server.LogLevel)
	}
}

// bindsAllInterfaces reports whether addr listens on every interface
// (":6379", "0.0.0.0:6379", "[::]:6379").
func bindsAllInterfaces(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// LogLevelValue maps the configured level string onto slog.Level,
// defaulting to info for anything unrecognized.
func (c *Config) LogLevelValue() slog.Level {
	switch strings.ToLower(c.Server.LogLevel) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
