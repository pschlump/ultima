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
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Config is the root configuration. Groups are nested structs so
// SetDefaults recursion reaches every field.
type Config struct {
	Server ServerConfig `json:"server"`
	Debug  DebugConfig  `json:"debug"`
}

// ServerConfig holds the three listener addresses and M0 server knobs.
type ServerConfig struct {
	RespAddr    string `json:"resp_addr" default:":6379"`
	GrpcAddr    string `json:"grpc_addr" default:":6380"`
	HTTPAddr    string `json:"http_addr" default:":6381"`
	ShardCount  int    `json:"shard_count" default:"0"` // 0 = runtime.NumCPU()
	LogLevel    string `json:"log_level" default:"info"`
	RequirePass string `json:"requirepass" default:""`
	RespTLS     bool   `json:"resp_tls_enabled" default:"false"`
}

// DebugConfig holds feature flags (§8 debug.enabled map).
type DebugConfig struct {
	Enabled map[string]bool `json:"enabled"`
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
