package wssrv

import (
	"net/http/httptest"
	"testing"
)

func TestCheckOrigin(t *testing.T) {
	cases := []struct {
		name   string
		host   string
		origin string
		allow  []string
		want   bool
	}{
		{"no Origin header (non-browser client)", "127.0.0.1:6381", "", nil, true},
		{"same origin", "127.0.0.1:6381", "http://127.0.0.1:6381", nil, true},
		{"same origin, case-insensitive host", "example.com:6381", "http://EXAMPLE.com:6381", nil, true},
		{"cross origin rejected", "127.0.0.1:6381", "https://evil.example.com", nil, false},
		{"cross origin, host-only mismatch", "127.0.0.1:6381", "http://127.0.0.1:9999", nil, false},
		{"allowlisted full origin", "127.0.0.1:6381", "https://admin.example.com", []string{"https://admin.example.com"}, true},
		{"allowlisted host", "127.0.0.1:6381", "https://admin.example.com", []string{"admin.example.com"}, true},
		{"allowlist entry does not cover others", "127.0.0.1:6381", "https://evil.example.com", []string{"admin.example.com"}, false},
		{"wildcard", "127.0.0.1:6381", "https://anything.example.com", []string{"*"}, true},
		{"garbage Origin rejected", "127.0.0.1:6381", "not a url", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/ws/v1", nil)
			r.Host = tc.host
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := checkOrigin(r, tc.allow); got != tc.want {
				t.Errorf("checkOrigin(Origin=%q, Host=%q, allow=%v) = %v, want %v",
					tc.origin, tc.host, tc.allow, got, tc.want)
			}
		})
	}
}
