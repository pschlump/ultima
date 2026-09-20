// Package ultima REST client for the management API (design doc §10.1;
// the contract is
// api/openapi.yaml): auth (login/refresh/logout/password), info, shards,
// keyspace browsing (keys/scan, key get/delete), runtime config get/put
// and admin account CRUD. stdlib net/http only; JWT bearer per §9.3.
package ultima

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RESTClient talks to the HTTP management surface (§10.1).
type RESTClient struct {
	base      string // http://host:port
	hc        *http.Client
	tokenFunc func(ctx context.Context) (string, error)
}

// RESTOption configures NewRESTClient.
type RESTOption func(*RESTClient)

// WithRESTToken authenticates requests with a static access token.
func WithRESTToken(token string) RESTOption {
	return WithRESTTokenFunc(func(context.Context) (string, error) { return token, nil })
}

// WithRESTTokenFunc authenticates requests with a provider consulted per
// request (a TokenManager's Token method, so refreshed tokens are used).
func WithRESTTokenFunc(f func(ctx context.Context) (string, error)) RESTOption {
	return func(c *RESTClient) { c.tokenFunc = f }
}

// WithRESTHTTPClient overrides the http.Client (timeouts, transport).
func WithRESTHTTPClient(hc *http.Client) RESTOption {
	return func(c *RESTClient) { c.hc = hc }
}

// NewRESTClient builds a client for the management API at addr
// ("host:port", or a full http:// URL).
func NewRESTClient(addr string, opts ...RESTOption) *RESTClient {
	base := addr
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	c := &RESTClient{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Timeout: 30 * time.Second},
	}
	for _, f := range opts {
		f(c)
	}
	return c
}

// RESTError is a non-2xx management-API reply; Code carries the body's
// "error" field (the spec's Error schema).
type RESTError struct {
	StatusCode int
	Code       string
}

func (e *RESTError) Error() string {
	return fmt.Sprintf("ultima: http %d: %s", e.StatusCode, e.Code)
}

// TokenPair is the login/refresh reply (§9.3): an EdDSA JWT access token
// plus a rotating refresh token (D20).
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// Info is INFO rendered as structured JSON: section → field → value.
type Info struct {
	Sections map[string]map[string]string `json:"sections"`
}

// ShardStat is one shard's introspection row (§10.1 /shards).
type ShardStat struct {
	Index      int   `json:"index"`
	Keys       int   `json:"keys"`
	Expires    int   `json:"expires"`
	MemBytes   int64 `json:"mem_bytes"`
	QueueDepth int   `json:"queue_depth"`
	ExpiryHeap int   `json:"expiry_heap"`
}

// ScanResult is one SCAN step (§10.1 /keys/scan); Cursor "0" = done.
type ScanResult struct {
	Cursor string   `json:"cursor"`
	Keys   []string `json:"keys"`
}

// KeyPreview is the typed, bounded value preview of one key
// (§10.1 /key/{key}). Value is type-shaped: string → string, hash →
// map[string]string, list/set → []string, zset →
// []struct{Member, Score string} (score string2d-exact).
type KeyPreview struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	TTLms int64  `json:"ttl_ms"`
	Value any    `json:"value"`
}

// AccountView is one auth account (§9.5).
type AccountView struct {
	Username    string `json:"username"`
	Class       string `json:"class"`
	TotpEnabled bool   `json:"totp_enabled"`
	Disabled    bool   `json:"disabled"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// do performs one JSON round-trip; a non-2xx status becomes *RESTError.
func (c *RESTClient) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.tokenFunc != nil {
		tok, err := c.tokenFunc(ctx)
		if err != nil {
			return err
		}
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Err string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Err == "" {
			e.Err = http.StatusText(resp.StatusCode)
		}
		return &RESTError{StatusCode: resp.StatusCode, Code: e.Err}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// --- auth (§9.3) --------------------------------------------------------------

// Login — POST /api/v1/auth/login (public). totp is the current 6-digit
// code, required when the account has TOTP enabled.
func (c *RESTClient) Login(ctx context.Context, username, password, totp string) (*TokenPair, error) {
	var out TokenPair
	err := c.do(ctx, http.MethodPost, "/api/v1/auth/login", nil,
		map[string]string{"username": username, "password": password, "totp": totp}, &out)
	return &out, err
}

// Refresh — POST /api/v1/auth/refresh (public): rotates the refresh token
// (the old one is consumed).
func (c *RESTClient) Refresh(ctx context.Context, refreshToken string) (*TokenPair, error) {
	var out TokenPair
	err := c.do(ctx, http.MethodPost, "/api/v1/auth/refresh", nil,
		map[string]string{"refresh_token": refreshToken}, &out)
	return &out, err
}

// Logout — POST /api/v1/auth/logout: revokes the refresh-token family.
func (c *RESTClient) Logout(ctx context.Context, refreshToken string) error {
	return c.do(ctx, http.MethodPost, "/api/v1/auth/logout", nil,
		map[string]string{"refresh_token": refreshToken}, nil)
}

// ChangePassword — POST /api/v1/auth/password (revokes existing sessions).
func (c *RESTClient) ChangePassword(ctx context.Context, current, newPW, totp string) error {
	return c.do(ctx, http.MethodPost, "/api/v1/auth/password", nil,
		map[string]string{"current_password": current, "new_password": newPW, "totp": totp}, nil)
}

// --- server -----------------------------------------------------------------

// Ping — GET /api/v1/ping (PONG).
func (c *RESTClient) Ping(ctx context.Context) error {
	var out struct {
		Message string `json:"message"`
	}
	return c.do(ctx, http.MethodGet, "/api/v1/ping", nil, nil, &out)
}

// Info — GET /api/v1/info.
func (c *RESTClient) Info(ctx context.Context) (*Info, error) {
	var out Info
	err := c.do(ctx, http.MethodGet, "/api/v1/info", nil, nil, &out)
	return &out, err
}

// Shards — GET /api/v1/shards.
func (c *RESTClient) Shards(ctx context.Context) ([]ShardStat, error) {
	var out struct {
		Shards []ShardStat `json:"shards"`
	}
	err := c.do(ctx, http.MethodGet, "/api/v1/shards", nil, nil, &out)
	return out.Shards, err
}

// --- data -------------------------------------------------------------------

// Scan — GET /api/v1/keys/scan. cursor "0" starts; match "" matches all;
// count <= 0 omits the COUNT hint; db selects the logical DB.
func (c *RESTClient) Scan(ctx context.Context, cursor, match string, count, db int) (*ScanResult, error) {
	q := url.Values{"cursor": {cursor}, "db": {strconv.Itoa(db)}}
	if match != "" {
		q.Set("match", match)
	}
	if count > 0 {
		q.Set("count", strconv.Itoa(count))
	}
	var out ScanResult
	err := c.do(ctx, http.MethodGet, "/api/v1/keys/scan", q, nil, &out)
	return &out, err
}

// GetKey — GET /api/v1/key/{key}; a missing key is a 404 *RESTError.
func (c *RESTClient) GetKey(ctx context.Context, key string, db int) (*KeyPreview, error) {
	q := url.Values{"db": {strconv.Itoa(db)}}
	var out KeyPreview
	err := c.do(ctx, http.MethodGet, "/api/v1/key/"+url.PathEscape(key), q, nil, &out)
	return &out, err
}

// DeleteKey — DELETE /api/v1/key/{key}; reports whether the key existed.
func (c *RESTClient) DeleteKey(ctx context.Context, key string, db int) (bool, error) {
	q := url.Values{"db": {strconv.Itoa(db)}}
	var out struct {
		Deleted bool `json:"deleted"`
	}
	err := c.do(ctx, http.MethodDelete, "/api/v1/key/"+url.PathEscape(key), q, nil, &out)
	return out.Deleted, err
}

// --- config -----------------------------------------------------------------

// GetConfig — GET /api/v1/config (CONFIG GET * equivalent).
func (c *RESTClient) GetConfig(ctx context.Context) (map[string]string, error) {
	var out struct {
		Entries map[string]string `json:"entries"`
	}
	err := c.do(ctx, http.MethodGet, "/api/v1/config", nil, nil, &out)
	return out.Entries, err
}

// PutConfig — PUT /api/v1/config (CONFIG SET equivalent; all entries
// applied or none).
func (c *RESTClient) PutConfig(ctx context.Context, entries map[string]string) error {
	return c.do(ctx, http.MethodPut, "/api/v1/config", nil,
		map[string]any{"entries": entries}, nil)
}

// --- admin (§9.5) -------------------------------------------------------------

// ListUsers — GET /api/v1/admin/users (admin only).
func (c *RESTClient) ListUsers(ctx context.Context) ([]AccountView, error) {
	var out struct {
		Users []AccountView `json:"users"`
	}
	err := c.do(ctx, http.MethodGet, "/api/v1/admin/users", nil, nil, &out)
	return out.Users, err
}

// CreateUser — POST /api/v1/admin/users; class is "admin" or "data".
func (c *RESTClient) CreateUser(ctx context.Context, username, password, class string) (*AccountView, error) {
	var out AccountView
	err := c.do(ctx, http.MethodPost, "/api/v1/admin/users", nil,
		map[string]string{"username": username, "password": password, "class": class}, &out)
	return &out, err
}

// UpdateUser — PUT /api/v1/admin/users/{name}; class "" leaves it
// unchanged, disabled nil likewise.
func (c *RESTClient) UpdateUser(ctx context.Context, name, class string, disabled *bool) (*AccountView, error) {
	body := map[string]any{}
	if class != "" {
		body["class"] = class
	}
	if disabled != nil {
		body["disabled"] = *disabled
	}
	var out AccountView
	err := c.do(ctx, http.MethodPut, "/api/v1/admin/users/"+url.PathEscape(name), nil, body, &out)
	return &out, err
}

// DeleteUser — DELETE /api/v1/admin/users/{name}.
func (c *RESTClient) DeleteUser(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/api/v1/admin/users/"+url.PathEscape(name), nil, nil, nil)
}

// RevokeSessions — POST /api/v1/admin/users/{name}/revoke-sessions.
func (c *RESTClient) RevokeSessions(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPost,
		"/api/v1/admin/users/"+url.PathEscape(name)+"/revoke-sessions", nil, nil, nil)
}
