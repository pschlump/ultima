// Package ultima token manager (design doc §9.3): login with
// username/password/(optional TOTP), proactive refresh shortly before the
// access token's exp claim, re-login when the refresh family is dead, and
// a one-time probe for servers running with auth.disabled (then Token is
// a no-op returning ""). All three surfaces consume it: REST via
// WithRESTTokenFunc, gRPC via WithGRPCTokenFunc, WS via
// WSOptions.TokenProvider.
package ultima

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Credentials are the login material for TokenManager.
type Credentials struct {
	Username string
	Password string
	// TOTP is the current 6-digit code for accounts with TOTP enabled
	// (§9.3). Note it is a point-in-time code: a TokenManager holding one
	// can re-login only while the code is still in its validity window;
	// long-lived processes should rely on refresh-token rotation instead.
	TOTP string
}

// refreshSkew is how long before the JWT exp claim Token proactively
// refreshes instead of handing out the nearly-dead token.
const refreshSkew = 30 * time.Second

// TokenManager hands out valid access tokens, refreshing or re-logging in
// as needed. Goroutine-safe.
type TokenManager struct {
	rest  *RESTClient // its own client with no token func (login/refresh are public)
	creds Credentials

	mu       sync.Mutex
	probed   bool // auth-disabled probe has a definitive answer
	disabled bool // server runs with auth.enabled = false
	access   string
	refresh  string
	exp      time.Time
}

// NewTokenManager builds a manager that logs in against the management
// API at httpAddr ("host:port" or full URL) with creds.
func NewTokenManager(httpAddr string, creds Credentials) *TokenManager {
	return &TokenManager{rest: NewRESTClient(httpAddr), creds: creds}
}

// Token returns a valid access token, or "" when the server has auth
// disabled (probed once via GET /api/v1/info: reachable without a token →
// auth disabled; 401 → enabled).
func (m *TokenManager) Token(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.probed {
		if err := m.probe(ctx); err != nil {
			return "", err // network failure: probe again next call
		}
	}
	if m.disabled {
		return "", nil
	}
	if m.access != "" && time.Now().Before(m.exp.Add(-refreshSkew)) {
		return m.access, nil
	}
	if m.refresh != "" {
		pair, err := m.rest.Refresh(ctx, m.refresh)
		if err == nil {
			m.store(pair)
			return m.access, nil
		}
		var re *RESTError
		if !errors.As(err, &re) || re.StatusCode != 401 {
			return "", err // transient failure; keep the family
		}
		// 401: the family is dead (rotation theft, revocation, expiry) —
		// fall through to a fresh login.
	}
	pair, err := m.rest.Login(ctx, m.creds.Username, m.creds.Password, m.creds.TOTP)
	if err != nil {
		return "", err
	}
	m.store(pair)
	return m.access, nil
}

// probe determines once whether the server requires auth at all.
func (m *TokenManager) probe(ctx context.Context) error {
	_, err := m.rest.Info(ctx)
	if err == nil {
		m.probed, m.disabled = true, true
		return nil
	}
	var re *RESTError
	if errors.As(err, &re) {
		m.probed = true // any definitive HTTP answer: auth is on
		return nil
	}
	return err
}

// store installs a fresh token pair and decodes the access token's exp
// claim (unverified — verification is the server's job) for the
// proactive-refresh window; undecodable tokens fall back to expires_in.
func (m *TokenManager) store(pair *TokenPair) {
	m.access = pair.AccessToken
	m.refresh = pair.RefreshToken
	m.exp = time.Now().Add(time.Duration(pair.ExpiresIn) * time.Second)
	var claims jwt.RegisteredClaims
	if _, _, err := jwt.NewParser().ParseUnverified(pair.AccessToken, &claims); err == nil && claims.ExpiresAt != nil {
		m.exp = claims.ExpiresAt.Time
	}
}
