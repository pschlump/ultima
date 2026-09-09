package auth

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// tokenIssuer is the JWT iss claim for Ultima-issued access tokens.
const tokenIssuer = "ultima"

// Claims is the access-token payload (§9.3): the username in Subject,
// the account class, and the standard registered claims. There are no
// data-surface permissions in the token yet — ACL identity arrives with
// the ACL milestone.
type Claims struct {
	Class Class `json:"class"`
	jwt.RegisteredClaims
}

// issueAccess signs a short-lived EdDSA access token for the account.
func (s *Service) issueAccess(a *Account) (string, error) {
	now := time.Now().UTC()
	jti, err := newToken()
	if err != nil {
		return "", err
	}
	claims := Claims{
		Class: a.Class,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    tokenIssuer,
			Subject:   a.Username,
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.accessTTL)),
		},
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(s.priv)
	if err != nil {
		return "", fmt.Errorf("auth: signing access token: %w", err)
	}
	return tok, nil
}

// ErrInvalidAccess marks any access-token verification failure; the HTTP
// layer maps it to 401 without disclosing the reason.
var ErrInvalidAccess = errors.New("auth: invalid access token")

// VerifyAccess validates an access token's Ed25519 signature, expiry,
// and issuer, then checks the account still exists and is enabled — a
// deleted or disabled account's tokens die immediately rather than
// living out their TTL.
func (s *Service) VerifyAccess(tok string) (Identity, error) {
	var claims Claims
	_, err := jwt.ParseWithClaims(tok, &claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodEdDSA {
			return nil, fmt.Errorf("auth: unexpected signing method %q", t.Method.Alg())
		}
		return ed25519.PublicKey(s.pub), nil
	},
		jwt.WithIssuer(tokenIssuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return Identity{}, ErrInvalidAccess
	}
	if claims.Subject == "" || (claims.Class != ClassAdmin && claims.Class != ClassData) {
		return Identity{}, ErrInvalidAccess
	}
	a, ok := s.store.Get(claims.Subject)
	if !ok || a.Disabled {
		return Identity{}, ErrInvalidAccess
	}
	return Identity{Username: a.Username, Class: a.Class}, nil
}
