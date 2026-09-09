// Package auth implements the M6a account/login layer (design doc §9):
// administrative and data accounts with bcrypt password hashes, Ed25519
// (EdDSA) JWT access tokens (D20), rotating refresh-token families with
// theft detection, and optional TOTP two-factor authentication via
// github.com/pschlump/htotp (D17). The HTTP endpoints live in
// lib/handler/auth.go; gRPC and WebSocket enforcement hook the same
// Service from lib/grpcsrv and lib/wssrv. RESP AUTH stays independent
// (§10.1).
package auth

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// LoadKeys reads the Ed25519 key pair from the configured PEM files
// (D20): pkcs#8 private key for signing, PKIX/SPKI public key for
// verifying. Both files must exist and parse — there is no
// auto-generation, since a silently regenerated pair would invalidate
// every outstanding token. Generate a pair with bin/gen-jwt-keys.sh.
func LoadKeys(privatePath, publicPath string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	privPEM, err := os.ReadFile(privatePath)
	if err != nil {
		return nil, nil, fmt.Errorf("auth: reading private key %s: %w (generate a pair with bin/gen-jwt-keys.sh)", privatePath, err)
	}
	block, _ := pem.Decode(privPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("auth: %s: no PEM block found", privatePath)
	}
	privAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("auth: %s: parsing PKCS#8 private key: %w", privatePath, err)
	}
	priv, ok := privAny.(ed25519.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("auth: %s: not an Ed25519 private key (got %T)", privatePath, privAny)
	}

	pubPEM, err := os.ReadFile(publicPath)
	if err != nil {
		return nil, nil, fmt.Errorf("auth: reading public key %s: %w", publicPath, err)
	}
	block, _ = pem.Decode(pubPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("auth: %s: no PEM block found", publicPath)
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("auth: %s: parsing PKIX public key: %w", publicPath, err)
	}
	pub, ok := pubAny.(ed25519.PublicKey)
	if !ok {
		return nil, nil, fmt.Errorf("auth: %s: not an Ed25519 public key (got %T)", publicPath, pubAny)
	}

	if !priv.Public().(ed25519.PublicKey).Equal(pub) {
		return nil, nil, fmt.Errorf("auth: %s and %s are not a matching key pair", privatePath, publicPath)
	}
	return priv, pub, nil
}
