package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Class is the account class (design doc §9.1): administrative accounts
// manage the server and other accounts; data users run commands subject
// to their ACL permissions (ACL enforcement itself is a later milestone).
type Class string

const (
	// ClassAdmin accounts manage the server and other accounts.
	ClassAdmin Class = "admin"
	// ClassData users run commands subject to their ACL permissions.
	ClassData Class = "data"
)

// Account is one login identity (§9.1). PassHash is bcrypt; the
// cleartext never touches the store. TOTP enrollment is two-phase:
// PendingTOTPSecret is staged by enable/regenerate and promoted to
// TOTPSecret (with TOTPEnabled) only after a valid code confirms it.
type Account struct {
	Username          string    `json:"username"`
	Class             Class     `json:"class"`
	PassHash          string    `json:"pass_hash"`
	TOTPSecret        string    `json:"totp_secret,omitempty"`
	TOTPEnabled       bool      `json:"totp_enabled,omitempty"`
	PendingTOTPSecret string    `json:"pending_totp_secret,omitempty"`
	Disabled          bool      `json:"disabled,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// RefreshFamily is one login session's refresh-token lineage (§9.3).
// The refresh token itself is a 256-bit opaque random string; only its
// SHA-256 hash is stored. Rotation moves CurrentHash forward and keeps
// the previous generation in PreviousHash exactly long enough to detect
// reuse: presenting the previous token again means either the client or
// a thief holds it, and there is no way to tell which — so the whole
// family is revoked.
type RefreshFamily struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	CurrentHash  string    `json:"current_hash"`
	PreviousHash string    `json:"previous_hash,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	Revoked      bool      `json:"revoked,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// storeFile is the on-disk layout of the accounts file (JSON, atomic
// tmp+rename writes — same durability philosophy as the M5c snapshot).
type storeFile struct {
	Accounts map[string]*Account       `json:"accounts"`
	Families map[string]*RefreshFamily `json:"refresh_families"`
}

// Sentinel errors mapped to HTTP statuses by lib/handler/auth.go.
var (
	ErrAccountExists    = errors.New("auth: account already exists")
	ErrAccountNotFound  = errors.New("auth: account not found")
	ErrBadCredentials   = errors.New("auth: invalid username or password")
	ErrTOTPRequired     = errors.New("auth: totp code required")
	ErrBadTOTP          = errors.New("auth: invalid totp code")
	ErrNoPendingTOTP    = errors.New("auth: no pending totp enrollment")
	ErrTOTPAlreadyOn    = errors.New("auth: totp already enabled")
	ErrTOTPNotEnabled   = errors.New("auth: totp not enabled")
	ErrLastAdmin        = errors.New("auth: cannot remove the last enabled administrative account")
	ErrInvalidRefresh   = errors.New("auth: invalid or expired refresh token")
	ErrTheftDetected    = errors.New("auth: refresh token reuse detected; session revoked")
	ErrAccountDisabled  = errors.New("auth: account is disabled")
	ErrBootstrapIllegal = errors.New("auth: the bootstrap admin account cannot be deleted")
)

// dummyHash keeps the unknown-user login path on the same bcrypt cost as
// the known-user path, so timing does not reveal which usernames exist.
var dummyHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

// Store is the mutex-guarded account + refresh-family set with
// write-through persistence to a JSON file.
type Store struct {
	mu       sync.Mutex
	path     string
	accounts map[string]*Account
	families map[string]*RefreshFamily
}

// LoadStore reads the accounts file at path. A missing file is a fresh,
// empty store (bootstrap happens in NewService). A malformed file is an
// error — accounts are too security-sensitive to silently reset.
func LoadStore(path string) (*Store, error) {
	s := &Store{
		path:     path,
		accounts: map[string]*Account{},
		families: map[string]*RefreshFamily{},
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("auth: reading accounts file %s: %w", path, err)
	}
	var f storeFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("auth: parsing accounts file %s: %w", path, err)
	}
	if f.Accounts != nil {
		s.accounts = f.Accounts
	}
	if f.Families != nil {
		s.families = f.Families
	}
	return s, nil
}

// Exists reports whether the backing file exists (bootstrap detection).
func (s *Store) Exists() bool {
	_, err := os.Stat(s.path)
	return err == nil
}

// save serializes the store atomically (tmp + rename). Call with mu held.
func (s *Store) save() error {
	raw, err := json.MarshalIndent(storeFile{Accounts: s.accounts, Families: s.families}, "", "  ")
	if err != nil {
		return fmt.Errorf("auth: encoding accounts file: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("auth: creating accounts directory: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("auth: writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("auth: renaming %s over %s: %w", tmp, s.path, err)
	}
	return nil
}

// Get returns a copy of the named account.
func (s *Store) Get(username string) (Account, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[username]
	if !ok {
		return Account{}, false
	}
	return *a, true
}

// List returns all accounts sorted by username.
func (s *Store) List() []Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Account, 0, len(s.accounts))
	for _, a := range s.accounts {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	return out
}

// HashPassword bcrypts a cleartext password at the default cost.
func HashPassword(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("auth: hashing password: %w", err)
	}
	return string(h), nil
}

// Create adds a new account with an already-hashed password.
func (s *Store) Create(username string, class Class, passHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.accounts[username]; ok {
		return ErrAccountExists
	}
	now := time.Now().UTC()
	s.accounts[username] = &Account{
		Username:  username,
		Class:     class,
		PassHash:  passHash,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return s.save()
}

// mutate applies fn to the named account under the lock and saves.
func (s *Store) mutate(username string, fn func(a *Account) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[username]
	if !ok {
		return ErrAccountNotFound
	}
	before := *a
	if err := fn(a); err != nil {
		*a = before
		return err
	}
	a.UpdatedAt = time.Now().UTC()
	return s.save()
}

// SetPassword replaces the account's password hash.
func (s *Store) SetPassword(username, passHash string) error {
	return s.mutate(username, func(a *Account) error {
		a.PassHash = passHash
		return nil
	})
}

// SetClass changes the account class, guarding the last admin.
func (s *Store) SetClass(username string, class Class) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[username]
	if !ok {
		return ErrAccountNotFound
	}
	if a.Class == ClassAdmin && class != ClassAdmin && s.countEnabledAdminsLocked() <= 1 && !a.Disabled {
		return ErrLastAdmin
	}
	a.Class = class
	a.UpdatedAt = time.Now().UTC()
	return s.save()
}

// SetDisabled enables/disables the account, guarding the last admin and
// the bootstrap identity.
func (s *Store) SetDisabled(username string, disabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[username]
	if !ok {
		return ErrAccountNotFound
	}
	if disabled && a.Class == ClassAdmin && !a.Disabled && s.countEnabledAdminsLocked() <= 1 {
		return ErrLastAdmin
	}
	a.Disabled = disabled
	a.UpdatedAt = time.Now().UTC()
	return s.save()
}

// Delete removes the account and all its refresh families. The bootstrap
// admin account (§9.1) cannot be deleted — disable it instead (§9.5).
func (s *Store) Delete(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[username]
	if !ok {
		return ErrAccountNotFound
	}
	if username == BootstrapAdmin {
		return ErrBootstrapIllegal
	}
	if a.Class == ClassAdmin && !a.Disabled && s.countEnabledAdminsLocked() <= 1 {
		return ErrLastAdmin
	}
	delete(s.accounts, username)
	for id, fam := range s.families {
		if fam.Username == username {
			delete(s.families, id)
		}
	}
	return s.save()
}

// countEnabledAdminsLocked counts enabled administrative accounts.
// Call with mu held.
func (s *Store) countEnabledAdminsLocked() int {
	n := 0
	for _, a := range s.accounts {
		if a.Class == ClassAdmin && !a.Disabled {
			n++
		}
	}
	return n
}

// CountEnabledAdmins reports the number of enabled administrative
// accounts (startup-posture warning, §9.5).
func (s *Store) CountEnabledAdmins() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.countEnabledAdminsLocked()
}

// CheckPassword verifies a password against the named account. Unknown
// users get a dummy bcrypt compare so the timing matches the known-user
// path; disabled accounts fail like bad credentials.
func (s *Store) CheckPassword(username, password string) (Account, error) {
	s.mu.Lock()
	a, ok := s.accounts[username]
	var hash string
	if ok {
		hash = a.PassHash
	} else {
		hash = string(dummyHash)
	}
	s.mu.Unlock() // bcrypt is slow; don't hold the lock across it
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil || !ok {
		return Account{}, ErrBadCredentials
	}
	if a.Disabled {
		return Account{}, ErrAccountDisabled
	}
	return *a, nil
}

// StageTOTP stores a pending TOTP secret awaiting confirmation.
func (s *Store) StageTOTP(username, secret string, replacing bool) error {
	return s.mutate(username, func(a *Account) error {
		if a.TOTPEnabled && !replacing {
			return ErrTOTPAlreadyOn
		}
		a.PendingTOTPSecret = secret
		return nil
	})
}

// ConfirmTOTP promotes the pending secret to active.
func (s *Store) ConfirmTOTP(username string) error {
	return s.mutate(username, func(a *Account) error {
		if a.PendingTOTPSecret == "" {
			return ErrNoPendingTOTP
		}
		a.TOTPSecret = a.PendingTOTPSecret
		a.PendingTOTPSecret = ""
		a.TOTPEnabled = true
		return nil
	})
}

// DisableTOTP turns 2FA off and clears both secrets.
func (s *Store) DisableTOTP(username string) error {
	return s.mutate(username, func(a *Account) error {
		if !a.TOTPEnabled {
			return ErrTOTPNotEnabled
		}
		a.TOTPEnabled = false
		a.TOTPSecret = ""
		a.PendingTOTPSecret = ""
		return nil
	})
}

// --- refresh families (§9.3) ---

// newToken returns a 256-bit opaque token, base64url-encoded.
func newToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("auth: generating token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func hashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// NewFamily starts a refresh-token family for username and returns the
// first refresh token. ttl is absolute for the family: rotation slides
// the expiry forward, so an active session never ages out, but a stolen
// or abandoned token dies at ttl after its last use.
func (s *Store) NewFamily(username string, ttl time.Duration) (string, error) {
	tok, err := newToken()
	if err != nil {
		return "", err
	}
	id, err := newToken()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.families[id] = &RefreshFamily{
		ID:          id,
		Username:    username,
		CurrentHash: hashToken(tok),
		ExpiresAt:   now.Add(ttl),
		CreatedAt:   now,
	}
	if err := s.save(); err != nil {
		return "", err
	}
	return tok, nil
}

// Rotate exchanges a refresh token for the next one in its family
// (single-use rotation, §9.3). Presenting the previous (already rotated)
// token again is treated as theft and revokes the whole family.
func (s *Store) Rotate(tok string, ttl time.Duration) (username, newTok string, err error) {
	h := hashToken(tok)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for _, fam := range s.families {
		if fam.Revoked {
			continue
		}
		if fam.PreviousHash == h {
			fam.Revoked = true
			_ = s.save()
			return "", "", ErrTheftDetected
		}
		if fam.CurrentHash != h {
			continue
		}
		if now.After(fam.ExpiresAt) {
			return "", "", ErrInvalidRefresh
		}
		newTok, err = newToken()
		if err != nil {
			return "", "", err
		}
		fam.PreviousHash = fam.CurrentHash
		fam.CurrentHash = hashToken(newTok)
		fam.ExpiresAt = now.Add(ttl)
		if err := s.save(); err != nil {
			return "", "", err
		}
		return fam.Username, newTok, nil
	}
	return "", "", ErrInvalidRefresh
}

// RevokeByToken revokes the family the presented token belongs to
// (logout). Unknown tokens are a silent no-op.
func (s *Store) RevokeByToken(tok string) {
	h := hashToken(tok)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, fam := range s.families {
		if fam.CurrentHash == h || fam.PreviousHash == h {
			fam.Revoked = true
		}
	}
	_ = s.save()
}

// RevokeAllForUser revokes every family of the named account (password
// change, admin revoke-sessions).
func (s *Store) RevokeAllForUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, fam := range s.families {
		if fam.Username == username {
			fam.Revoked = true
		}
	}
	return s.save()
}
