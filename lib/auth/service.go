package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/pschlump/htotp"

	"github.com/pschlump/ultima/lib/config"
)

// BootstrapAdmin is the built-in bootstrap identity (§9.1). It exists so
// a fresh server has one administrative account to log in with; §9.5
// recommends disabling it once another admin exists, and the startup
// check warns while it remains the only enabled admin.
const BootstrapAdmin = "admin"

// Identity is the verified caller behind a request or connection: the
// JWT subject plus account class. It travels in request/connection
// context (middleware.go) and onto commands.ConnState.User.
type Identity struct {
	Username string
	Class    Class
}

// Service ties the config, the Ed25519 key pair, and the account store
// together: login, token issue/verify/refresh, account CRUD, and TOTP
// enrollment. Construct one per process; all methods are safe for
// concurrent use.
type Service struct {
	priv       ed25519.PrivateKey
	pub        ed25519.PublicKey
	store      *Store
	accessTTL  time.Duration
	refreshTTL time.Duration
	issuer     string
	totpSkew   uint
	logger     *slog.Logger
}

// NewService builds the auth service from config (design doc §8, D20).
// persistDir supplies the default accounts-file location. Startup fails
// on any error — a missing or unparseable key pair is never worked
// around.
func NewService(cfg config.AuthConfig, persistDir string, logger *slog.Logger) (*Service, error) {
	priv, pub, err := LoadKeys(cfg.JwtPrivateKeyFile, cfg.JwtPublicKeyFile)
	if err != nil {
		return nil, err
	}
	accessTTL, err := time.ParseDuration(cfg.AccessTokenTTL)
	if err != nil || accessTTL <= 0 {
		return nil, fmt.Errorf("auth: bad access_token_ttl %q", cfg.AccessTokenTTL)
	}
	refreshTTL, err := time.ParseDuration(cfg.RefreshTokenTTL)
	if err != nil || refreshTTL <= 0 {
		return nil, fmt.Errorf("auth: bad refresh_token_ttl %q", cfg.RefreshTokenTTL)
	}
	accountsPath := cfg.AccountsFile
	if accountsPath == "" {
		accountsPath = filepath.Join(persistDir, "accounts.json")
	}
	store, err := LoadStore(accountsPath)
	if err != nil {
		return nil, err
	}

	s := &Service{
		priv:       priv,
		pub:        pub,
		store:      store,
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
		issuer:     cfg.TotpIssuer,
		totpSkew:   uint(max(cfg.TotpSkew, 0)),
		logger:     logger,
	}

	if !store.Exists() {
		if err := s.bootstrap(cfg.BootstrapAdminPassword); err != nil {
			return nil, err
		}
	}
	if _, ok := store.Get(BootstrapAdmin); ok && store.CountEnabledAdmins() == 1 {
		logger.Warn("startup posture: the built-in admin account is the only enabled administrative account",
			"recommendation", "create another admin via /api/v1/admin/users, then disable admin (design doc §9.5)")
	}
	return s, nil
}

// bootstrap creates the initial admin account on first boot. The
// password comes from config ($ENV$ capable); when unset a random one is
// generated and logged exactly once — it is never written to disk.
func (s *Service) bootstrap(configured string) error {
	password := configured
	generated := false
	if password == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return fmt.Errorf("auth: generating bootstrap password: %w", err)
		}
		password = hex.EncodeToString(b[:])
		generated = true
	}
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	if err := s.store.Create(BootstrapAdmin, ClassAdmin, hash); err != nil {
		return fmt.Errorf("auth: creating bootstrap admin: %w", err)
	}
	if generated {
		s.logger.Warn("bootstrap admin account created with a generated password",
			"username", BootstrapAdmin, "password", password,
			"note", "shown once; change it via /api/v1/auth/password or set auth.bootstrap_admin_password")
	} else {
		s.logger.Info("bootstrap admin account created", "username", BootstrapAdmin)
	}
	return nil
}

// TokenPair is the login/refresh response (§9.3).
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"` // access-token lifetime, seconds
}

// Login authenticates username + password (+ TOTP code when the account
// has 2FA enabled) and issues a token pair (§9.3).
func (s *Service) Login(username, password, totpCode string) (TokenPair, error) {
	a, err := s.store.CheckPassword(username, password)
	if err != nil {
		return TokenPair{}, err
	}
	if a.TOTPEnabled {
		if totpCode == "" {
			return TokenPair{}, ErrTOTPRequired
		}
		if !s.verifyTOTP(a.TOTPSecret, totpCode) {
			return TokenPair{}, ErrBadTOTP
		}
	}
	return s.issuePair(&a)
}

func (s *Service) issuePair(a *Account) (TokenPair, error) {
	access, err := s.issueAccess(a)
	if err != nil {
		return TokenPair{}, err
	}
	refresh, err := s.store.NewFamily(a.Username, s.refreshTTL)
	if err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: access, RefreshToken: refresh, ExpiresIn: int64(s.accessTTL.Seconds())}, nil
}

// Refresh rotates a refresh token (§9.3): the old token dies, a new pair
// is issued. Reuse of a rotated token revokes the whole family
// (ErrTheftDetected).
func (s *Service) Refresh(refreshTok string) (TokenPair, error) {
	username, newRefresh, err := s.store.Rotate(refreshTok, s.refreshTTL)
	if err != nil {
		return TokenPair{}, err
	}
	a, ok := s.store.Get(username)
	if !ok || a.Disabled {
		s.store.RevokeByToken(newRefresh)
		return TokenPair{}, ErrInvalidRefresh
	}
	access, err := s.issueAccess(&a)
	if err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: access, RefreshToken: newRefresh, ExpiresIn: int64(s.accessTTL.Seconds())}, nil
}

// Logout revokes the family a refresh token belongs to.
func (s *Service) Logout(refreshTok string) {
	s.store.RevokeByToken(refreshTok)
}

// ChangePassword replaces the caller's password (requires the current
// one, plus a TOTP code when 2FA is enabled) and revokes every existing
// session of the account (§9.5).
func (s *Service) ChangePassword(username, currentPassword, newPassword, totpCode string) error {
	a, err := s.store.CheckPassword(username, currentPassword)
	if err != nil {
		return err
	}
	if a.TOTPEnabled {
		if totpCode == "" {
			return ErrTOTPRequired
		}
		if !s.verifyTOTP(a.TOTPSecret, totpCode) {
			return ErrBadTOTP
		}
	}
	if newPassword == "" {
		return fmt.Errorf("auth: new password must not be empty")
	}
	hash, err := HashPassword(newPassword)
	if err != nil {
		return err
	}
	if err := s.store.SetPassword(username, hash); err != nil {
		return err
	}
	return s.store.RevokeAllForUser(username)
}

// TOTPEnrollment is the enable/regenerate response: the secret and its
// otpauth:// provisioning URI are shown once (§9.2), plus a QR code PNG
// for direct authenticator-app enrollment from the web UI.
type TOTPEnrollment struct {
	Secret          string `json:"secret"`
	ProvisioningURI string `json:"provisioning_uri"`
	QRCodePNGBase64 string `json:"qr_code_png_base64"`
}

// EnableTOTP stages a new TOTP secret; ConfirmTOTP activates it.
// replacing=true implements regenerate (§9.2): the old secret stops
// working as soon as the new one is confirmed, and the pending secret
// replaces any earlier pending enrollment.
func (s *Service) EnableTOTP(username string, replacing bool) (TOTPEnrollment, error) {
	secret := htotp.RandomSecret(20)
	if secret == "" {
		return TOTPEnrollment{}, fmt.Errorf("auth: generating totp secret failed")
	}
	if err := s.store.StageTOTP(username, secret, replacing); err != nil {
		return TOTPEnrollment{}, err
	}
	t := htotp.NewDefaultTOTP(secret)
	uri := t.ProvisioningUri(username, s.issuer)
	png, err := htotp.GenerateQRCodePNG(uri)
	if err != nil {
		return TOTPEnrollment{}, fmt.Errorf("auth: generating QR code: %w", err)
	}
	return TOTPEnrollment{
		Secret:          secret,
		ProvisioningURI: uri,
		QRCodePNGBase64: base64.StdEncoding.EncodeToString(png),
	}, nil
}

// ConfirmTOTP activates the pending enrollment when the code checks out
// against the pending secret.
func (s *Service) ConfirmTOTP(username, totpCode string) error {
	a, ok := s.store.Get(username)
	if !ok {
		return ErrAccountNotFound
	}
	if a.PendingTOTPSecret == "" {
		return ErrNoPendingTOTP
	}
	if !s.verifyTOTP(a.PendingTOTPSecret, totpCode) {
		return ErrBadTOTP
	}
	return s.store.ConfirmTOTP(username)
}

// DisableTOTP turns 2FA off; requires the password and a current TOTP
// code (§9.5).
func (s *Service) DisableTOTP(username, password, totpCode string) error {
	a, err := s.store.CheckPassword(username, password)
	if err != nil {
		return err
	}
	if !a.TOTPEnabled {
		return ErrTOTPNotEnabled
	}
	if !s.verifyTOTP(a.TOTPSecret, totpCode) {
		return ErrBadTOTP
	}
	return s.store.DisableTOTP(username)
}

// verifyTOTP checks a code against a base32 secret with the configured
// skew (htotp, D17).
func (s *Service) verifyTOTP(secret, code string) bool {
	t := htotp.NewDefaultTOTP(secret)
	t.SetSkew(s.totpSkew, 0)
	return t.Verify(code)
}

// --- administrative account management (§9.5) ---

// AccountView is the store record minus its secrets, for API replies.
type AccountView struct {
	Username    string    `json:"username"`
	Class       Class     `json:"class"`
	TOTPEnabled bool      `json:"totp_enabled"`
	Disabled    bool      `json:"disabled"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func viewOf(a Account) AccountView {
	return AccountView{
		Username:    a.Username,
		Class:       a.Class,
		TOTPEnabled: a.TOTPEnabled,
		Disabled:    a.Disabled,
		CreatedAt:   a.CreatedAt,
		UpdatedAt:   a.UpdatedAt,
	}
}

// ListAccounts returns all accounts, secrets stripped.
func (s *Service) ListAccounts() []AccountView {
	accts := s.store.List()
	out := make([]AccountView, 0, len(accts))
	for _, a := range accts {
		out = append(out, viewOf(a))
	}
	return out
}

// CreateAccount adds an admin or data account (§9.5).
func (s *Service) CreateAccount(username, password string, class Class) (AccountView, error) {
	if username == "" || password == "" {
		return AccountView{}, fmt.Errorf("auth: username and password are required")
	}
	if class != ClassAdmin && class != ClassData {
		return AccountView{}, fmt.Errorf("auth: class must be %q or %q", ClassAdmin, ClassData)
	}
	hash, err := HashPassword(password)
	if err != nil {
		return AccountView{}, err
	}
	if err := s.store.Create(username, class, hash); err != nil {
		return AccountView{}, err
	}
	a, _ := s.store.Get(username)
	return viewOf(a), nil
}

// UpdateAccount applies class/disabled changes (§9.5 PUT).
func (s *Service) UpdateAccount(username string, class *Class, disabled *bool) (AccountView, error) {
	if class != nil {
		if *class != ClassAdmin && *class != ClassData {
			return AccountView{}, fmt.Errorf("auth: class must be %q or %q", ClassAdmin, ClassData)
		}
		if err := s.store.SetClass(username, *class); err != nil {
			return AccountView{}, err
		}
	}
	if disabled != nil {
		if err := s.store.SetDisabled(username, *disabled); err != nil {
			return AccountView{}, err
		}
	}
	a, ok := s.store.Get(username)
	if !ok {
		return AccountView{}, ErrAccountNotFound
	}
	return viewOf(a), nil
}

// DeleteAccount removes an account and its sessions (last-admin and
// bootstrap-admin protections live in the store).
func (s *Service) DeleteAccount(username string) error {
	return s.store.Delete(username)
}

// RevokeSessions revokes every refresh family of the account (§9.5).
func (s *Service) RevokeSessions(username string) error {
	if _, ok := s.store.Get(username); !ok {
		return ErrAccountNotFound
	}
	return s.store.RevokeAllForUser(username)
}
