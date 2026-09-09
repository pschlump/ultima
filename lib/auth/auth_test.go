package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pschlump/htotp"

	"github.com/pschlump/ultima/lib/config"
)

// writeTestKeys generates a fresh Ed25519 pair into dir as
// jwt.pem/jwt.pub (PKCS#8 / PKIX PEM, same as bin/gen-jwt-keys.sh).
func writeTestKeys(t *testing.T, dir string) (privPath, pubPath string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating key pair: %s", err)
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshalling private key: %s", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshalling public key: %s", err)
	}
	privPath = filepath.Join(dir, "jwt.pem")
	pubPath = filepath.Join(dir, "jwt.pub")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o600); err != nil {
		t.Fatalf("writing private key: %s", err)
	}
	if err := os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o644); err != nil {
		t.Fatalf("writing public key: %s", err)
	}
	return privPath, pubPath
}

func testConfig(privPath, pubPath, accountsPath string) config.AuthConfig {
	return config.AuthConfig{
		Enabled:           true,
		JwtPrivateKeyFile: privPath,
		JwtPublicKeyFile:  pubPath,
		AccessTokenTTL:    "15m",
		RefreshTokenTTL:   "720h",
		TotpIssuer:        "UltimaTest",
		TotpSkew:          1,
		AccountsFile:      accountsPath,
	}
}

// newTestService builds a Service over a temp-dir key pair and accounts
// file, with a fixed bootstrap admin password.
func newTestService(t *testing.T, cfg *config.AuthConfig) *Service {
	t.Helper()
	dir := t.TempDir()
	privPath, pubPath := writeTestKeys(t, dir)
	c := testConfig(privPath, pubPath, filepath.Join(dir, "accounts.json"))
	if cfg != nil {
		*cfg = c
	} else {
		cfg = &c
	}
	cfg.BootstrapAdminPassword = "bootstrap-pw"
	svc, err := NewService(*cfg, dir, slog.Default())
	if err != nil {
		t.Fatalf("NewService: %s", err)
	}
	return svc
}

func TestLoadKeys(t *testing.T) {
	dir := t.TempDir()
	privPath, pubPath := writeTestKeys(t, dir)

	priv, pub, err := LoadKeys(privPath, pubPath)
	if err != nil {
		t.Fatalf("LoadKeys: %s", err)
	}
	if !priv.Public().(ed25519.PublicKey).Equal(pub) {
		t.Fatal("loaded pair does not match")
	}

	if _, _, err := LoadKeys(filepath.Join(dir, "missing.pem"), pubPath); err == nil {
		t.Fatal("expected error for missing private key")
	}
	if err := os.WriteFile(pubPath, []byte("not pem"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadKeys(privPath, pubPath); err == nil {
		t.Fatal("expected error for malformed public key")
	}

	// Mismatched-but-valid pair must be rejected.
	dir2 := t.TempDir()
	_, pubPath2 := writeTestKeys(t, dir2)
	if _, _, err := LoadKeys(privPath, pubPath2); err == nil {
		t.Fatal("expected error for mismatched key pair")
	}
}

func TestBootstrapAndLogin(t *testing.T) {
	svc := newTestService(t, nil)

	pair, err := svc.Login(BootstrapAdmin, "bootstrap-pw", "")
	if err != nil {
		t.Fatalf("Login: %s", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" || pair.ExpiresIn != 900 {
		t.Fatalf("unexpected pair: %+v", pair)
	}
	id, err := svc.VerifyAccess(pair.AccessToken)
	if err != nil {
		t.Fatalf("VerifyAccess: %s", err)
	}
	if id.Username != BootstrapAdmin || id.Class != ClassAdmin {
		t.Fatalf("unexpected identity: %+v", id)
	}

	if _, err := svc.Login(BootstrapAdmin, "wrong-pw", ""); !errors.Is(err, ErrBadCredentials) {
		t.Fatalf("expected ErrBadCredentials, got %v", err)
	}
	if _, err := svc.Login("no-such-user", "pw", ""); !errors.Is(err, ErrBadCredentials) {
		t.Fatalf("expected ErrBadCredentials for unknown user, got %v", err)
	}
}

func TestAccessTokenVerification(t *testing.T) {
	var cfg config.AuthConfig
	svc := newTestService(t, &cfg)

	pair, err := svc.Login(BootstrapAdmin, "bootstrap-pw", "")
	if err != nil {
		t.Fatalf("Login: %s", err)
	}
	if _, err := svc.VerifyAccess(pair.AccessToken); err != nil {
		t.Fatalf("own token must verify: %s", err)
	}

	// A token signed by a different key must not verify.
	other := newTestService(t, nil)
	otherPair, err := other.Login(BootstrapAdmin, "bootstrap-pw", "")
	if err != nil {
		t.Fatalf("other Login: %s", err)
	}
	if _, err := svc.VerifyAccess(otherPair.AccessToken); !errors.Is(err, ErrInvalidAccess) {
		t.Fatalf("expected ErrInvalidAccess for foreign token, got %v", err)
	}

	// Expired tokens must not verify.
	svc.accessTTL = -time.Minute
	expired, err := svc.issueAccess(&Account{Username: BootstrapAdmin, Class: ClassAdmin})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifyAccess(expired); !errors.Is(err, ErrInvalidAccess) {
		t.Fatalf("expected ErrInvalidAccess for expired token, got %v", err)
	}

	// Deleting the account kills its outstanding tokens immediately.
	if _, err := svc.CreateAccount("temp", "pw", ClassData); err != nil {
		t.Fatal(err)
	}
	tmpPair, err := svc.Login("temp", "pw", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.DeleteAccount("temp"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifyAccess(tmpPair.AccessToken); !errors.Is(err, ErrInvalidAccess) {
		t.Fatalf("expected ErrInvalidAccess for deleted account, got %v", err)
	}
}

func TestRefreshRotationAndTheft(t *testing.T) {
	svc := newTestService(t, nil)

	pair, err := svc.Login(BootstrapAdmin, "bootstrap-pw", "")
	if err != nil {
		t.Fatal(err)
	}

	// Rotate: old token dies, new pair works.
	pair2, err := svc.Refresh(pair.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh: %s", err)
	}
	if pair2.RefreshToken == pair.RefreshToken {
		t.Fatal("rotation returned the same refresh token")
	}
	if _, err := svc.VerifyAccess(pair2.AccessToken); err != nil {
		t.Fatalf("rotated access token does not verify: %s", err)
	}

	// Reusing the rotated token is theft: family revoked, both tokens dead.
	if _, err := svc.Refresh(pair.RefreshToken); !errors.Is(err, ErrTheftDetected) {
		t.Fatalf("expected ErrTheftDetected, got %v", err)
	}
	if _, err := svc.Refresh(pair2.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("expected family revoked after theft, got %v", err)
	}

	// Unknown tokens are simply invalid.
	if _, err := svc.Refresh("not-a-token"); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("expected ErrInvalidRefresh, got %v", err)
	}
}

func TestLogoutRevokesFamily(t *testing.T) {
	svc := newTestService(t, nil)
	pair, err := svc.Login(BootstrapAdmin, "bootstrap-pw", "")
	if err != nil {
		t.Fatal(err)
	}
	svc.Logout(pair.RefreshToken)
	if _, err := svc.Refresh(pair.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("expected ErrInvalidRefresh after logout, got %v", err)
	}
}

func TestTOTPEnrollmentFlow(t *testing.T) {
	svc := newTestService(t, nil)

	enroll, err := svc.EnableTOTP(BootstrapAdmin, false)
	if err != nil {
		t.Fatalf("EnableTOTP: %s", err)
	}
	if enroll.Secret == "" || enroll.ProvisioningURI == "" || enroll.QRCodePNGBase64 == "" {
		t.Fatalf("incomplete enrollment: %+v", enroll)
	}

	// Not yet active: login without a code still works.
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", ""); err != nil {
		t.Fatalf("login before confirm should not need totp: %s", err)
	}

	// Confirm with a code from the pending secret.
	code := htotp.NewDefaultTOTP(enroll.Secret).Now()
	if err := svc.ConfirmTOTP(BootstrapAdmin, code); err != nil {
		t.Fatalf("ConfirmTOTP: %s", err)
	}

	// Second enable without replacing must fail.
	if _, err := svc.EnableTOTP(BootstrapAdmin, false); !errors.Is(err, ErrTOTPAlreadyOn) {
		t.Fatalf("expected ErrTOTPAlreadyOn, got %v", err)
	}

	// Login now requires the code.
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", ""); !errors.Is(err, ErrTOTPRequired) {
		t.Fatalf("expected ErrTOTPRequired, got %v", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", "000000"); !errors.Is(err, ErrBadTOTP) {
		t.Fatalf("expected ErrBadTOTP, got %v", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", htotp.NewDefaultTOTP(enroll.Secret).Now()); err != nil {
		t.Fatalf("login with totp: %s", err)
	}

	// Regenerate: old secret dies, new one must be confirmed.
	re, err := svc.EnableTOTP(BootstrapAdmin, true)
	if err != nil {
		t.Fatalf("regenerate: %s", err)
	}
	if err := svc.ConfirmTOTP(BootstrapAdmin, htotp.NewDefaultTOTP(re.Secret).Now()); err != nil {
		t.Fatalf("confirm regenerate: %s", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", htotp.NewDefaultTOTP(enroll.Secret).Now()); !errors.Is(err, ErrBadTOTP) {
		t.Fatalf("old secret should be dead after regenerate, got %v", err)
	}

	// Disable requires password + current code.
	if err := svc.DisableTOTP(BootstrapAdmin, "bootstrap-pw", "000000"); !errors.Is(err, ErrBadTOTP) {
		t.Fatalf("expected ErrBadTOTP, got %v", err)
	}
	if err := svc.DisableTOTP(BootstrapAdmin, "bootstrap-pw", htotp.NewDefaultTOTP(re.Secret).Now()); err != nil {
		t.Fatalf("DisableTOTP: %s", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", ""); err != nil {
		t.Fatalf("login after disable: %s", err)
	}
}

func TestAccountCRUDAndGuards(t *testing.T) {
	svc := newTestService(t, nil)

	if _, err := svc.CreateAccount("alice", "pw1", ClassAdmin); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateAccount("bob", "pw2", ClassData); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateAccount("alice", "pw1", ClassData); !errors.Is(err, ErrAccountExists) {
		t.Fatalf("expected ErrAccountExists, got %v", err)
	}

	views := svc.ListAccounts()
	if len(views) != 3 {
		t.Fatalf("expected 3 accounts, got %d", len(views))
	}

	// Bootstrap admin cannot be deleted; last enabled admin is protected.
	if err := svc.DeleteAccount(BootstrapAdmin); !errors.Is(err, ErrBootstrapIllegal) {
		t.Fatalf("expected ErrBootstrapIllegal, got %v", err)
	}
	if err := svc.DeleteAccount("alice"); err != nil {
		t.Fatal(err)
	}
	disabled := true
	if _, err := svc.UpdateAccount(BootstrapAdmin, nil, &disabled); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("expected ErrLastAdmin disabling last admin, got %v", err)
	}
	// With alice gone, demoting bootstrap admin must also fail.
	dataClass := ClassData
	if _, err := svc.UpdateAccount(BootstrapAdmin, &dataClass, nil); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("expected ErrLastAdmin demoting last admin, got %v", err)
	}

	// But with another admin present, disabling bootstrap admin works.
	if _, err := svc.CreateAccount("carol", "pw3", ClassAdmin); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateAccount(BootstrapAdmin, nil, &disabled); err != nil {
		t.Fatalf("disable bootstrap admin with second admin present: %s", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", ""); !errors.Is(err, ErrAccountDisabled) {
		t.Fatalf("expected ErrAccountDisabled, got %v", err)
	}

	// bob (data) is unaffected and deletable.
	if _, err := svc.Login("bob", "pw2", ""); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeleteAccount("bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Login("bob", "pw2", ""); !errors.Is(err, ErrBadCredentials) {
		t.Fatalf("expected ErrBadCredentials after delete, got %v", err)
	}
}

func TestPasswordChangeRevokesSessions(t *testing.T) {
	svc := newTestService(t, nil)
	pair, err := svc.Login(BootstrapAdmin, "bootstrap-pw", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ChangePassword(BootstrapAdmin, "bootstrap-pw", "new-pw", ""); err != nil {
		t.Fatalf("ChangePassword: %s", err)
	}
	if _, err := svc.Refresh(pair.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("expected sessions revoked after password change, got %v", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "bootstrap-pw", ""); !errors.Is(err, ErrBadCredentials) {
		t.Fatalf("old password should be dead, got %v", err)
	}
	if _, err := svc.Login(BootstrapAdmin, "new-pw", ""); err != nil {
		t.Fatalf("new password should work: %s", err)
	}
	if err := svc.ChangePassword(BootstrapAdmin, "wrong", "x", ""); !errors.Is(err, ErrBadCredentials) {
		t.Fatalf("expected ErrBadCredentials, got %v", err)
	}
}

func TestStorePersistenceAcrossReload(t *testing.T) {
	var cfg config.AuthConfig
	svc := newTestService(t, &cfg)

	if _, err := svc.CreateAccount("dave", "pw4", ClassData); err != nil {
		t.Fatal(err)
	}
	pair, err := svc.Login("dave", "pw4", "")
	if err != nil {
		t.Fatal(err)
	}

	// A second service over the same directory sees the accounts and the
	// refresh family.
	svc2, err := NewService(cfg, t.TempDir(), slog.Default())
	if err != nil {
		t.Fatalf("reload NewService: %s", err)
	}
	if _, err := svc2.Login("dave", "pw4", ""); err != nil {
		t.Fatalf("login after reload: %s", err)
	}
	if _, err := svc2.Refresh(pair.RefreshToken); err != nil {
		t.Fatalf("refresh after reload: %s", err)
	}
}

func TestNewServiceFailures(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(filepath.Join(dir, "none.pem"), filepath.Join(dir, "none.pub"), filepath.Join(dir, "a.json"))
	if _, err := NewService(cfg, dir, slog.Default()); err == nil {
		t.Fatal("expected error for missing key files")
	}
	privPath, pubPath := writeTestKeys(t, dir)
	cfg.JwtPrivateKeyFile = privPath
	cfg.JwtPublicKeyFile = pubPath
	cfg.AccessTokenTTL = "not-a-duration"
	if _, err := NewService(cfg, dir, slog.Default()); err == nil {
		t.Fatal("expected error for bad access_token_ttl")
	}
	cfg.AccessTokenTTL = "15m"
	// Malformed accounts file must fail, never silently reset.
	if err := os.WriteFile(cfg.AccountsFile, []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(cfg, dir, slog.Default()); err == nil {
		t.Fatal("expected error for corrupt accounts file")
	}
}
