package httpapi

// Auth and account-administration handlers (design doc §9.3/§9.5) — the
// M6a routes migrated into the OpenAPI contract (M6c). Reply shapes and
// HTTP statuses are byte-identical to the pre-M6c lib/handler/auth.go
// handlers; request validation moved from manual leniency to JsonBody
// over the generated validate: tags.

import (
	"errors"
	"net/http"

	httpapigen "github.com/pschlump/ultima/gen/httpapi"
	"github.com/pschlump/ultima/lib/auth"
)

// authError maps the auth sentinel errors onto HTTP statuses without
// leaking which credential failed.
func authError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, auth.ErrTOTPRequired):
		writeError(w, http.StatusUnauthorized, "totp_required")
	case errors.Is(err, auth.ErrBadCredentials), errors.Is(err, auth.ErrBadTOTP),
		errors.Is(err, auth.ErrAccountDisabled), errors.Is(err, auth.ErrInvalidRefresh),
		errors.Is(err, auth.ErrTheftDetected):
		writeError(w, http.StatusUnauthorized, "unauthorized")
	case errors.Is(err, auth.ErrAccountNotFound), errors.Is(err, auth.ErrNoPendingTOTP):
		writeError(w, http.StatusNotFound, "not_found")
	case errors.Is(err, auth.ErrAccountExists), errors.Is(err, auth.ErrLastAdmin),
		errors.Is(err, auth.ErrBootstrapIllegal), errors.Is(err, auth.ErrTOTPAlreadyOn),
		errors.Is(err, auth.ErrTOTPNotEnabled):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

// deref returns *p, or "" for a nil optional string field.
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// Login implements POST /api/v1/auth/login (public).
func (s *Server) Login(w http.ResponseWriter, r *http.Request) {
	var body httpapigen.LoginRequest
	if !JsonBody(w, r, &body) {
		return
	}
	pair, err := s.svc.Login(body.Username, body.Password, deref(body.Totp))
	if err != nil {
		authError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

// Refresh implements POST /api/v1/auth/refresh (public).
func (s *Server) Refresh(w http.ResponseWriter, r *http.Request) {
	var body httpapigen.RefreshRequest
	if !JsonBody(w, r, &body) {
		return
	}
	pair, err := s.svc.Refresh(body.RefreshToken)
	if err != nil {
		authError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

// Logout implements POST /api/v1/auth/logout.
func (s *Server) Logout(w http.ResponseWriter, r *http.Request) {
	var body httpapigen.RefreshRequest
	if !JsonBody(w, r, &body) {
		return
	}
	s.svc.Logout(body.RefreshToken)
	statusOK(w)
}

// ChangePassword implements POST /api/v1/auth/password.
func (s *Server) ChangePassword(w http.ResponseWriter, r *http.Request) {
	id, _ := auth.IdentityFrom(r.Context())
	var body httpapigen.PasswordChangeRequest
	if !JsonBody(w, r, &body) {
		return
	}
	if err := s.svc.ChangePassword(id.Username, body.CurrentPassword, body.NewPassword, deref(body.Totp)); err != nil {
		authError(w, err)
		return
	}
	statusOK(w)
}

// TotpEnable implements POST /api/v1/auth/totp/enable.
func (s *Server) TotpEnable(w http.ResponseWriter, r *http.Request) {
	s.totpStage(w, r, false)
}

// TotpRegenerate implements POST /api/v1/auth/totp/regenerate.
func (s *Server) TotpRegenerate(w http.ResponseWriter, r *http.Request) {
	s.totpStage(w, r, true)
}

// totpStage stages a TOTP secret (enable, or regenerate when replacing).
func (s *Server) totpStage(w http.ResponseWriter, r *http.Request, replacing bool) {
	id, _ := auth.IdentityFrom(r.Context())
	enroll, err := s.svc.EnableTOTP(id.Username, replacing)
	if err != nil {
		authError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, enroll)
}

// TotpConfirm implements POST /api/v1/auth/totp/confirm.
func (s *Server) TotpConfirm(w http.ResponseWriter, r *http.Request) {
	id, _ := auth.IdentityFrom(r.Context())
	var body httpapigen.TOTPCodeRequest
	if !JsonBody(w, r, &body) {
		return
	}
	if err := s.svc.ConfirmTOTP(id.Username, body.Totp); err != nil {
		authError(w, err)
		return
	}
	statusOK(w)
}

// TotpDisable implements POST /api/v1/auth/totp/disable.
func (s *Server) TotpDisable(w http.ResponseWriter, r *http.Request) {
	id, _ := auth.IdentityFrom(r.Context())
	var body httpapigen.TOTPDisableRequest
	if !JsonBody(w, r, &body) {
		return
	}
	if err := s.svc.DisableTOTP(id.Username, body.Password, deref(body.Totp)); err != nil {
		authError(w, err)
		return
	}
	statusOK(w)
}

// UsersList implements GET /api/v1/admin/users.
func (s *Server) UsersList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"users": s.svc.ListAccounts()})
}

// UsersCreate implements POST /api/v1/admin/users.
func (s *Server) UsersCreate(w http.ResponseWriter, r *http.Request) {
	var body httpapigen.UserCreateRequest
	if !JsonBody(w, r, &body) {
		return
	}
	view, err := s.svc.CreateAccount(body.Username, body.Password, auth.Class(body.Class))
	if err != nil {
		authError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

// UsersUpdate implements PUT /api/v1/admin/users/{name}.
func (s *Server) UsersUpdate(w http.ResponseWriter, r *http.Request, name string) {
	var body httpapigen.UserUpdateRequest
	if !JsonBody(w, r, &body) {
		return
	}
	var class *auth.Class
	if body.Class != nil {
		c := auth.Class(*body.Class)
		class = &c
	}
	view, err := s.svc.UpdateAccount(name, class, body.Disabled)
	if err != nil {
		authError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// UsersDelete implements DELETE /api/v1/admin/users/{name}.
func (s *Server) UsersDelete(w http.ResponseWriter, _ *http.Request, name string) {
	if err := s.svc.DeleteAccount(name); err != nil {
		authError(w, err)
		return
	}
	statusOK(w)
}

// UsersRevokeSessions implements POST /api/v1/admin/users/{name}/revoke-sessions.
func (s *Server) UsersRevokeSessions(w http.ResponseWriter, _ *http.Request, name string) {
	if err := s.svc.RevokeSessions(name); err != nil {
		authError(w, err)
		return
	}
	statusOK(w)
}
