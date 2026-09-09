package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/auth"
)

// RegisterAuth wires the M6a login/account endpoints (design doc §9.3,
// §9.5) onto r. Public: login and refresh. Bearer-authenticated: logout,
// password change, TOTP enrollment. Administrative: /api/v1/admin/users*.
// Called only when auth.enabled is set — with auth off these routes do
// not exist and the API stays open.
func RegisterAuth(r chi.Router, svc *auth.Service) {
	r.Post("/api/v1/auth/login", loginHandler(svc))
	r.Post("/api/v1/auth/refresh", refreshHandler(svc))

	r.Group(func(r chi.Router) {
		r.Use(svc.RequireAuth)
		r.Post("/api/v1/auth/logout", logoutHandler(svc))
		r.Post("/api/v1/auth/password", passwordHandler(svc))
		r.Post("/api/v1/auth/totp/enable", totpEnableHandler(svc, false))
		r.Post("/api/v1/auth/totp/regenerate", totpEnableHandler(svc, true))
		r.Post("/api/v1/auth/totp/confirm", totpConfirmHandler(svc))
		r.Post("/api/v1/auth/totp/disable", totpDisableHandler(svc))
	})

	r.Group(func(r chi.Router) {
		r.Use(svc.RequireAdmin)
		r.Get("/api/v1/admin/users", usersListHandler(svc))
		r.Post("/api/v1/admin/users", usersCreateHandler(svc))
		r.Put("/api/v1/admin/users/{name}", usersUpdateHandler(svc))
		r.Delete("/api/v1/admin/users/{name}", usersDeleteHandler(svc))
		r.Post("/api/v1/admin/users/{name}/revoke-sessions", usersRevokeSessionsHandler(svc))
	})
}

// decodeBody unmarshals the JSON request body; empty bodies decode to
// the zero value so optional-field endpoints stay lenient.
func decodeBody(r *http.Request, v any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(v)
}

// authError maps the auth sentinel errors onto HTTP statuses without
// leaking which credential failed.
func authError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, auth.ErrTOTPRequired):
		writeJSON(w, http.StatusUnauthorized, map[string]string{"status": "error", "error": "totp_required"})
	case errors.Is(err, auth.ErrBadCredentials), errors.Is(err, auth.ErrBadTOTP),
		errors.Is(err, auth.ErrAccountDisabled), errors.Is(err, auth.ErrInvalidRefresh),
		errors.Is(err, auth.ErrTheftDetected):
		writeJSON(w, http.StatusUnauthorized, map[string]string{"status": "error", "error": "unauthorized"})
	case errors.Is(err, auth.ErrAccountNotFound), errors.Is(err, auth.ErrNoPendingTOTP):
		writeJSON(w, http.StatusNotFound, map[string]string{"status": "error", "error": "not_found"})
	case errors.Is(err, auth.ErrAccountExists), errors.Is(err, auth.ErrLastAdmin),
		errors.Is(err, auth.ErrBootstrapIllegal), errors.Is(err, auth.ErrTOTPAlreadyOn),
		errors.Is(err, auth.ErrTOTPNotEnabled):
		writeJSON(w, http.StatusConflict, map[string]string{"status": "error", "error": err.Error()})
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": err.Error()})
	}
}

func loginHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		TOTP     string `json:"totp"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		pair, err := svc.Login(body.Username, body.Password, body.TOTP)
		if err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, pair)
	}
}

func refreshHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		RefreshToken string `json:"refresh_token"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		pair, err := svc.Refresh(body.RefreshToken)
		if err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, pair)
	}
}

func logoutHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		RefreshToken string `json:"refresh_token"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		svc.Logout(body.RefreshToken)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func passwordHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
		TOTP            string `json:"totp"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		id, _ := auth.IdentityFrom(r.Context())
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		if err := svc.ChangePassword(id.Username, body.CurrentPassword, body.NewPassword, body.TOTP); err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func totpEnableHandler(svc *auth.Service, replacing bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, _ := auth.IdentityFrom(r.Context())
		enroll, err := svc.EnableTOTP(id.Username, replacing)
		if err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, enroll)
	}
}

func totpConfirmHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		TOTP string `json:"totp"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		id, _ := auth.IdentityFrom(r.Context())
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		if err := svc.ConfirmTOTP(id.Username, body.TOTP); err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func totpDisableHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		Password string `json:"password"`
		TOTP     string `json:"totp"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		id, _ := auth.IdentityFrom(r.Context())
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		if err := svc.DisableTOTP(id.Username, body.Password, body.TOTP); err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func usersListHandler(svc *auth.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"users": svc.ListAccounts()})
	}
}

func usersCreateHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		Username string     `json:"username"`
		Password string     `json:"password"`
		Class    auth.Class `json:"class"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		view, err := svc.CreateAccount(body.Username, body.Password, body.Class)
		if err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, view)
	}
}

func usersUpdateHandler(svc *auth.Service) http.HandlerFunc {
	type req struct {
		Class    *auth.Class `json:"class"`
		Disabled *bool       `json:"disabled"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var body req
		if err := decodeBody(r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "error": "bad request body"})
			return
		}
		view, err := svc.UpdateAccount(chi.URLParam(r, "name"), body.Class, body.Disabled)
		if err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	}
}

func usersDeleteHandler(svc *auth.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := svc.DeleteAccount(chi.URLParam(r, "name")); err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func usersRevokeSessionsHandler(svc *auth.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := svc.RevokeSessions(chi.URLParam(r, "name")); err != nil {
			authError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
