package main

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/pschlump/ultima/lib/handler"
)

// newRouter builds the chi mux for the HTTP/WS port: request logging and
// panic recovery wrap the M0 stub routes registered by lib/handler.
func newRouter(logger *slog.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(handler.RequestLogger(logger))
	handler.Register(r, logger)
	return r
}
