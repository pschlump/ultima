package main

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/handler"
	"github.com/pschlump/ultima/lib/wssrv"
)

// newRouter builds the chi mux for the HTTP/WS port: request logging and
// panic recovery wrap the HTTP routes from lib/handler and the /ws/v1
// WebSocket command endpoint from lib/wssrv (design doc §6.3).
func newRouter(logger *slog.Logger, eng *commands.Engine) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(handler.RequestLogger(logger))
	handler.Register(r)
	r.Get("/ws/v1", wssrv.Handler(eng, logger))
	return r
}
