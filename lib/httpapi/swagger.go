package httpapi

// Spec serving and Swagger UI (design doc §10.1, D7): the contract
// api/openapi.yaml is copied into this package by bin/gen-api.sh and
// embedded here; it is served verbatim at /api/openapi.yaml, and a
// self-hosted Swagger UI over it is mounted at /api/docs. Both routes are
// public. TestSpecMatchesContract guards against doc drift between the
// embedded copy and the contract source of truth.

import (
	_ "embed"
	"net/http"

	"github.com/flowchartsman/swaggerui"
)

//go:embed openapi.yaml
var openapiSpec []byte

// serveSpec serves the embedded contract at /api/openapi.yaml.
func (s *Server) serveSpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openapiSpec)
}

// swaggerUI serves the self-hosted Swagger UI over the embedded spec.
// chi's Mount does not rewrite URL.Path for a plain http.Handler
// (chi v5.3), so the /api/docs prefix is stripped here.
func (s *Server) swaggerUI() http.Handler {
	return http.StripPrefix("/api/docs", swaggerui.Handler(openapiSpec))
}
