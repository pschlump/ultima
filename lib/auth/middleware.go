package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ctxKey is the context key carrying the verified Identity.
type ctxKey struct{}

// ContextWithIdentity returns a context carrying id (set by the HTTP
// middleware and the gRPC interceptors after token verification).
func ContextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// IdentityFrom extracts the verified identity from ctx.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// bearerToken pulls the token out of an "Authorization: Bearer …" header;
// the scheme match is case-insensitive (gRPC metadata conventions and many
// clients lowercase it).
func bearerToken(header string) string {
	if len(header) < 7 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func writeAuthError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "error": code})
}

// RequireAuth is HTTP middleware demanding a valid Bearer access token
// (§9.3); the verified Identity lands in the request context. It wraps
// only the handler — never the ResponseWriter — so the WS upgrader's
// Hijack contract (lib/handler) is unaffected.
func (s *Service) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := s.VerifyAccess(bearerToken(r.Header.Get("Authorization")))
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r.WithContext(ContextWithIdentity(r.Context(), id)))
	})
}

// RequireAdmin is RequireAuth plus the administrative-class check (§9.5).
func (s *Service) RequireAdmin(next http.Handler) http.Handler {
	return s.RequireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := IdentityFrom(r.Context())
		if id.Class != ClassAdmin {
			writeAuthError(w, http.StatusForbidden, "forbidden")
			return
		}
		next.ServeHTTP(w, r)
	}))
}

// identityFromMetadata validates the `authorization: bearer …` call
// metadata (§6.2) and returns the verified identity.
func (s *Service) identityFromMetadata(ctx context.Context) (Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Identity{}, status.Error(codes.Unauthenticated, "missing credentials")
	}
	var tok string
	for _, v := range md.Get("authorization") {
		if t := bearerToken(v); t != "" {
			tok = t
			break
		}
	}
	if tok == "" {
		return Identity{}, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	id, err := s.VerifyAccess(tok)
	if err != nil {
		return Identity{}, status.Error(codes.Unauthenticated, "invalid token")
	}
	return id, nil
}

// UnaryInterceptor enforces JWT auth on unary RPCs (§9.3).
func (s *Service) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id, err := s.identityFromMetadata(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ContextWithIdentity(ctx, id), req)
	}
}

// StreamInterceptor enforces JWT auth on streaming RPCs (§9.3). The
// identity is verified once at stream establishment; the wrapped stream
// hands the identity-carrying context to the handler.
func (s *Service) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		id, err := s.identityFromMetadata(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &identityStream{ServerStream: ss, ctx: ContextWithIdentity(ss.Context(), id)})
	}
}

type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *identityStream) Context() context.Context { return s.ctx }
