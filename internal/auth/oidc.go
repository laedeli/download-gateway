// Package auth provides OPTIONAL OIDC bearer validation for the gateway's
// /api/* routes. When OIDC_ISSUER is unset the middleware is a pass-through
// (prod back-compat — the gateway historically ran unauthenticated behind
// acquire); when set, every /api call must carry a valid bearer from that
// issuer (issuer-only, no audience check).
//
// Init is lazy + self-healing (NOT sync.Once): if the IdP is unreachable at the
// first request it returns 503 and retries on the next call, so the gateway
// recovers on its own once Keycloak is up — the katalog-manager pattern.
package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
)

type Verifier struct {
	issuer string

	mu       sync.Mutex
	verifier *oidc.IDTokenVerifier
	ready    bool
}

// NewVerifier returns a verifier. A blank issuer disables auth (pass-through).
func NewVerifier(issuer string) *Verifier {
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		slog.Warn("OIDC_ISSUER unset — gateway /api routes are UNAUTHENTICATED (set OIDC_ISSUER to require a bearer)")
	} else {
		slog.Info("gateway OIDC enabled", "issuer", issuer)
	}
	return &Verifier{issuer: issuer}
}

// Enabled reports whether bearer validation is active.
func (v *Verifier) Enabled() bool { return v.issuer != "" }

// ensure lazily builds the verifier, retrying on every call until the IdP
// responds (self-healing).
func (v *Verifier) ensure(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.ready {
		return nil
	}
	p, err := oidc.NewProvider(ctx, v.issuer)
	if err != nil {
		return err
	}
	v.verifier = p.Verifier(&oidc.Config{SkipClientIDCheck: true})
	v.ready = true
	return nil
}

// Middleware wraps next with bearer validation (no-op when disabled).
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !v.Enabled() {
			next.ServeHTTP(w, r)
			return
		}
		authz := r.Header.Get("Authorization")
		if !strings.HasPrefix(authz, "Bearer ") {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		raw := strings.TrimPrefix(authz, "Bearer ")
		if err := v.ensure(r.Context()); err != nil {
			slog.Warn("oidc provider unavailable", "err", err)
			http.Error(w, "oidc provider unavailable", http.StatusServiceUnavailable)
			return
		}
		if _, err := v.verifier.Verify(r.Context(), raw); err != nil {
			http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
