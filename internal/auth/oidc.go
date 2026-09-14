// Package auth provides OPTIONAL OIDC bearer validation for the gateway's
// /api/* routes. When OIDC_ISSUER is unset the middleware is a pass-through
// (prod back-compat — the gateway historically ran unauthenticated behind
// acquire); when set, every /api call must carry a valid bearer from that
// issuer (issuer-only, no audience check).
//
// ALLOWED_CLIENTS narrows that to named callers: with an issuer AND a list of
// OIDC client ids, a valid token is only accepted when its azp (or client_id)
// claim is one of them (403 otherwise). Only then is the client configuration
// API exposed at all — a gateway anyone with a realm token can reach must not
// let them repoint its download clients.
//
// Init is lazy + self-healing (NOT sync.Once): if the IdP is unreachable at the
// first request it returns 503 and retries on the next call, so the gateway
// recovers on its own once Keycloak is up — the katalog-manager pattern.
package auth

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
)

type Verifier struct {
	issuer  string
	allowed map[string]bool // OIDC client ids; empty = any valid token

	mu       sync.Mutex
	verifier *oidc.IDTokenVerifier
	ready    bool
}

// NewVerifier returns a verifier. A blank issuer disables auth (pass-through).
// allowedClients restricts accepted tokens to those OIDC client ids; it only
// takes effect together with an issuer.
func NewVerifier(issuer string, allowedClients ...string) *Verifier {
	issuer = strings.TrimSpace(issuer)
	allowed := map[string]bool{}
	for _, id := range allowedClients {
		if id = strings.TrimSpace(id); id != "" {
			allowed[id] = true
		}
	}
	switch {
	case issuer == "":
		slog.Warn("OIDC_ISSUER unset — gateway /api routes are UNAUTHENTICATED (set OIDC_ISSUER to require a bearer)")
		if len(allowed) > 0 {
			slog.Warn("ALLOWED_CLIENTS is ignored without OIDC_ISSUER; the client configuration API stays disabled")
		}
	case len(allowed) > 0:
		slog.Info("gateway OIDC enabled for allowed clients only", "issuer", issuer, "allowed_clients", keys(allowed))
	default:
		slog.Info("gateway OIDC enabled", "issuer", issuer)
	}
	v := &Verifier{issuer: issuer}
	if issuer != "" {
		v.allowed = allowed
	}
	return v
}

// ParseClientList splits a comma-separated ALLOWED_CLIENTS value.
func ParseClientList(s string) []string {
	var out []string
	for _, id := range strings.Split(s, ",") {
		if id = strings.TrimSpace(id); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// Enabled reports whether bearer validation is active.
func (v *Verifier) Enabled() bool { return v.issuer != "" }

// Restricted reports whether tokens are limited to ALLOWED_CLIENTS, which is
// also what gates the client configuration API.
func (v *Verifier) Restricted() bool { return v.issuer != "" && len(v.allowed) > 0 }

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
		tok, err := v.verifier.Verify(r.Context(), raw)
		if err != nil {
			http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
			return
		}
		if v.Restricted() && !v.allows(tok) {
			http.Error(w, "this client is not allowed to call the gateway", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allows reports whether the token was issued to an allowed client. Access
// tokens name their client in azp; some issuers use client_id instead.
func (v *Verifier) allows(tok *oidc.IDToken) bool {
	var claims struct {
		Azp      string `json:"azp"`
		ClientID string `json:"client_id"`
	}
	if err := tok.Claims(&claims); err != nil {
		return false
	}
	return (claims.Azp != "" && v.allowed[claims.Azp]) || (claims.ClientID != "" && v.allowed[claims.ClientID])
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
