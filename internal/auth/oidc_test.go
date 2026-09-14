package auth_test

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/laedeli/download-gateway/internal/auth"
	"github.com/laedeli/download-gateway/internal/auth/authtest"
)

func call(t *testing.T, v *auth.Verifier, bearer string) int {
	t.Helper()
	h := v.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/clients", nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// Without ALLOWED_CLIENTS any valid token from the issuer is accepted, as before.
func TestMiddlewareWithoutAllowListAcceptsAnyValidToken(t *testing.T) {
	iss := authtest.NewIssuer(t)
	v := auth.NewVerifier(iss.URL)
	if v.Restricted() {
		t.Fatal("restricted without ALLOWED_CLIENTS")
	}
	if got := call(t, v, iss.Token(t, map[string]any{"azp": "anyone"})); got != http.StatusNoContent {
		t.Fatalf("valid token = %d, want pass-through", got)
	}
	if got := call(t, v, ""); got != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", got)
	}
}

func TestMiddlewareAllowList(t *testing.T) {
	iss := authtest.NewIssuer(t)
	v := auth.NewVerifier(iss.URL, auth.ParseClientList(" acquire , ,console")...)
	if !v.Restricted() {
		t.Fatal("not restricted with an issuer and ALLOWED_CLIENTS")
	}
	for _, tc := range []struct {
		name   string
		claims map[string]any
		want   int
	}{
		{"azp allowed", map[string]any{"azp": "acquire"}, http.StatusNoContent},
		{"client_id allowed", map[string]any{"client_id": "console"}, http.StatusNoContent},
		{"azp not allowed", map[string]any{"azp": "someone-else"}, http.StatusForbidden},
		{"no client claim", map[string]any{}, http.StatusForbidden},
		{"empty azp", map[string]any{"azp": ""}, http.StatusForbidden},
	} {
		if got := call(t, v, iss.Token(t, tc.claims)); got != tc.want {
			t.Errorf("%s: status = %d, want %d", tc.name, got, tc.want)
		}
	}
	if got := call(t, v, "not-a-jwt"); got != http.StatusUnauthorized {
		t.Errorf("garbage token = %d, want 401", got)
	}

	other := authtest.NewIssuer(t)
	if got := call(t, v, other.Token(t, map[string]any{"iss": iss.URL, "azp": "acquire"})); got != http.StatusUnauthorized {
		t.Errorf("token signed by another key = %d, want 401", got)
	}
}

// ALLOWED_CLIENTS alone must not switch anything on: no issuer means no auth
// and, above all, no configuration API.
func TestAllowListNeedsIssuer(t *testing.T) {
	v := auth.NewVerifier("", "acquire")
	if v.Enabled() || v.Restricted() {
		t.Fatalf("enabled=%v restricted=%v without an issuer", v.Enabled(), v.Restricted())
	}
	if got := call(t, v, ""); got != http.StatusNoContent {
		t.Fatalf("pass-through = %d", got)
	}
}

func TestParseClientList(t *testing.T) {
	if got := auth.ParseClientList(""); len(got) != 0 {
		t.Errorf("empty = %v", got)
	}
	if got := auth.ParseClientList("a, b,,c "); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("list = %v", got)
	}
}
