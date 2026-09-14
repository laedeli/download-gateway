package main

import (
	"net/http"
	"strings"
	"testing"

	"github.com/laedeli/download-gateway/internal/adapters"
	"github.com/laedeli/download-gateway/internal/auth"
	"github.com/laedeli/download-gateway/internal/auth/authtest"
)

const pushedSecret = "pushed-secret-never-returned"

var configRoutes = []struct{ method, path string }{
	{http.MethodGet, "/api/v1/config/types"},
	{http.MethodGet, "/api/v1/config/clients"},
	{http.MethodPut, "/api/v1/config/clients"},
	{http.MethodPost, "/api/v1/config/clients/test"},
}

// The config API only exists with both an issuer and ALLOWED_CLIENTS.
func TestConfigRoutesHiddenUnlessRestricted(t *testing.T) {
	iss := authtest.NewIssuer(t)
	token := iss.Token(t, map[string]any{"azp": "acquire"})
	for name, v := range map[string]*auth.Verifier{
		"no auth":                  auth.NewVerifier(""),
		"allowed clients only":     auth.NewVerifier("", "acquire"),
		"issuer without allowlist": auth.NewVerifier(iss.URL),
	} {
		tg := newTestGateway(t, nil, v)
		for _, rt := range configRoutes {
			if rec := tg.call(t, rt.method, rt.path, map[string]any{}, token); rec.Code != http.StatusNotFound {
				t.Errorf("%s: %s %s = %d, want 404", name, rt.method, rt.path, rec.Code)
			}
		}
		// The rest of the API is unaffected.
		if rec := tg.call(t, http.MethodGet, "/api/v1/clients", nil, token); rec.Code != http.StatusOK {
			t.Errorf("%s: GET /api/v1/clients = %d", name, rec.Code)
		}
	}
}

// With ALLOWED_CLIENTS every /api/v1 route, config or not, needs a token issued
// to an allowed client.
func TestAPIRequiresAllowedClient(t *testing.T) {
	iss := authtest.NewIssuer(t)
	tg := newTestGateway(t, nil, auth.NewVerifier(iss.URL, "acquire"))
	allowed := iss.Token(t, map[string]any{"azp": "acquire"})
	stranger := iss.Token(t, map[string]any{"azp": "some-other-app"})

	routes := append([]struct{ method, path string }{
		{http.MethodGet, "/api/v1/clients"},
		{http.MethodGet, "/api/v1/clients/status"},
		{http.MethodGet, "/api/v1/downloads"},
		{http.MethodPost, "/api/v1/downloads"},
	}, configRoutes...)
	for _, rt := range routes {
		if rec := tg.call(t, rt.method, rt.path, map[string]any{}, ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without a token = %d, want 401", rt.method, rt.path, rec.Code)
		}
		if rec := tg.call(t, rt.method, rt.path, map[string]any{}, stranger); rec.Code != http.StatusForbidden {
			t.Errorf("%s %s from another client = %d, want 403", rt.method, rt.path, rec.Code)
		}
	}
	for _, path := range []string{"/api/v1/clients", "/api/v1/config/types", "/api/v1/config/clients"} {
		if rec := tg.call(t, http.MethodGet, path, nil, allowed); rec.Code != http.StatusOK {
			t.Errorf("GET %s from the allowed client = %d %s", path, rec.Code, rec.Body)
		}
	}
}

func TestConfigPushAndList(t *testing.T) {
	iss := authtest.NewIssuer(t)
	token := iss.Token(t, map[string]any{"client_id": "acquire"})
	reg := adapters.NewRegistry()
	reg.AddEnv(adapters.ClientSpec{BaseURL: "http://odl.example", Auth: adapters.AuthToken, Secret: "env-token"},
		adapters.NewODownloader("http://odl.example", "env-token"))
	tg := newTestGateway(t, reg, auth.NewVerifier(iss.URL, "acquire"))
	_, nzb := newFakeNZBGet(t)

	types := decode[[]adapters.TypeInfo](t, tg.call(t, http.MethodGet, "/api/v1/config/types", nil, token))
	if len(types) != 3 {
		t.Fatalf("types = %+v", types)
	}

	empty := decode[configClientsResponse](t, tg.call(t, http.MethodGet, "/api/v1/config/clients", nil, token))
	if empty.Revision != 0 || len(empty.Clients) != 1 || empty.Clients[0].Source != "env" {
		t.Fatalf("before any push = %+v", empty)
	}

	rec := tg.call(t, http.MethodPut, "/api/v1/config/clients", map[string]any{
		"revision": 7,
		"clients": []map[string]any{
			{"id": "usenet", "type": "nzbget", "baseUrl": nzb.URL, "auth": "basic",
				"username": "nzbget", "secret": pushedSecret, "category": "acquire", "protocols": []string{"usenet"}},
			{"id": "odownloader", "type": "odownloader", "baseUrl": "http://other.example", "auth": "none"},
			{"id": "broken", "type": "qbittorrent", "baseUrl": "ftp://x", "auth": "none"},
		},
	}, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("push = %d %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), pushedSecret) {
		t.Fatalf("push response echoes the secret: %s", rec.Body)
	}
	applied := decode[configApplyResponse](t, rec)
	if applied.Revision != 7 || len(applied.Applied) != 1 || applied.Applied[0] != "usenet" {
		t.Errorf("applied = %+v", applied)
	}
	errs := map[string]string{}
	for _, e := range applied.Errors {
		errs[e.ID] = e.Message
	}
	if !strings.Contains(errs["odownloader"], "environment") || !strings.Contains(errs["broken"], "baseUrl") || len(errs) != 2 {
		t.Errorf("errors = %+v", applied.Errors)
	}

	rec = tg.call(t, http.MethodGet, "/api/v1/config/clients", nil, token)
	if strings.Contains(rec.Body.String(), pushedSecret) || strings.Contains(rec.Body.String(), "env-token") {
		t.Fatalf("client list carries a secret: %s", rec.Body)
	}
	if strings.Contains(rec.Body.String(), `"secret"`) {
		t.Fatalf("client list has a secret field: %s", rec.Body)
	}
	list := decode[configClientsResponse](t, rec)
	if list.Revision != 7 || len(list.Clients) != 2 {
		t.Fatalf("list = %+v", list)
	}
	odl, usenet := list.Clients[0], list.Clients[1]
	if odl.ID != "odownloader" || odl.Source != "env" || odl.BaseURL != "http://odl.example" || !odl.SecretSet {
		t.Errorf("env client = %+v (an API push must not replace it)", odl)
	}
	if usenet.Source != "api" || !usenet.SecretSet || usenet.Username != "nzbget" || usenet.Category != "acquire" ||
		usenet.Type != "nzbget" || len(usenet.Protocols) != 1 {
		t.Errorf("api client = %+v", usenet)
	}

	ids := decode[[]string](t, tg.call(t, http.MethodGet, "/api/v1/clients", nil, token))
	if strings.Join(ids, ",") != "odownloader,usenet" {
		t.Errorf("GET /api/v1/clients = %v, want an array of ids", ids)
	}

	// An empty set removes every API client and nothing else.
	rec = tg.call(t, http.MethodPut, "/api/v1/config/clients", map[string]any{"revision": 8, "clients": []any{}}, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("empty push = %d %s", rec.Code, rec.Body)
	}
	if ids := decode[[]string](t, tg.call(t, http.MethodGet, "/api/v1/clients", nil, token)); strings.Join(ids, ",") != "odownloader" {
		t.Errorf("after the empty push = %v", ids)
	}
}

func TestConfigPushRejectsIncompleteBodies(t *testing.T) {
	iss := authtest.NewIssuer(t)
	token := iss.Token(t, map[string]any{"azp": "acquire"})
	tg := newTestGateway(t, nil, auth.NewVerifier(iss.URL, "acquire"))
	tg.push(t, 1, map[string]any{"id": "usenet", "type": "nzbget", "baseUrl": "http://nzb.example", "auth": "none"})

	for name, body := range map[string]any{
		"no clients":       map[string]any{"revision": 2},
		"null clients":     map[string]any{"revision": 2, "clients": nil},
		"no revision":      map[string]any{"clients": []any{}},
		"negative":         map[string]any{"revision": -1, "clients": []any{}},
		"not json":         []byte("{"),
		"clients not list": map[string]any{"revision": 2, "clients": "usenet"},
	} {
		if rec := tg.call(t, http.MethodPut, "/api/v1/config/clients", body, token); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d %s, want 400", name, rec.Code, rec.Body)
		}
	}
	if tg.reg.Get("usenet") == nil || tg.reg.Revision() != 1 {
		t.Fatal("a rejected push changed the configuration")
	}
}

func TestConfigTestEndpoint(t *testing.T) {
	iss := authtest.NewIssuer(t)
	token := iss.Token(t, map[string]any{"azp": "acquire"})
	tg := newTestGateway(t, nil, auth.NewVerifier(iss.URL, "acquire"))
	_, nzb := newFakeNZBGet(t)

	rec := tg.call(t, http.MethodPost, "/api/v1/config/clients/test", map[string]any{
		"id": "usenet", "type": "nzbget", "baseUrl": nzb.URL, "auth": "basic", "username": "u", "secret": pushedSecret,
	}, token)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), pushedSecret) {
		t.Fatalf("test = %d %s", rec.Code, rec.Body)
	}
	ok := decode[configTestResponse](t, rec)
	if !ok.Reachable || ok.Version != "24.3" || ok.Error != "" {
		t.Errorf("reachable client = %+v", ok)
	}
	if ok.Status.ID != "usenet" || ok.Status.Type != "nzbget" || !ok.Status.Reachable || ok.Status.Detail["news_servers_active"] != "1/1" {
		t.Errorf("status = %+v", ok.Status)
	}
	if tg.reg.Get("usenet") != nil {
		t.Fatal("testing a client registered it")
	}

	// Unreachable endpoint, invalid spec, bad id: all answered 200 with the reason.
	for name, body := range map[string]map[string]any{
		"unreachable": {"type": "nzbget", "baseUrl": "http://127.0.0.1:1", "auth": "none"},
		"invalid":     {"type": "nzbget", "baseUrl": "not a url", "auth": "none"},
		"bad id":      {"id": "Not_Valid", "type": "nzbget", "baseUrl": nzb.URL, "auth": "none"},
	} {
		rec := tg.call(t, http.MethodPost, "/api/v1/config/clients/test", body, token)
		res := decode[configTestResponse](t, rec)
		if rec.Code != http.StatusOK || res.Reachable || res.Error == "" || res.Status.Error == "" {
			t.Errorf("%s: %d %+v", name, rec.Code, res)
		}
	}
}
