package adapters

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"testing"
)

const testSecret = "s3cr3t-never-shown"

func TestTypesCatalog(t *testing.T) {
	got := map[string]TypeInfo{}
	for _, ti := range Types() {
		got[ti.Type] = ti
	}
	for _, want := range []struct {
		typ       string
		protocols []string
		payload   bool
		savePath  bool
		pause     bool
	}{
		{"nzbget", []string{"usenet"}, true, false, true},
		{"qbittorrent", []string{"torrent"}, true, true, true},
		{"odownloader", []string{"http"}, false, false, false},
	} {
		ti, ok := got[want.typ]
		if !ok {
			t.Fatalf("type %s missing from %v", want.typ, got)
		}
		if !slices.Equal(ti.Protocols, want.protocols) || ti.AcceptsPayload != want.payload ||
			ti.SupportsSavePath != want.savePath || ti.CanPause != want.pause {
			t.Errorf("%s = %+v", want.typ, ti)
		}
	}
	if _, ok := got["jdownloader"]; ok || len(got) != 3 {
		t.Errorf("catalog = %v, want exactly the three wired types", got)
	}

	// Types hands out copies: mutating one must not change the catalog.
	Types()[0].Protocols[0] = "mutated"
	if Types()[0].Protocols[0] == "mutated" {
		t.Error("Types leaked the catalog's backing slice")
	}
}

func TestClientIDRule(t *testing.T) {
	for id, ok := range map[string]bool{
		"nzbget": true, "qb-2": true, "0": true, strings.Repeat("a", 40): true,
		"": false, "-lead": false, "Upper": false, "has.dot": false, "under_score": false,
		strings.Repeat("a", 41): false,
	} {
		if ValidClientID(id) != ok {
			t.Errorf("ValidClientID(%q) = %v, want %v", id, !ok, ok)
		}
	}
}

func TestPrepareSpec(t *testing.T) {
	base := ClientSpec{ID: "qb", Type: "qbittorrent", BaseURL: "http://qb.example:8080/", Auth: "basic",
		Username: " admin ", Secret: testSecret}

	s, err := PrepareSpec(base)
	if err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	if s.BaseURL != "http://qb.example:8080" || s.Username != "admin" || s.Secret != testSecret {
		t.Errorf("normalized = %+v", s)
	}
	if !slices.Equal(s.Protocols, []string{"torrent"}) {
		t.Errorf("protocols default = %v, want the type's", s.Protocols)
	}

	none, err := PrepareSpec(ClientSpec{Type: "nzbget", BaseURL: "https://nzb.example/sub", Auth: "none",
		Username: "leftover", Secret: "leftover"})
	if err != nil {
		t.Fatalf("auth none rejected: %v", err)
	}
	if none.Username != "" || none.Secret != "" {
		t.Errorf("auth none kept credentials: %+v", none)
	}

	for _, tc := range []struct {
		name string
		edit func(*ClientSpec)
		want string
	}{
		{"unknown type", func(s *ClientSpec) { s.Type = "jdownloader" }, "type must be one of"},
		{"empty url", func(s *ClientSpec) { s.BaseURL = "" }, "baseUrl"},
		{"relative url", func(s *ClientSpec) { s.BaseURL = "qb.example:8080" }, "baseUrl"},
		{"ftp url", func(s *ClientSpec) { s.BaseURL = "ftp://qb.example" }, "baseUrl"},
		{"credentials in url", func(s *ClientSpec) { s.BaseURL = "http://u:" + testSecret + "@qb.example" }, "credentials"},
		{"query", func(s *ClientSpec) { s.BaseURL = "http://qb.example/?x=1" }, "query"},
		{"auth not offered", func(s *ClientSpec) { s.Auth = "token" }, "auth must be one of basic, none"},
		{"empty auth", func(s *ClientSpec) { s.Auth = "" }, "auth must be one of"},
		{"basic without user", func(s *ClientSpec) { s.Username = "" }, "username is required"},
		{"protocol outside type", func(s *ClientSpec) { s.Protocols = []string{"usenet"} }, "protocols"},
		{"control char", func(s *ClientSpec) { s.Category = "a\nb" }, "control characters"},
		{"long save path", func(s *ClientSpec) { s.SavePath = "/" + strings.Repeat("x", maxSavePath) }, "savePath"},
	} {
		in := base
		tc.edit(&in)
		_, err := PrepareSpec(in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to mention %q", tc.name, err, tc.want)
			continue
		}
		if strings.Contains(err.Error(), testSecret) {
			t.Errorf("%s: error message quotes the secret: %v", tc.name, err)
		}
	}

	if _, err := PrepareSpec(ClientSpec{Type: "odownloader", BaseURL: "http://odl.example", Auth: "token"}); err == nil {
		t.Error("token auth without a secret must be rejected")
	}
}

// A changed secret must replace the adapter; an identical push must not.
func TestSpecHashCoversSecret(t *testing.T) {
	a := ClientSpec{Type: "nzbget", BaseURL: "http://n", Auth: "basic", Username: "u", Secret: "one"}
	b := a
	if specHash(a) != specHash(b) {
		t.Fatal("identical specs hash differently")
	}
	b.Secret = "two"
	if specHash(a) == specHash(b) {
		t.Fatal("a secret change does not change the hash")
	}
	// Field boundaries must not shift: "ab"+"c" is not "a"+"bc".
	c := ClientSpec{Type: "nzbget", BaseURL: "http://n", Auth: "basic", Username: "ab", Secret: "c"}
	d := ClientSpec{Type: "nzbget", BaseURL: "http://n", Auth: "basic", Username: "a", Secret: "bc"}
	if specHash(c) == specHash(d) {
		t.Fatal("hash is ambiguous across field boundaries")
	}
}

// Secrets are write-only: neither JSON encoding nor structured logging of a
// spec or an adapter may carry one.
func TestSecretsAreRedacted(t *testing.T) {
	spec := ClientSpec{ID: "nzb", Type: "nzbget", BaseURL: "http://user:" + testSecret + "@nzb.example",
		Auth: "basic", Username: "user", Secret: testSecret}

	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte(testSecret)) || bytes.Contains(b, []byte(`"secret"`)) {
		t.Errorf("JSON carries the secret: %s", b)
	}
	if !bytes.Contains(b, []byte(`"secretSet":true`)) {
		t.Errorf("JSON should say a secret is set: %s", b)
	}

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	log.Info("spec", "client", spec)
	log.Info("nzbget", "adapter", NewNZBGet(spec.BaseURL, "user", testSecret, "c"))
	log.Info("qbittorrent", "adapter", NewQBittorrent(spec.BaseURL, "user", testSecret))
	log.Info("odownloader", "adapter", NewODownloader(spec.BaseURL, testSecret))
	if strings.Contains(logs.String(), testSecret) {
		t.Errorf("logs carry the secret:\n%s", logs.String())
	}
}
