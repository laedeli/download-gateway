package adapters

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDecodeCapped(t *testing.T) {
	var s string
	if err := decodeCapped(strings.NewReader(`"`+strings.Repeat("a", 100)+`"`), 102, &s); err != nil || len(s) != 100 {
		t.Fatalf("a value at the cap = (%d bytes, %v)", len(s), err)
	}
	err := decodeCapped(strings.NewReader(`"`+strings.Repeat("a", 2<<20)+`"`), 1<<20, &s)
	if err == nil || !strings.Contains(err.Error(), "exceeds 1 MiB") {
		t.Fatalf("a value past the cap = %v", err)
	}
	if err := decodeCapped(strings.NewReader(`{`), 1<<20, &s); err == nil || strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("a short broken body = %v, want a decode error", err)
	}
}

// A client endpoint that streams more than a status object is cut off rather
// than read into memory.
func TestOversizedClientResponses(t *testing.T) {
	huge := `{"dl_info_speed":1,"connection_status":"` + strings.Repeat("x", 2<<20) + `"}`
	qb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/transfer/info" {
			_, _ = w.Write([]byte(huge))
		}
	}))
	defer qb.Close()
	if cs := NewQBittorrent(qb.URL, "", "").ClientStatus(t.Context()); cs.Reachable || !strings.Contains(cs.Error, "exceeds") {
		t.Errorf("qbittorrent status from an oversized body = %+v", cs)
	}

	nzb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":"` + strings.Repeat("9", 2<<20) + `"}`))
	}))
	defer nzb.Close()
	if _, err := NewNZBGet(nzb.URL, "", "", "").Probe(t.Context()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("nzbget probe of an oversized body = %v", err)
	}
}

func TestPublicError(t *testing.T) {
	se := &StatusError{Op: "qbittorrent login failed", Code: 403, Detail: "internal-page-text"}
	if !strings.Contains(se.Error(), "internal-page-text") {
		t.Errorf("Error() = %q, want the detail kept for logs", se.Error())
	}
	for _, err := range []error{se, fmt.Errorf("nzbget describe 7: %w", se)} {
		got := PublicError(err)
		if strings.Contains(got, "internal-page-text") || !strings.Contains(got, "HTTP 403") {
			t.Errorf("PublicError(%v) = %q", err, got)
		}
	}
	long := PublicError(errors.New("bad\x00\n" + strings.Repeat("é", 400)))
	if len(long) > maxErrorDetail || !utf8.ValidString(long) || strings.ContainsAny(long, "\x00\n") {
		t.Errorf("PublicError of a long message = %d bytes %q", len(long), long)
	}
	if PublicError(nil) != "" {
		t.Error("PublicError(nil) is not empty")
	}
}

func TestNZBGetProbeRejectsNonVersions(t *testing.T) {
	for _, result := range []string{`""`, `"<html>"`, `"` + strings.Repeat("1", 40) + `"`, `{"a":1}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"result":` + result + `}`))
		}))
		if v, err := NewNZBGet(srv.URL, "", "", "").Probe(t.Context()); err == nil {
			t.Errorf("result %s passed the probe as version %q", result, v)
		}
		srv.Close()
	}
}
