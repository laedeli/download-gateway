package adapters

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
)

func TestRefusedAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"169.254.169.254":        true,
		"169.254.0.1":            true,
		"::ffff:169.254.169.254": true,
		"fe80::1":                true,
		"100.100.100.200":        true,
		"fd00:ec2::254":          true,
		"10.0.0.5":               false,
		"192.168.1.10":           false,
		"127.0.0.1":              false,
		"100.100.100.201":        false,
		"2001:db8::1":            false,
	} {
		if got := RefusedAddr(netip.MustParseAddr(addr)); got != want {
			t.Errorf("RefusedAddr(%s) = %v, want %v", addr, got, want)
		}
	}
}

func mustClient(t *testing.T, spec ClientSpec) Adapter {
	t.Helper()
	ad, _, err := NewClient(spec)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return ad
}

// The refusal happens when connecting, on the resolved address, so it holds
// for literal addresses and names alike; literals keep the test off the network.
func TestPushedClientRefusesMetadataAddresses(t *testing.T) {
	for _, base := range []string{"http://169.254.169.254", "http://[::ffff:169.254.169.254]:8080", "http://[fd00:ec2::254]"} {
		ad := mustClient(t, ClientSpec{Type: "nzbget", BaseURL: base, Auth: AuthNone})
		_, err := ad.(Prober).Probe(t.Context())
		if err == nil || !strings.Contains(err.Error(), "link-local or metadata") {
			t.Errorf("probe of %s = %v, want a refusal", base, err)
		}
	}
}

// internalServer stands in for something only the gateway can reach. It records
// every request body it receives.
type internalServer struct {
	mu     sync.Mutex
	bodies []string
}

func newInternalServer(t *testing.T) (*internalServer, *httptest.Server) {
	s := &internalServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.bodies = append(s.bodies, r.URL.Path+" "+string(b))
		s.mu.Unlock()
		_, _ = w.Write([]byte("internal-role-name"))
	}))
	t.Cleanup(srv.Close)
	return s, srv
}

// A pushed qBittorrent client whose endpoint redirects elsewhere must neither
// re-post its login form there nor return what the other host answered.
func TestPushedClientRefusesRedirectToAnotherHost(t *testing.T) {
	internal, target := newInternalServer(t)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := http.StatusFound
		if r.URL.Path == "/api/v2/auth/login" {
			code = http.StatusTemporaryRedirect // re-sends the POST body when followed
		}
		http.Redirect(w, r, target.URL+r.URL.Path, code)
	}))
	defer front.Close()

	ad := mustClient(t, ClientSpec{Type: "qbittorrent", BaseURL: front.URL, Auth: AuthBasic, Username: "u", Secret: "hunter2"})
	version, err := ad.(Prober).Probe(t.Context())
	if err == nil || !strings.Contains(err.Error(), errRedirectRefused.Error()) {
		t.Fatalf("probe = (%q, %v), want the redirect refused", version, err)
	}
	internal.mu.Lock()
	defer internal.mu.Unlock()
	if len(internal.bodies) != 0 {
		t.Fatalf("the other host received %q", internal.bodies)
	}
}

// Moving to https on the same host is still another scheme: refused.
func TestPushedClientRefusesSchemeChange(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://"+srv.Listener.Addr().String()+r.URL.Path, http.StatusMovedPermanently)
	}))
	defer srv.Close()
	ad := mustClient(t, ClientSpec{Type: "nzbget", BaseURL: srv.URL, Auth: AuthNone})
	if _, err := ad.(Prober).Probe(t.Context()); err == nil || !strings.Contains(err.Error(), errRedirectRefused.Error()) {
		t.Fatalf("probe = %v, want the redirect refused", err)
	}
}

// A redirect within the client's own origin is still followed.
func TestPushedClientFollowsSameOriginRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/app/version":
			http.Redirect(w, r, "/webui/api/v2/app/version", http.StatusFound)
		case "/webui/api/v2/app/version":
			_, _ = w.Write([]byte("v4.6.2"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	ad := mustClient(t, ClientSpec{Type: "qbittorrent", BaseURL: srv.URL, Auth: AuthNone})
	if v, err := ad.(Prober).Probe(t.Context()); err != nil || v != "v4.6.2" {
		t.Fatalf("probe = (%q, %v)", v, err)
	}
}

// Environment clients are built as they always were.
func TestEnvClientsKeepDefaultHTTP(t *testing.T) {
	for name, c := range map[string]*http.Client{
		"nzbget":      NewNZBGet("http://nzb.example", "", "", "").HTTP,
		"qbittorrent": NewQBittorrent("http://qb.example", "", "").HTTP,
		"odownloader": NewODownloader("http://odl.example", "").HTTP,
	} {
		if c.Transport != nil || c.CheckRedirect != nil {
			t.Errorf("%s: the constructor applied the pushed-client policy", name)
		}
	}
}
