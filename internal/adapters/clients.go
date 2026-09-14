package adapters

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// Protocols a client can carry. acquire routes a grab to a client by protocol.
const (
	ProtocolUsenet  = "usenet"
	ProtocolTorrent = "torrent"
	ProtocolHTTP    = "http"
)

// Auth modes a client can be configured with. "basic" is a username plus a
// secret password, "token" a secret bearer token, "none" no credentials at all.
const (
	AuthBasic = "basic"
	AuthToken = "token"
	AuthNone  = "none"
)

// Where a client's configuration came from. Env clients are wired at boot from
// QBITTORRENT_*/NZBGET_*/ODOWNLOADER_* and cannot be replaced over the API.
const (
	SourceEnv = "env"
	SourceAPI = "api"
)

// TypeInfo describes one compiled-in adapter type: what a client of that type
// carries and how it can be configured. It is the body of
// GET /api/v1/config/types.
type TypeInfo struct {
	Type             string   `json:"type"`
	Protocols        []string `json:"protocols"`
	Auth             []string `json:"auth"`
	AcceptsPayload   bool     `json:"acceptsPayload"`
	SupportsSavePath bool     `json:"supportsSavePath"`
	CanPause         bool     `json:"canPause"`

	build func(ClientSpec) Adapter
}

// catalog is the set of adapter types a client can be created from. The
// JDownloader stub is deliberately absent: it is not wired to anything real.
//
// Every build applies the pushed-client endpoint policy (see guard); the
// environment clients main wires with the constructors directly do not get it.
var catalog = []TypeInfo{
	{
		Type: "nzbget", Protocols: []string{ProtocolUsenet}, Auth: []string{AuthBasic, AuthNone},
		AcceptsPayload: true, CanPause: true,
		build: func(s ClientSpec) Adapter {
			n := NewNZBGet(s.BaseURL, s.Username, s.Secret, s.Category)
			guard(n.HTTP)
			return n
		},
	},
	{
		Type: "odownloader", Protocols: []string{ProtocolHTTP}, Auth: []string{AuthToken, AuthNone},
		build: func(s ClientSpec) Adapter {
			o := NewODownloader(s.BaseURL, s.Secret)
			guard(o.HTTP)
			return o
		},
	},
	{
		Type: "qbittorrent", Protocols: []string{ProtocolTorrent}, Auth: []string{AuthBasic, AuthNone},
		AcceptsPayload: true, SupportsSavePath: true, CanPause: true,
		build: func(s ClientSpec) Adapter {
			q := NewQBittorrent(s.BaseURL, s.Username, s.Secret)
			q.Category = s.Category
			guard(q.HTTP)
			return q
		},
	},
}

// Types returns the adapter types a client can be configured with, sorted by
// name. The slices are copies, so callers may not mutate the catalog.
func Types() []TypeInfo {
	out := make([]TypeInfo, 0, len(catalog))
	for _, t := range catalog {
		t.Protocols = slices.Clone(t.Protocols)
		t.Auth = slices.Clone(t.Auth)
		out = append(out, t)
	}
	return out
}

// LookupType returns the type info for a type name.
func LookupType(name string) (TypeInfo, bool) {
	for _, t := range catalog {
		if t.Type == name {
			return t, true
		}
	}
	return TypeInfo{}, false
}

// ClientSpec is one configured client: an instance of a compiled-in adapter
// type with its endpoint and credentials. It is the client object of
// PUT /api/v1/config/clients.
//
// Secret is write-only. It never leaves the gateway: MarshalJSON and LogValue
// both drop it, so an accidental writeJSON or slog call cannot leak it.
type ClientSpec struct {
	ID        string   `json:"id"`
	Type      string   `json:"type"`
	BaseURL   string   `json:"baseUrl"`
	Auth      string   `json:"auth"`
	Username  string   `json:"username"`
	Secret    string   `json:"secret"`
	Category  string   `json:"category"`
	SavePath  string   `json:"savePath"`
	Protocols []string `json:"protocols"`
}

// MarshalJSON encodes the spec without its secret; secretSet says whether one
// is configured.
func (s ClientSpec) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.info(""))
}

// LogValue redacts the secret from structured logs.
func (s ClientSpec) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", s.ID),
		slog.String("type", s.Type),
		slog.String("base_url", RedactURL(s.BaseURL)),
		slog.String("auth", s.Auth),
		slog.String("category", s.Category),
		slog.Bool("secret_set", s.Secret != ""),
	)
}

// ClientInfo is a client as GET /api/v1/config/clients reports it. It has no
// secret field at all, only whether one is set.
type ClientInfo struct {
	ID        string   `json:"id"`
	Type      string   `json:"type"`
	BaseURL   string   `json:"baseUrl"`
	Auth      string   `json:"auth"`
	Username  string   `json:"username"`
	Category  string   `json:"category"`
	SavePath  string   `json:"savePath"`
	Protocols []string `json:"protocols"`
	Source    string   `json:"source,omitempty"`
	SecretSet bool     `json:"secretSet"`
}

func (s ClientSpec) info(source string) ClientInfo {
	protocols := slices.Clone(s.Protocols)
	if protocols == nil {
		protocols = []string{}
	}
	return ClientInfo{
		ID: s.ID, Type: s.Type, BaseURL: RedactURL(s.BaseURL), Auth: s.Auth, Username: s.Username,
		Category: s.Category, SavePath: s.SavePath, Protocols: protocols,
		Source: source, SecretSet: s.Secret != "",
	}
}

// clientIDPattern is the id rule shared with acquire's download_clients table.
var clientIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// ValidClientID reports whether id is a legal client id.
func ValidClientID(id string) bool { return clientIDPattern.MatchString(id) }

// Field limits. Generous, but they keep a hostile or broken config push from
// holding megabytes per client in memory.
const (
	maxBaseURL  = 2048
	maxUsername = 256
	maxSecret   = 4096
	maxCategory = 128
	maxSavePath = 1024
)

// PrepareSpec validates a client spec against its type and returns the
// normalized form: trimmed fields, no trailing slash on the base URL, default
// protocols, and credentials the auth mode does not use dropped. It does not
// check the id (a client can be tested before it is named); see ValidClientID.
//
// Error messages never quote a username, secret or URL.
func PrepareSpec(in ClientSpec) (ClientSpec, error) {
	s := in
	s.ID = strings.TrimSpace(s.ID)
	s.Type = strings.TrimSpace(s.Type)
	ti, ok := LookupType(s.Type)
	if !ok {
		names := make([]string, 0, len(catalog))
		for _, t := range catalog {
			names = append(names, t.Type)
		}
		return ClientSpec{}, fmt.Errorf("type must be one of %s", strings.Join(names, ", "))
	}

	raw := strings.TrimSpace(s.BaseURL)
	u, err := url.Parse(raw)
	switch {
	case raw == "" || len(raw) > maxBaseURL || err != nil:
		return ClientSpec{}, errors.New("baseUrl must be an absolute http or https URL")
	case u.Scheme != "http" && u.Scheme != "https", u.Host == "":
		return ClientSpec{}, errors.New("baseUrl must be an absolute http or https URL")
	case u.User != nil:
		return ClientSpec{}, errors.New("baseUrl must not carry credentials; use username and secret")
	case strings.ContainsAny(raw, "?#"):
		return ClientSpec{}, errors.New("baseUrl must not have a query or fragment")
	}
	s.BaseURL = strings.TrimRight(raw, "/")

	s.Auth = strings.TrimSpace(s.Auth)
	if !slices.Contains(ti.Auth, s.Auth) {
		return ClientSpec{}, fmt.Errorf("auth must be one of %s for %s", strings.Join(ti.Auth, ", "), ti.Type)
	}
	s.Username = strings.TrimSpace(s.Username)
	switch s.Auth {
	case AuthBasic:
		if s.Username == "" {
			return ClientSpec{}, errors.New("username is required for basic auth")
		}
	case AuthToken:
		s.Username = ""
		if s.Secret == "" {
			return ClientSpec{}, errors.New("secret is required for token auth")
		}
	case AuthNone:
		s.Username, s.Secret = "", ""
	}

	s.Category = strings.TrimSpace(s.Category)
	s.SavePath = strings.TrimSpace(s.SavePath)
	for _, f := range []struct {
		name, value string
		limit       int
	}{
		{"username", s.Username, maxUsername},
		{"secret", s.Secret, maxSecret},
		{"category", s.Category, maxCategory},
		{"savePath", s.SavePath, maxSavePath},
	} {
		if len(f.value) > f.limit {
			return ClientSpec{}, fmt.Errorf("%s is longer than %d bytes", f.name, f.limit)
		}
		if strings.ContainsFunc(f.value, isControl) {
			return ClientSpec{}, fmt.Errorf("%s must not contain control characters", f.name)
		}
	}

	protocols := make([]string, 0, len(s.Protocols))
	for _, p := range s.Protocols {
		p = strings.TrimSpace(p)
		if !slices.Contains(ti.Protocols, p) {
			return ClientSpec{}, fmt.Errorf("protocols for %s must be within %s", ti.Type, strings.Join(ti.Protocols, ", "))
		}
		if !slices.Contains(protocols, p) {
			protocols = append(protocols, p)
		}
	}
	if len(protocols) == 0 {
		protocols = slices.Clone(ti.Protocols)
	}
	slices.Sort(protocols)
	s.Protocols = protocols
	return s, nil
}

// NewClient validates spec and builds the adapter it describes.
func NewClient(spec ClientSpec) (Adapter, ClientSpec, error) {
	s, err := PrepareSpec(spec)
	if err != nil {
		return nil, ClientSpec{}, err
	}
	return buildClient(s), s, nil
}

// buildClient builds the adapter for a spec PrepareSpec already accepted.
func buildClient(s ClientSpec) Adapter {
	ti, _ := LookupType(s.Type)
	return ti.build(s)
}

// specHash fingerprints everything that shapes an adapter, secret included, so
// a changed password replaces the adapter while an identical push keeps it
// (and its session) running. Length-prefixed so field boundaries cannot shift.
func specHash(s ClientSpec) [sha256.Size]byte {
	h := sha256.New()
	for _, f := range []string{
		s.Type, s.BaseURL, s.Auth, s.Username, s.Secret, s.Category, s.SavePath,
		strings.Join(s.Protocols, ","),
	} {
		fmt.Fprintf(h, "%d:%s|", len(f), f)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

// RedactURL strips any userinfo from a URL before it is shown or logged. API
// clients cannot carry one (PrepareSpec refuses it), but an env URL might. A URL
// that does not even parse is dropped rather than echoed.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.User == nil {
		return raw
	}
	u.User = nil
	return u.String()
}
