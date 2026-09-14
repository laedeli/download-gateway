package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laedeli/download-gateway/internal/adapters"
	"github.com/laedeli/download-gateway/internal/auth"
	"github.com/laedeli/download-gateway/internal/events"
)

// recorder is a publisher that keeps every event for assertions.
type recorder struct {
	mu        sync.Mutex
	started   []events.Started
	progress  []events.Progress
	completed []events.Completed
	failed    []events.Failed
	notReady  atomic.Bool
}

func (p *recorder) EmitStarted(_ context.Context, e events.Started) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started = append(p.started, e)
	return nil
}

func (p *recorder) EmitProgress(_ context.Context, e events.Progress) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.progress = append(p.progress, e)
	return nil
}

func (p *recorder) EmitCompleted(_ context.Context, e events.Completed) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed = append(p.completed, e)
	return nil
}

func (p *recorder) EmitFailed(_ context.Context, e events.Failed) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed = append(p.failed, e)
	return nil
}

func (p *recorder) Ready() bool { return !p.notReady.Load() }

// testGateway is a gateway with an open API (no issuer) unless a verifier is
// given, plus the handler to call it through.
type testGateway struct {
	*gateway
	pub     *recorder
	handler http.Handler
}

func newTestGateway(t *testing.T, reg *adapters.Registry, verifier *auth.Verifier) *testGateway {
	t.Helper()
	if reg == nil {
		reg = adapters.NewRegistry()
	}
	pub := &recorder{}
	g := newGateway(t.Context(), reg, pub, time.Hour)
	t.Cleanup(g.bg.Wait)
	return &testGateway{gateway: g, pub: pub, handler: g.routes(verifier)}
}

// call sends a request with an optional JSON body and bearer token.
func (tg *testGateway) call(t *testing.T, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if raw, ok := body.([]byte); ok {
			buf.Write(raw)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	tg.handler.ServeHTTP(rec, req)
	return rec
}

// push applies a client configuration directly (the HTTP config API is
// covered separately) and waits for background adoption to finish.
func (tg *testGateway) push(t *testing.T, revision int64, clients ...map[string]any) adapters.ApplyResult {
	t.Helper()
	// Specs go through JSON like a real push: ClientSpec's own MarshalJSON drops
	// secrets, so tests describe clients as plain maps.
	raw, _ := json.Marshal(clients)
	var specs []adapters.ClientSpec
	if err := json.Unmarshal(raw, &specs); err != nil {
		t.Fatalf("decode specs: %v", err)
	}
	res := tg.reg.Apply(revision, specs, tg.hasJobs)
	for _, c := range res.Started {
		tg.bg.Add(1)
		go func(id string) {
			defer tg.bg.Done()
			tg.adopt(tg.ctx, id)
		}(c.ID)
	}
	tg.bg.Wait()
	return res
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return v
}

// fakeQB is a qBittorrent WebUI with one torrent list it serves by tag.
type fakeQB struct {
	mu       sync.Mutex
	torrents []map[string]any
	adds     []url.Values
	files    [][]byte
}

func newFakeQB(t *testing.T) (*fakeQB, *httptest.Server) {
	f := &fakeQB{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "sid"})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/app/version":
			_, _ = w.Write([]byte("v4.6.2"))
		case "/api/v2/transfer/info":
			_, _ = w.Write([]byte(`{"dl_info_speed":10,"up_info_speed":1,"connection_status":"connected"}`))
		case "/api/v2/torrents/add":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				_ = r.ParseForm()
			}
			f.adds = append(f.adds, r.Form)
			if file, _, err := r.FormFile("torrents"); err == nil {
				b := new(bytes.Buffer)
				_, _ = b.ReadFrom(file)
				f.files = append(f.files, b.Bytes())
			}
			f.torrents = append(f.torrents, map[string]any{
				"hash": "h", "name": "added", "state": "downloading", "progress": 0.1,
				"tags": r.FormValue("tags"), "category": r.FormValue("category"),
			})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/torrents/info":
			tag := r.URL.Query().Get("tag")
			out := []map[string]any{}
			for _, t := range f.torrents {
				if tag == "" || t["tags"] == tag {
					out = append(out, t)
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeQB) setState(tag, state string, progress float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.torrents {
		if t["tags"] == tag {
			t["state"], t["progress"] = state, progress
		}
	}
}

// fakeNZBGet answers JSON-RPC and records the last append.
type fakeNZBGet struct {
	mu     sync.Mutex
	append []json.RawMessage
	calls  int
}

func newFakeNZBGet(t *testing.T) (*fakeNZBGet, *httptest.Server) {
	f := &fakeNZBGet{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rpc struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&rpc)
		f.mu.Lock()
		f.calls++
		f.mu.Unlock()
		switch rpc.Method {
		case "append":
			f.mu.Lock()
			f.append = rpc.Params
			f.mu.Unlock()
			_, _ = w.Write([]byte(`{"result":7}`))
		case "version":
			_, _ = w.Write([]byte(`{"result":"24.3"}`))
		case "status":
			_, _ = w.Write([]byte(`{"result":{"DownloadRate":5,"NewsServers":[{"ID":1,"Active":true}]}}`))
		default:
			_, _ = w.Write([]byte(`{"result":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

// newCountingServer records whether anything called it and returns its URL.
func newCountingServer(t *testing.T, called *atomic.Bool) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
		http.Error(w, "unexpected call", http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}
