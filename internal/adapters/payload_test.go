package adapters

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/iotest"
)

// fakeNZBGet records every JSON-RPC call and answers append with a fixed NZBID.
type fakeNZBGet struct {
	mu    sync.Mutex
	calls map[string][]json.RawMessage
}

func newFakeNZBGet(t *testing.T) (*fakeNZBGet, *httptest.Server) {
	f := &fakeNZBGet{calls: map[string][]json.RawMessage{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rpc struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&rpc); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.calls[rpc.Method] = rpc.Params
		f.mu.Unlock()
		switch rpc.Method {
		case "append":
			_, _ = w.Write([]byte(`{"result":42}`))
		case "version":
			_, _ = w.Write([]byte(`{"result":"24.3"}`))
		default:
			_, _ = w.Write([]byte(`{"result":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeNZBGet) params(t *testing.T, method string) []json.RawMessage {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.calls[method]
	if !ok {
		t.Fatalf("nzbget %s was never called", method)
	}
	return p
}

func str(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("param %s is not a string", raw)
	}
	return s
}

// With a payload NZBGet gets the file content (base64) and a .nzb filename,
// never the source link; the job category overrides the client default.
func TestNZBGetAddUploadsPayload(t *testing.T) {
	fake, srv := newFakeNZBGet(t)
	nzb := []byte(`<?xml version="1.0"?><nzb></nzb>`)

	id, err := NewNZBGet(srv.URL, "u", "p", "default").Add(t.Context(), Job{
		Source: "https://indexer.example/get?apikey=k", Title: "Some Title",
		Payload: nzb, PayloadName: "dir/Some.Release", Category: "acquire",
	})
	if err != nil || id != "42" {
		t.Fatalf("add = (%q, %v), want (42, nil)", id, err)
	}
	p := fake.params(t, "append")
	if got := str(t, p[0]); got != "Some.Release.nzb" {
		t.Errorf("filename = %q, want Some.Release.nzb", got)
	}
	content, err := base64.StdEncoding.DecodeString(str(t, p[1]))
	if err != nil || string(content) != string(nzb) {
		t.Errorf("content = %q (%v), want the NZB bytes", content, err)
	}
	if got := str(t, p[2]); got != "acquire" {
		t.Errorf("category = %q, want the job's", got)
	}
	if got := str(t, p[8]); got != "FORCE" {
		t.Errorf("dupe mode = %q", got)
	}
}

// Without a payload the URL path is unchanged: NZBGet fetches the source and
// the client's own category applies.
func TestNZBGetAddByURLKeepsClientCategory(t *testing.T) {
	fake, srv := newFakeNZBGet(t)
	if _, err := NewNZBGet(srv.URL, "u", "p", "default").Add(t.Context(), Job{
		Source: "https://nzb.example/x.nzb", Title: "X",
	}); err != nil {
		t.Fatal(err)
	}
	p := fake.params(t, "append")
	if str(t, p[0]) != "X.nzb" || str(t, p[1]) != "https://nzb.example/x.nzb" || str(t, p[2]) != "default" {
		t.Errorf("append params = %s", p)
	}
}

// A payload travels as a multipart "torrents" file part carrying the tag,
// save path and category; no urls field is sent.
func TestQBittorrentAddUploadsPayload(t *testing.T) {
	torrent := []byte("d8:announce0:4:infod4:name1:xee")
	var got struct {
		file                            []byte
		filename, tags, save, cat, urls string
		tmm                             string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "sid"})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/torrents/add":
			if r.Header.Get("Referer") == "" {
				http.Error(w, "csrf", http.StatusUnauthorized)
				return
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
				return
			}
			f, hdr, err := r.FormFile("torrents")
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			got.file, _ = io.ReadAll(f)
			got.filename = hdr.Filename
			got.tags, got.save, got.cat = r.FormValue("tags"), r.FormValue("savepath"), r.FormValue("category")
			got.urls, got.tmm = r.FormValue("urls"), r.FormValue("autoTMM")
			_, _ = w.Write([]byte("Ok."))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	q := NewQBittorrent(srv.URL, "u", "p")
	q.Category = "fallback"
	tag, err := q.Add(t.Context(), Job{
		Source: "https://tracker.example/t.torrent?passkey=k", Title: "Some Title",
		SavePath: "/downloads/acquire", Category: "acquire", Payload: torrent,
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if string(got.file) != string(torrent) {
		t.Errorf("uploaded %q, want the torrent bytes", got.file)
	}
	if got.filename != "Some Title.torrent" {
		t.Errorf("filename = %q", got.filename)
	}
	if got.tags != tag || got.save != "/downloads/acquire" || got.tmm != "false" || got.cat != "acquire" {
		t.Errorf("fields = %+v, tag %q", got, tag)
	}
	if got.urls != "" {
		t.Errorf("the source link leaked to the client: %q", got.urls)
	}
}

// A payload upload must survive a session expiry: authed resends the request
// after a re-login, so the body has to be readable twice.
func TestQBittorrentPayloadResendsAfterRelogin(t *testing.T) {
	var adds int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "sid"})
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/torrents/add":
			adds++
			if adds == 1 {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if f, _, err := r.FormFile("torrents"); err != nil {
				http.Error(w, "no file on resend", http.StatusBadRequest)
			} else {
				b, _ := io.ReadAll(f)
				if string(b) != "torrent" {
					http.Error(w, "empty file on resend", http.StatusBadRequest)
					return
				}
				_, _ = w.Write([]byte("Ok."))
			}
		}
	}))
	defer srv.Close()

	if _, err := NewQBittorrent(srv.URL, "u", "p").Add(t.Context(), Job{Title: "t", Payload: []byte("torrent")}); err != nil {
		t.Fatalf("add after re-login: %v", err)
	}
	if adds != 2 {
		t.Fatalf("add attempts = %d, want 2", adds)
	}
}

func TestODownloaderRejectsPayload(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer srv.Close()

	_, err := NewODownloader(srv.URL, "t").Add(t.Context(), Job{Source: "https://host.example/f", Payload: []byte("x")})
	if !errors.Is(err, ErrPayloadUnsupported) {
		t.Fatalf("err = %v, want ErrPayloadUnsupported", err)
	}
	if called {
		t.Error("the daemon should not be contacted for a payload it cannot take")
	}
}

func TestPayloadFileName(t *testing.T) {
	for _, tc := range []struct{ name, title, ext, want string }{
		{"Release.nzb", "t", ".nzb", "Release.nzb"},
		{"Release.NZB", "t", ".nzb", "Release.NZB"},
		{"Release", "t", ".nzb", "Release.nzb"},
		{"../../etc/passwd", "t", ".torrent", "passwd.torrent"},
		{`C:\x\y.torrent`, "t", ".torrent", "y.torrent"},
		{"", "Some Title", ".nzb", "Some Title.nzb"},
		{"a\x00b", "", ".nzb", "ab.nzb"},
		{"", "", ".nzb", "download.nzb"},
	} {
		if got := payloadFileName(tc.name, tc.title, tc.ext); got != tc.want {
			t.Errorf("payloadFileName(%q, %q) = %q, want %q", tc.name, tc.title, got, tc.want)
		}
	}
}

func TestProbes(t *testing.T) {
	_, nzb := newFakeNZBGet(t)
	if v, err := NewNZBGet(nzb.URL, "u", "p", "").Probe(t.Context()); err != nil || v != "24.3" {
		t.Errorf("nzbget probe = (%q, %v)", v, err)
	}

	qb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/auth/login":
			_, _ = w.Write([]byte("Ok."))
		case "/api/v2/app/version":
			_, _ = w.Write([]byte("v4.6.2"))
		}
	}))
	defer qb.Close()
	if v, err := NewQBittorrent(qb.URL, "u", "p").Probe(t.Context()); err != nil || v != "v4.6.2" {
		t.Errorf("qbittorrent probe = (%q, %v)", v, err)
	}

	// Something that answers 200 with a web page is not a qBittorrent WebUI.
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html><body>hello</body></html>"))
	}))
	defer page.Close()
	if _, err := NewQBittorrent(page.URL, "u", "p").Probe(t.Context()); err == nil {
		t.Error("an HTML page passed the qbittorrent probe")
	}

	odl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer good" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"content":[]}`))
	}))
	defer odl.Close()
	if _, err := NewODownloader(odl.URL, "good").Probe(t.Context()); err != nil {
		t.Errorf("odownloader probe with a good token: %v", err)
	}
	if _, err := NewODownloader(odl.URL, "bad").Probe(t.Context()); err == nil {
		t.Error("odownloader probe accepted a rejected token")
	}
}

func TestBase64Reader(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, base64Chunk - 1, base64Chunk, base64Chunk + 1, 3*base64Chunk + 2} {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(i * 7)
		}
		got, err := io.ReadAll(iotest.HalfReader(&base64Reader{src: src}))
		if err != nil || string(got) != base64.StdEncoding.EncodeToString(src) {
			t.Errorf("%d bytes: encoded differently (%v)", n, err)
		}
	}
}

// The streamed append body is exactly what marshalling the call would send,
// with a Content-Length rather than chunked encoding.
func TestNZBGetStreamedAppendMatchesMarshal(t *testing.T) {
	var raw []byte
	var length int64
	var chunked bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ = io.ReadAll(r.Body)
		length, chunked = r.ContentLength, len(r.TransferEncoding) > 0
		_, _ = w.Write([]byte(`{"result":42}`))
	}))
	defer srv.Close()

	payload := make([]byte, 3*base64Chunk+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	title := `A <B> & "C"`
	if _, err := NewNZBGet(srv.URL, "", "", "").Add(t.Context(), Job{Title: title, Payload: payload, Category: "acquire"}); err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(map[string]any{
		"method": "append", "id": 1,
		"params": []any{title + ".nzb", base64.StdEncoding.EncodeToString(payload), "acquire", 0, false, false, "", 0, "FORCE"},
	})
	if string(raw) != string(want) {
		t.Fatalf("streamed body differs from the marshalled call:\n got %.200s\nwant %.200s", raw, want)
	}
	if length != int64(len(want)) || chunked {
		t.Errorf("content length = %d (chunked %v), want %d", length, chunked, len(want))
	}
}
