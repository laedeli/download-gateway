package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/laedeli/download-gateway/internal/adapters"
	"github.com/laedeli/download-gateway/internal/auth"
)

func openVerifier() *auth.Verifier { return auth.NewVerifier("") }

func TestReadyz(t *testing.T) {
	tg := newTestGateway(t, nil, openVerifier())

	// No clients at all is still ready: they arrive by config push.
	if rec := tg.call(t, http.MethodGet, "/readyz", nil, ""); rec.Code != http.StatusOK {
		t.Fatalf("readyz with zero clients = %d %s", rec.Code, rec.Body)
	}

	tg.pub.notReady.Store(true)
	if rec := tg.call(t, http.MethodGet, "/readyz", nil, ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with the publisher down = %d", rec.Code)
	}
	tg.pub.notReady.Store(false)

	tg.stopping.Store(true)
	if rec := tg.call(t, http.MethodGet, "/readyz", nil, ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz while shutting down = %d", rec.Code)
	}
}

// readyz is public even when the API requires a token.
func TestReadyzIsPublic(t *testing.T) {
	tg := newTestGateway(t, nil, auth.NewVerifier("http://issuer.invalid", "acquire"))
	if rec := tg.call(t, http.MethodGet, "/readyz", nil, ""); rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d", rec.Code)
	}
}

// Env clients keep their historical ids and are listed with source env, their
// secret reduced to a flag.
func TestEnvClientsRegistered(t *testing.T) {
	t.Setenv("NZBGET_URL", "http://nzbget.example:6789")
	t.Setenv("NZBGET_USER", "nzbget")
	t.Setenv("NZBGET_PASS", "env-secret")
	t.Setenv("NZBGET_CATEGORY", "acquire")
	t.Setenv("QBITTORRENT_BASE_URL", "http://qbittorrent.example:8080")
	t.Setenv("QBITTORRENT_USER", "")
	t.Setenv("QBITTORRENT_PASS", "")
	t.Setenv("ODOWNLOADER_BASE_URL", "")

	reg := adapters.NewRegistry()
	registerEnvClients(reg)

	nzb, ok := reg.Get("nzbget").(*adapters.NZBGet)
	if !ok || nzb.Username != "nzbget" || nzb.Password != "env-secret" || nzb.Category != "acquire" {
		t.Fatalf("env nzbget adapter = %+v", reg.Get("nzbget"))
	}
	list := reg.List()
	if len(list) != 2 {
		t.Fatalf("list = %+v", list)
	}
	n, q := list[0], list[1]
	if n.ID != "nzbget" || n.Source != "env" || n.Auth != "basic" || !n.SecretSet || n.Category != "acquire" {
		t.Errorf("nzbget = %+v", n)
	}
	if q.ID != "qbittorrent" || q.Auth != "none" || q.SecretSet || q.Protocols[0] != "torrent" {
		t.Errorf("qbittorrent = %+v", q)
	}
}

// With a payload NZBGet gets the file content; the job takes the client's
// category and the events carry the client id and type.
func TestAddPayloadNZBGet(t *testing.T) {
	fake, srv := newFakeNZBGet(t)
	tg := newTestGateway(t, nil, openVerifier())
	tg.push(t, 1, map[string]any{
		"id": "usenet", "type": "nzbget", "baseUrl": srv.URL, "auth": "basic",
		"username": "nzbget", "secret": "pw", "category": "acquire",
	})

	nzb := []byte(`<?xml version="1.0"?><nzb/>`)
	rec := tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{
		"client": "usenet", "title": "Some Title", "wanted_item_id": "w_1",
		"payload_b64": base64.StdEncoding.EncodeToString(nzb), "payload_name": "Some.Title.nzb",
	}, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("add = %d %s", rec.Code, rec.Body)
	}
	out := decode[map[string]string](t, rec)
	if out["adapter"] != "usenet" || out["client_job_id"] != "7" {
		t.Errorf("response = %v", out)
	}

	fake.mu.Lock()
	params := fake.append
	fake.mu.Unlock()
	var filename, content, category string
	_ = json.Unmarshal(params[0], &filename)
	_ = json.Unmarshal(params[1], &content)
	_ = json.Unmarshal(params[2], &category)
	if got, _ := base64.StdEncoding.DecodeString(content); !bytes.Equal(got, nzb) {
		t.Errorf("uploaded %q, want the NZB", got)
	}
	if filename != "Some.Title.nzb" || category != "acquire" {
		t.Errorf("filename %q, category %q", filename, category)
	}

	if len(tg.pub.started) != 1 {
		t.Fatalf("started events = %+v", tg.pub.started)
	}
	if ev := tg.pub.started[0]; ev.Adapter != "usenet" || ev.ClientType != "nzbget" || ev.WantedItemID != "w_1" {
		t.Errorf("started = %+v", ev)
	}
	jobs := decode[[]jobDTO](t, tg.call(t, http.MethodGet, "/api/v1/downloads", nil, ""))
	if len(jobs) != 1 || jobs[0].Adapter != "usenet" || jobs[0].ClientJobID != "7" {
		t.Errorf("jobs = %+v", jobs)
	}
}

// A torrent payload is a multipart upload; save path and category default to
// the client's and can be overridden per add. adapter still works as an alias.
func TestAddPayloadQBittorrent(t *testing.T) {
	fake, srv := newFakeQB(t)
	tg := newTestGateway(t, nil, openVerifier())
	tg.push(t, 1, map[string]any{
		"id": "torrents", "type": "qbittorrent", "baseUrl": srv.URL, "auth": "none",
		"category": "acquire", "savePath": "/downloads/acquire",
	})
	torrent := []byte("d4:infod4:name1:xee")

	rec := tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{
		"client": "torrents", "title": "T", "payload_b64": base64.StdEncoding.EncodeToString(torrent),
	}, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("add = %d %s", rec.Code, rec.Body)
	}
	rec = tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{
		"adapter": "torrents", "source": "magnet:?xt=urn:btih:abc", "save_path": "/elsewhere", "category": "other",
	}, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("add by alias = %d %s", rec.Code, rec.Body)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.files) != 1 || !bytes.Equal(fake.files[0], torrent) {
		t.Fatalf("uploaded files = %q", fake.files)
	}
	first, second := fake.adds[0], fake.adds[1]
	if first.Get("savepath") != "/downloads/acquire" || first.Get("category") != "acquire" || first.Get("urls") != "" {
		t.Errorf("payload add defaults = %v", first)
	}
	if second.Get("savepath") != "/elsewhere" || second.Get("category") != "other" || second.Get("urls") != "magnet:?xt=urn:btih:abc" {
		t.Errorf("link add overrides = %v", second)
	}
}

func TestAddPayloadRejectedForODownloader(t *testing.T) {
	var called atomic.Bool
	srv := newCountingServer(t, &called)
	tg := newTestGateway(t, nil, openVerifier())
	tg.push(t, 1, map[string]any{"id": "hoster", "type": "odownloader", "baseUrl": srv, "auth": "token", "secret": "t"})

	rec := tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{
		"client": "hoster", "payload_b64": base64.StdEncoding.EncodeToString([]byte("x")),
	}, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("payload to odownloader = %d %s, want 400", rec.Code, rec.Body)
	}
	if called.Load() {
		t.Error("the daemon was contacted for a payload it cannot take")
	}
}

func TestAddValidation(t *testing.T) {
	_, srv := newFakeQB(t)
	tg := newTestGateway(t, nil, openVerifier())
	tg.push(t, 1, map[string]any{"id": "torrents", "type": "qbittorrent", "baseUrl": srv.URL, "auth": "none"})

	for _, tc := range []struct {
		name string
		body any
		want int
	}{
		{"not json", []byte("{"), http.StatusBadRequest},
		{"no client", map[string]any{"source": "magnet:?x"}, http.StatusBadRequest},
		{"no source or payload", map[string]any{"client": "torrents"}, http.StatusBadRequest},
		{"aliases disagree", map[string]any{"client": "torrents", "adapter": "other", "source": "magnet:?x"}, http.StatusBadRequest},
		{"bad base64", map[string]any{"client": "torrents", "payload_b64": "%%%"}, http.StatusBadRequest},
		{"unknown client", map[string]any{"client": "nope", "source": "magnet:?x"}, http.StatusNotFound},
	} {
		if rec := tg.call(t, http.MethodPost, "/api/v1/downloads", tc.body, ""); rec.Code != tc.want {
			t.Errorf("%s: status = %d %s, want %d", tc.name, rec.Code, rec.Body, tc.want)
		}
	}

	huge := []byte(`{"client":"torrents","payload_b64":"` + strings.Repeat("A", maxAddBody) + `"}`)
	if rec := tg.call(t, http.MethodPost, "/api/v1/downloads", huge, ""); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("body over the cap = %d, want 413", rec.Code)
	}
}

// The largest release file acquire fetches must fit an add, base64 and all,
// with generous room for the other fields.
func TestAddBodyFitsLargestPayload(t *testing.T) {
	envelope, _ := json.Marshal(map[string]any{
		"client": strings.Repeat("c", 40), "title": strings.Repeat("t", 1<<10),
		"source": strings.Repeat("s", 16<<10), "save_path": strings.Repeat("p", 1<<10),
		"wanted_item_id": strings.Repeat("w", 64), "payload_name": strings.Repeat("n", 1<<10),
		"category": strings.Repeat("k", 128), "payload_b64": "",
	})
	if size := len(envelope) + base64.StdEncoding.EncodedLen(maxPayload); size > maxAddBody {
		t.Fatalf("an add with a %d MiB payload is %d bytes, over the %d byte cap", maxPayload>>20, size, maxAddBody)
	}
}

// Large adds share a memory budget: past it they are turned away with 429,
// while small adds still go through.
func TestLargeAddsShareABudget(t *testing.T) {
	_, srv := newFakeQB(t)
	tg := newTestGateway(t, nil, openVerifier())
	tg.push(t, 1, map[string]any{"id": "torrents", "type": "qbittorrent", "baseUrl": srv.URL, "auth": "none"})
	large := map[string]any{"client": "torrents", "payload_b64": base64.StdEncoding.EncodeToString(make([]byte, smallAddBody))}

	// One add of the largest size plus one just short of it leave less room
	// than the large add below needs.
	first, ok := tg.admitAdd(maxAddBody)
	second, ok2 := tg.admitAdd(addBodyBudget - maxAddBody - smallAddBody)
	if !ok || !ok2 {
		t.Fatal("the budget refused reservations inside it")
	}
	rec := tg.call(t, http.MethodPost, "/api/v1/downloads", large, "")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("large add past the budget = %d %s, want 429 with Retry-After", rec.Code, rec.Body)
	}
	if rec := tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{"client": "torrents", "source": "magnet:?x"}, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("small add while the budget is taken = %d %s", rec.Code, rec.Body)
	}
	if _, ok := tg.admitAdd(-1); ok {
		t.Error("a body of unknown length was admitted past the budget")
	}

	first()
	first() // idempotent
	second()
	if rec := tg.call(t, http.MethodPost, "/api/v1/downloads", large, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("large add after the budget freed = %d %s", rec.Code, rec.Body)
	}
	if tg.bodyInFlight != 0 {
		t.Errorf("budget in use after every add finished = %d", tg.bodyInFlight)
	}
}

// A client removed while it still has a job keeps being polled until the job
// finishes, refuses new adds meanwhile, and disappears afterwards.
func TestDrainingClient(t *testing.T) {
	fake, srv := newFakeQB(t)
	tg := newTestGateway(t, nil, openVerifier())
	client := map[string]any{"id": "torrents", "type": "qbittorrent", "baseUrl": srv.URL, "auth": "none"}
	tg.push(t, 1, client)

	rec := tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{
		"client": "torrents", "source": "magnet:?xt=urn:btih:abc", "wanted_item_id": "w_9",
	}, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("add = %d %s", rec.Code, rec.Body)
	}
	tag := decode[map[string]string](t, rec)["client_job_id"]

	res := tg.push(t, 2)
	if len(res.Draining) != 1 || res.Draining[0] != "torrents" {
		t.Fatalf("push without the client = %+v, want it draining", res)
	}
	if ids := decode[[]string](t, tg.call(t, http.MethodGet, "/api/v1/clients", nil, "")); len(ids) != 0 {
		t.Errorf("a draining client is still listed: %v", ids)
	}
	rec = tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{"client": "torrents", "source": "magnet:?x"}, "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("add to a draining client = %d, want 409", rec.Code)
	}

	tg.pollAll(t.Context())
	if len(tg.pub.progress) != 1 || tg.pub.progress[0].Adapter != "torrents" || tg.pub.progress[0].ClientType != "qbittorrent" {
		t.Fatalf("progress while draining = %+v", tg.pub.progress)
	}
	if tg.reg.Get("torrents") == nil {
		t.Fatal("the client was dropped while its job still runs")
	}

	fake.setState(tag, "uploading", 1)
	tg.pollAll(t.Context())
	if len(tg.pub.completed) != 1 || tg.pub.completed[0].WantedItemID != "w_9" || tg.pub.completed[0].ClientType != "qbittorrent" {
		t.Fatalf("completed = %+v", tg.pub.completed)
	}
	if tg.reg.Get("torrents") != nil {
		t.Fatal("the drained client was not dropped after its last job finished")
	}
}

// A pushed client's running jobs are adopted; a replace push re-adopts without
// resetting jobs that are already tracked.
func TestAdoptOnPush(t *testing.T) {
	fake, srv := newFakeQB(t)
	fake.torrents = []map[string]any{
		{"hash": "a", "name": "running", "state": "downloading", "progress": 0.3, "tags": "dlg-1111"},
		{"hash": "b", "name": "finished", "state": "uploading", "progress": 1.0, "tags": "dlg-2222"},
	}
	tg := newTestGateway(t, nil, openVerifier())
	client := map[string]any{"id": "torrents", "type": "qbittorrent", "baseUrl": srv.URL, "auth": "basic",
		"username": "admin", "secret": "one"}
	tg.push(t, 1, client)

	tg.mu.Lock()
	job := tg.jobs["torrents:dlg-1111"]
	_, finished := tg.jobs["torrents:dlg-2222"]
	tg.mu.Unlock()
	if job == nil || job.clientType != "qbittorrent" || finished {
		t.Fatalf("adopted jobs = %v", tg.snapshot())
	}

	tg.mu.Lock()
	job.wantedItemID = "w_1"
	tg.mu.Unlock()
	client["secret"] = "two"
	if res := tg.push(t, 2, client); len(res.Started) != 1 {
		t.Fatalf("a changed secret did not replace the client: %+v", res)
	}
	tg.mu.Lock()
	defer tg.mu.Unlock()
	if tg.jobs["torrents:dlg-1111"].wantedItemID != "w_1" {
		t.Fatal("re-adoption reset a tracked job")
	}
}

func TestClientStatusCarriesIDAndType(t *testing.T) {
	_, nzb := newFakeNZBGet(t)
	tg := newTestGateway(t, nil, openVerifier())
	tg.push(t, 1, map[string]any{"id": "usenet", "type": "nzbget", "baseUrl": nzb.URL, "auth": "none"},
		map[string]any{"id": "hoster", "type": "odownloader", "baseUrl": "http://odl.invalid", "auth": "none"})

	statuses := decode[[]adapters.ClientStatus](t, tg.call(t, http.MethodGet, "/api/v1/clients/status", nil, ""))
	if len(statuses) != 2 {
		t.Fatalf("statuses = %+v", statuses)
	}
	h, u := statuses[0], statuses[1]
	if h.ID != "hoster" || h.Type != "odownloader" || h.Reachable {
		t.Errorf("odownloader status = %+v", h)
	}
	if u.ID != "usenet" || u.Type != "nzbget" || u.Name != "nzbget" || !u.Reachable {
		t.Errorf("nzbget status = %+v", u)
	}
}
