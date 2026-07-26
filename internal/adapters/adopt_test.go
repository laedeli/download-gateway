package adapters

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A finished torrent keeps its tag and keeps seeding forever. Adopting it after
// a restart would fold to Completed on the next poll and replay a completed
// event — re-triggering ingest and knocking a fulfilled request backwards.
func TestQBittorrentAdoptSkipsTerminalTorrents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/auth/login" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Ok."))
			return
		}
		_ = json.NewEncoder(w).Encode([]qbTorrent{
			{Hash: "a", Name: "still-going", State: "downloading", Progress: 0.4, Tags: "dlg-1111"},
			{Hash: "b", Name: "seeding", State: "uploading", Progress: 1, Tags: "dlg-2222"},
			{Hash: "c", Name: "stalled-seed", State: "stalledUP", Progress: 1, Tags: "dlg-3333"},
			{Hash: "d", Name: "broken", State: "error", Tags: "dlg-4444"},
			{Hash: "e", Name: "someone-elses", State: "downloading", Progress: 0.1, Tags: "sonarr"},
		})
	}))
	defer srv.Close()

	got, err := NewQBittorrent(srv.URL, "u", "p").Adopt(t.Context())
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if len(got) != 1 || got[0].ClientJobID != "dlg-1111" {
		t.Fatalf("adopted %+v, want only the in-flight dlg-1111", got)
	}
}

// Ownership is the category; without one we cannot tell our downloads from
// anyone else's, so we must adopt nothing rather than hijack the queue.
func TestNZBGetAdoptRequiresCategory(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{"result":[{"NZBID":1,"NZBName":"x","Category":"other"}]}`))
	}))
	defer srv.Close()

	got, err := NewNZBGet(srv.URL, "u", "p", "").Adopt(t.Context())
	if err != nil || len(got) != 0 {
		t.Fatalf("adopt with no category = (%v, %v), want (nil, nil)", got, err)
	}
	if called {
		t.Error("should not even query NZBGet without a category")
	}
}

func TestNZBGetAdoptFiltersByCategory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":[
		  {"NZBID":1,"NZBName":"ours","Category":"acquire"},
		  {"NZBID":2,"NZBName":"theirs","Category":"sonarr"}]}`))
	}))
	defer srv.Close()

	got, err := NewNZBGet(srv.URL, "u", "p", "acquire").Adopt(t.Context())
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if len(got) != 1 || got[0].ClientJobID != "1" || got[0].Title != "ours" {
		t.Fatalf("adopted %+v, want only our category", got)
	}
}

// A client outage must NOT look like "the job is gone" — the gateway gives up on
// jobs it believes were deleted, so swallowing an error would drop live work.
func TestNZBGetDescribeSurfacesOutage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "backend down", http.StatusBadGateway)
	}))
	defer srv.Close()

	v, err := NewNZBGet(srv.URL, "u", "p", "acquire").Describe(t.Context(), "7")
	if err == nil {
		t.Fatalf("outage reported as a view (%+v), want an error", v)
	}
	if v.NativeState == NotFoundState {
		t.Error("an outage must never be reported as not-found")
	}
}

// When both lists come back cleanly and neither holds the job, it really is gone.
func TestNZBGetDescribeReportsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":[]}`))
	}))
	defer srv.Close()

	v, err := NewNZBGet(srv.URL, "u", "p", "acquire").Describe(t.Context(), "7")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if v.NativeState != NotFoundState {
		t.Fatalf("native state = %q, want %q", v.NativeState, NotFoundState)
	}
}
