package main

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/laedeli/download-gateway/internal/adapters"
	"github.com/laedeli/download-gateway/internal/auth"
	"github.com/laedeli/download-gateway/internal/auth/authtest"
)

// fakeAdapter is a client that numbers its jobs from 1 and describes them from
// views; a job it has no view of is one it does not know. err, when set, is a
// client that cannot be asked.
type fakeAdapter struct {
	mu    sync.Mutex
	name  string
	next  int
	views map[string]adapters.JobView
	err   error
}

func newFakeAdapter(name string) *fakeAdapter {
	return &fakeAdapter{name: name, views: map[string]adapters.JobView{}}
}

func (f *fakeAdapter) Name() string { return f.name }

func (f *fakeAdapter) Add(context.Context, adapters.Job) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	return strconv.Itoa(f.next), nil
}

func (f *fakeAdapter) Status(ctx context.Context, id string) (adapters.Status, error) {
	v, err := f.Describe(ctx, id)
	return v.State, err
}

func (f *fakeAdapter) Describe(_ context.Context, id string) (adapters.JobView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return adapters.JobView{}, f.err
	}
	if v, ok := f.views[id]; ok {
		return v, nil
	}
	v := adapters.NewJobView()
	v.State, v.NativeState = adapters.StatusQueued, adapters.NotFoundState
	return v, nil
}

func (f *fakeAdapter) Remove(context.Context, string) error { return nil }

func (f *fakeAdapter) set(id string, v adapters.JobView) {
	f.mu.Lock()
	f.views[id] = v
	f.mu.Unlock()
}

func (f *fakeAdapter) fail(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

// A job is described as its client describes it now: while it runs with the
// wanted item id it was added with, and once it finished — no longer tracked —
// with the files the client reports and no wanted item id.
func TestDescribeAJob(t *testing.T) {
	fake := newFakeAdapter("usenet")
	tg := newTestGateway(t, adapters.NewRegistry(fake), openVerifier())
	rec := tg.call(t, http.MethodPost, "/api/v1/downloads", map[string]any{
		"client": "usenet", "source": "https://indexer.example/get/1", "title": "Show Name S01E03", "wanted_item_id": "tg_1",
	}, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("add = %d %s", rec.Code, rec.Body)
	}
	id := decode[map[string]string](t, rec)["client_job_id"]

	running := adapters.NewJobView()
	running.State, running.NativeState, running.Title = adapters.StatusDownloading, "DOWNLOADING", "Show.Name.S01E03.1080p.WEB.h264-GRP"
	running.BytesDone, running.BytesTotal, running.SpeedBps, running.EtaSec, running.Health = 25, 100, 5, 15, 1000
	fake.set(id, running)
	got := decode[describeDTO](t, tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/"+id, nil, ""))
	if got.Adapter != "usenet" || got.ClientJobID != id || got.WantedItemID != "tg_1" || got.State != "downloading" ||
		got.NativeState != "DOWNLOADING" || got.ProgressPct != 25 || got.Downloaded != 25 || got.SizeBytes == nil ||
		*got.SizeBytes != 100 || got.EtaSec == nil || *got.EtaSec != 15 || got.Health == nil || *got.Health != 1000 ||
		got.Seeders != nil || len(got.Files) != 0 || got.Title != "Show.Name.S01E03.1080p.WEB.h264-GRP" {
		t.Fatalf("a running job = %+v", got)
	}

	done := adapters.NewJobView()
	done.State, done.NativeState, done.Title = adapters.StatusCompleted, "SUCCESS/ALL", "Show.Name.S01E03.1080p.WEB.h264-GRP"
	done.BytesDone, done.BytesTotal = 100, 100
	done.Files = []string{"/downloads/complete/acquire/Show.Name.S01E03.1080p.WEB.h264-GRP"}
	fake.set(id, done)
	tg.pollAll(t.Context())
	if len(tg.pub.completed) != 1 || len(tg.snapshot()) != 0 {
		t.Fatalf("the finished job is still tracked: completed %+v, tracked %d", tg.pub.completed, len(tg.snapshot()))
	}
	got = decode[describeDTO](t, tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/"+id, nil, ""))
	if got.State != "completed" || got.WantedItemID != "" || got.ProgressPct != 100 ||
		!reflect.DeepEqual(got.Files, done.Files) || got.Title != done.Title {
		t.Fatalf("a finished job = %+v", got)
	}

	failed := adapters.NewJobView()
	failed.State, failed.NativeState, failed.Message = adapters.StatusFailed, "FAILURE/HEALTH", "nzbget: FAILURE/HEALTH"
	fake.set("7", failed)
	if got := decode[describeDTO](t, tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/7", nil, "")); got.State != "failed" ||
		got.Message != "nzbget: FAILURE/HEALTH" || len(got.Files) != 0 {
		t.Fatalf("a failed job = %+v", got)
	}
}

// An unknown client and a job its client does not know are both 404, told
// apart by the job's native state; a client that cannot be asked is 502.
func TestDescribeAnswersWhatItCannotDescribe(t *testing.T) {
	fake := newFakeAdapter("usenet")
	tg := newTestGateway(t, adapters.NewRegistry(fake), openVerifier())

	rec := tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/3", nil, "")
	body := decode[map[string]string](t, rec)
	if rec.Code != http.StatusNotFound || body["native_state"] != adapters.NotFoundState || body["client_job_id"] != "3" ||
		body["adapter"] != "usenet" {
		t.Fatalf("a job the client does not know = %d %v", rec.Code, body)
	}
	rec = tg.call(t, http.MethodGet, "/api/v1/downloads/nope/3", nil, "")
	if body := decode[map[string]string](t, rec); rec.Code != http.StatusNotFound || body["native_state"] != "" {
		t.Fatalf("an unknown client = %d %v", rec.Code, body)
	}

	fake.fail(errors.New("nzbget listgroups: dial tcp: lookup nzbget.example: no such host"))
	if rec := tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/3", nil, ""); rec.Code != http.StatusBadGateway {
		t.Fatalf("a client that cannot be asked = %d %s", rec.Code, rec.Body)
	}
	// A client's own answer is not passed on, only its status.
	fake.fail(&adapters.StatusError{Op: "nzbget listgroups", Code: http.StatusInternalServerError, Detail: "<html>stack trace</html>"})
	rec = tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/3", nil, "")
	if body := decode[map[string]string](t, rec); rec.Code != http.StatusBadGateway || body["error"] != "nzbget listgroups: HTTP 500" {
		t.Fatalf("a client answering an error = %d %v", rec.Code, body)
	}
}

// Describing a job needs a token issued to an allowed client, like the rest of
// the downloads API.
func TestDescribeRequiresAllowedClient(t *testing.T) {
	iss := authtest.NewIssuer(t)
	fake := newFakeAdapter("usenet")
	done := adapters.NewJobView()
	done.State = adapters.StatusCompleted
	fake.set("1", done)
	tg := newTestGateway(t, adapters.NewRegistry(fake), auth.NewVerifier(iss.URL, "acquire"))

	if rec := tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/1", nil, ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("without a token = %d, want 401", rec.Code)
	}
	if rec := tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/1", nil, iss.Token(t, map[string]any{"azp": "some-other-app"})); rec.Code != http.StatusForbidden {
		t.Errorf("from another client = %d, want 403", rec.Code)
	}
	if rec := tg.call(t, http.MethodGet, "/api/v1/downloads/usenet/1", nil, iss.Token(t, map[string]any{"azp": "acquire"})); rec.Code != http.StatusOK {
		t.Errorf("from the allowed client = %d %s", rec.Code, rec.Body)
	}
}
