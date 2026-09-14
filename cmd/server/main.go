package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/laedeli/download-gateway/internal/adapters"
	"github.com/laedeli/download-gateway/internal/auth"
	"github.com/laedeli/download-gateway/internal/events"
)

// tracked is one in-flight job the poll loop watches until it reaches a
// terminal state, at which point it is removed.
type tracked struct {
	adapter      string // client id
	clientType   string // adapter type of that client when the job was tracked
	clientJobID  string
	title        string
	wantedItemID string
	last         adapters.Status
	view         adapters.JobView // newest snapshot, for GET /api/v1/downloads
	// Previous byte sample, used to derive a transfer rate for clients that do
	// not report one (NZBGet's listgroups has no per-group DownloadRate).
	lastBytes int64
	lastAt    time.Time
	// missingSince is when the client stopped reporting this job at all. A job
	// deleted out from under us folds to "queued" forever otherwise, so we give
	// up on it after lostAfter.
	missingSince time.Time
}

// lostAfter is how long a job may stay invisible to its client before the
// gateway declares it lost and stops tracking it.
const lostAfter = 10 * time.Minute

// pollConcurrency bounds the per-tick fan-out. The loop used to poll serially,
// so one slow client stalled every other job's updates.
const pollConcurrency = 8

// maxPayload is the largest NZB or .torrent content an add is sized for: the
// largest release file acquire fetches itself (an NZB, 50 MiB).
const maxPayload = 50 << 20

// maxAddBody caps POST /api/v1/downloads. A payload travels base64-encoded
// inside the JSON body, a third larger than the content; 4 MiB is ample room for
// the other fields. Raising acquire's NZB cap means raising maxPayload with it.
const maxAddBody = (maxPayload+2)/3*4 + 4<<20

// Large adds hold their payload in memory while it is handed to the client, so
// the declared bodies of the adds in flight share a budget: two adds of the
// largest size fit, or many ordinary NZBs. A body of at most smallAddBody bytes
// cannot carry a payload worth bounding and is always admitted.
const (
	addBodyBudget = 2 * maxAddBody
	smallAddBody  = 1 << 20
)

// publisher is the event sink the gateway emits to (*events.Publisher in
// production).
type publisher interface {
	EmitStarted(context.Context, events.Started) error
	EmitProgress(context.Context, events.Progress) error
	EmitCompleted(context.Context, events.Completed) error
	EmitFailed(context.Context, events.Failed) error
	Ready() bool
}

// gateway tracks jobs across the registered clients.
//
// Lock order: the registry may hold its own lock while it calls hasJobs (which
// takes g.mu) during Apply and Reap, so never call into the registry while
// holding g.mu.
type gateway struct {
	ctx  context.Context // process lifetime; adoption after a config push runs under it
	reg  *adapters.Registry
	pub  publisher
	poll time.Duration

	mu   sync.Mutex
	jobs map[string]*tracked // key: adapter + ":" + clientJobID

	stopping atomic.Bool    // set on shutdown so /readyz stops routing traffic here
	bg       sync.WaitGroup // adoption started by config pushes

	bodyMu       sync.Mutex
	bodyInFlight int64 // declared bytes of the large adds in flight, see admitAdd
}

func newGateway(ctx context.Context, reg *adapters.Registry, pub publisher, poll time.Duration) *gateway {
	return &gateway{ctx: ctx, reg: reg, pub: pub, poll: poll, jobs: map[string]*tracked{}}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := envOr("DOWNLOAD_GATEWAY_ADDR", ":8080")

	reg := adapters.NewRegistry()
	registerEnvClients(reg)

	pub, err := events.NewPublisher(events.ConfigFromEnv())
	if err != nil {
		slog.Error("kafka publisher init failed", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	gw := newGateway(ctx, reg, pub, envDur("POLL_INTERVAL", 5*time.Second))
	// Re-attach to downloads that were already running before this process
	// started, then begin polling.
	gw.adoptExisting(ctx)
	go gw.pollLoop(ctx)

	verifier := auth.NewVerifier(os.Getenv("OIDC_ISSUER"), auth.ParseClientList(os.Getenv("ALLOWED_CLIENTS"))...)

	srv := &http.Server{Addr: addr, Handler: gw.routes(verifier), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("download-gateway listening", "addr", addr, "clients", reg.Names(),
			"config_api", verifier.Restricted())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	gw.stopping.Store(true)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func (g *gateway) routes(verifier *auth.Verifier) http.Handler {
	r := chi.NewRouter()
	// Unauthenticated ops endpoints.
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", g.handleReady)
	r.Handle("/metrics", promhttp.Handler())
	// /api/* requires a valid bearer when OIDC_ISSUER is set (else pass-through),
	// issued to one of ALLOWED_CLIENTS when that is set too.
	r.Group(func(r chi.Router) {
		r.Use(verifier.Middleware)
		r.Get("/api/v1/clients", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, g.reg.Names())
		})
		r.Get("/api/v1/clients/status", g.handleClientStatus)
		r.Post("/api/v1/downloads", g.handleAdd)
		r.Delete("/api/v1/downloads/{adapter}/{id}", g.handleRemove)
		r.Get("/api/v1/downloads", g.handleList)
		r.Post("/api/v1/downloads/{adapter}/{id}/pause", g.handleControl("pause"))
		r.Post("/api/v1/downloads/{adapter}/{id}/resume", g.handleControl("resume"))
		// Whoever can push client configuration can point the gateway at any
		// endpoint, so the config API only exists when callers are restricted to
		// named clients. Otherwise its routes are simply not there (404).
		if verifier.Restricted() {
			r.Get("/api/v1/config/types", g.handleConfigTypes)
			r.Get("/api/v1/config/clients", g.handleConfigClients)
			r.Put("/api/v1/config/clients", g.handleConfigApply)
			r.Post("/api/v1/config/clients/test", g.handleConfigTest)
		}
	})
	return r
}

// registerEnvClients wires the clients this deployment configures through the
// environment (a client with no endpoint is omitted, not stubbed). They keep
// their historical ids — the type name — and are built exactly as before; the
// spec only describes them for the config API, which cannot replace them.
func registerEnvClients(reg *adapters.Registry) {
	if base := os.Getenv("ODOWNLOADER_BASE_URL"); base != "" {
		token := os.Getenv("ODOWNLOADER_TOKEN")
		reg.AddEnv(adapters.ClientSpec{
			BaseURL: base, Auth: authMode(token != "", adapters.AuthToken), Secret: token,
		}, adapters.NewODownloader(base, token))
		slog.Info("odownloader client configured from the environment", "base_url", adapters.RedactURL(base))
	}
	if base := os.Getenv("QBITTORRENT_BASE_URL"); base != "" {
		user, pass := os.Getenv("QBITTORRENT_USER"), os.Getenv("QBITTORRENT_PASS")
		reg.AddEnv(adapters.ClientSpec{
			BaseURL: base, Auth: authMode(user != "", adapters.AuthBasic), Username: user, Secret: pass,
		}, adapters.NewQBittorrent(base, user, pass))
		slog.Info("qbittorrent client configured from the environment", "base_url", adapters.RedactURL(base))
	}
	if base := os.Getenv("NZBGET_URL"); base != "" {
		user, pass, category := os.Getenv("NZBGET_USER"), os.Getenv("NZBGET_PASS"), os.Getenv("NZBGET_CATEGORY")
		reg.AddEnv(adapters.ClientSpec{
			BaseURL: base, Auth: authMode(user != "", adapters.AuthBasic), Username: user, Secret: pass,
			Category: category,
		}, adapters.NewNZBGet(base, user, pass, category))
		slog.Info("nzbget client configured from the environment", "base_url", adapters.RedactURL(base))
	}
}

func authMode(credentials bool, mode string) string {
	if credentials {
		return mode
	}
	return adapters.AuthNone
}

// addRequest is the POST /api/v1/downloads body. client and adapter are
// aliases; either names a client id.
type addRequest struct {
	Adapter      string `json:"adapter"`
	Client       string `json:"client"`
	Source       string `json:"source"`
	Title        string `json:"title"`
	SavePath     string `json:"save_path"`
	WantedItemID string `json:"wanted_item_id"`
	// Payload is the NZB or .torrent content, base64-encoded on the wire as
	// payload_b64 (encoding/json decodes it straight into bytes, without an
	// intermediate string). It is handed to the client instead of source.
	Payload     []byte `json:"payload_b64"`
	PayloadName string `json:"payload_name"`
	Category    string `json:"category"`
}

func (g *gateway) handleAdd(w http.ResponseWriter, r *http.Request) {
	admitted, ok := g.admitAdd(r.ContentLength)
	if !ok {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusTooManyRequests, "too many large adds in progress; retry shortly")
		return
	}
	defer admitted()

	var req addRequest
	if !decodeJSON(w, r, maxAddBody, &req) {
		return
	}
	id := req.Client
	if id == "" {
		id = req.Adapter
	}
	switch {
	case req.Client != "" && req.Adapter != "" && req.Client != req.Adapter:
		writeError(w, http.StatusBadRequest, "client and adapter name different clients")
		return
	case id == "" || (req.Source == "" && len(req.Payload) == 0):
		writeError(w, http.StatusBadRequest, "client (or adapter) and source or payload_b64 are required")
		return
	}

	// The lease keeps a config push from dropping the client between the add
	// reaching it and the job being tracked; it is released after track.
	c, release, err := g.reg.Lease(id)
	switch {
	case errors.Is(err, adapters.ErrUnknownClient):
		writeError(w, http.StatusNotFound, "no such client: "+id)
		return
	case errors.Is(err, adapters.ErrClientDraining):
		writeError(w, http.StatusConflict, "client "+id+" was removed and accepts no new downloads")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer release()

	if ti, ok := adapters.LookupType(c.Type); ok && len(req.Payload) > 0 && !ti.AcceptsPayload {
		writeError(w, http.StatusBadRequest, c.Type+" does not accept a payload; send a source link")
		return
	}
	savePath, category := req.SavePath, req.Category
	if savePath == "" {
		savePath = c.SavePath
	}
	if category == "" {
		category = c.Category
	}
	clientJobID, err := c.Adapter.Add(r.Context(), adapters.Job{
		ID: req.WantedItemID, Source: req.Source, SavePath: savePath, Title: req.Title,
		Category: category, Payload: req.Payload, PayloadName: req.PayloadName,
	})
	if errors.Is(err, adapters.ErrPayloadUnsupported) {
		writeError(w, http.StatusBadRequest, c.Type+" does not accept a payload; send a source link")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	g.track(&tracked{
		adapter: id, clientType: c.Type, clientJobID: clientJobID,
		title: req.Title, wantedItemID: req.WantedItemID, last: adapters.StatusQueued,
		view: adapters.NewJobView(),
	})
	_ = g.pub.EmitStarted(r.Context(), events.Started{
		ClientID: clientJobID, Adapter: id, ClientType: c.Type, WantedItemID: req.WantedItemID, Title: req.Title,
	})
	writeJSON(w, http.StatusAccepted, map[string]string{"adapter": id, "client": id, "client_job_id": clientJobID})
}

func (g *gateway) handleRemove(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "adapter")
	id := chi.URLParam(r, "id")
	ad := g.reg.Get(name)
	if ad == nil {
		writeError(w, http.StatusNotFound, "no such client")
		return
	}
	if err := ad.Remove(r.Context(), id); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	g.untrack(name, id)
	w.WriteHeader(http.StatusNoContent)
}

// jobDTO is one in-flight job as reported by GET /api/v1/downloads. It carries
// the last polled snapshot so a console can render progress without asking each
// client again (and so acquire can reconcile after its own restart).
type jobDTO struct {
	Adapter      string  `json:"adapter"`
	ClientJobID  string  `json:"client_job_id"`
	WantedItemID string  `json:"wanted_item_id,omitempty"`
	Title        string  `json:"title"`
	State        string  `json:"state"`
	NativeState  string  `json:"native_state,omitempty"`
	ProgressPct  float64 `json:"progress_pct"`
	Downloaded   int64   `json:"downloaded_bytes"`
	SizeBytes    *int64  `json:"size_bytes"`
	SpeedBps     int64   `json:"speed_bps"`
	EtaSec       *int32  `json:"eta_sec"`
	Seeders      *int32  `json:"seeders,omitempty"`
	Leechers     *int32  `json:"leechers,omitempty"`
	Health       *int32  `json:"health,omitempty"`
}

func (g *gateway) handleList(w http.ResponseWriter, _ *http.Request) {
	g.mu.Lock()
	out := make([]jobDTO, 0, len(g.jobs))
	for _, t := range g.jobs {
		v := t.view
		out = append(out, jobDTO{
			Adapter: t.adapter, ClientJobID: t.clientJobID, WantedItemID: t.wantedItemID,
			Title: titleOf(t, v), State: string(t.last), NativeState: v.NativeState,
			ProgressPct: pct(v.BytesDone, v.BytesTotal), Downloaded: v.BytesDone,
			SizeBytes: optInt64(v.BytesTotal), SpeedBps: v.SpeedBps,
			EtaSec:  optInt32(v.EtaSec),
			Seeders: optCount(v.Seeders), Leechers: optCount(v.Leechers), Health: optCount(v.Health),
		})
	}
	g.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// handleControl pauses or resumes one job, for adapters that support it.
func (g *gateway) handleControl(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "adapter")
		id := chi.URLParam(r, "id")
		ad := g.reg.Get(name)
		if ad == nil {
			writeError(w, http.StatusNotFound, "no such client")
			return
		}
		ctrl, ok := ad.(adapters.Controller)
		if !ok {
			writeError(w, http.StatusNotImplemented, name+" cannot "+action)
			return
		}
		var err error
		if action == "pause" {
			err = ctrl.Pause(r.Context(), id)
		} else {
			err = ctrl.Resume(r.Context(), id)
		}
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleClientStatus reports per-client health + aggregate speed, so a console
// can show "qBittorrent 4.2 MB/s · nzbget 18 MB/s, 2/2 news servers".
func (g *gateway) handleClientStatus(w http.ResponseWriter, r *http.Request) {
	clients := g.reg.Active()
	out := make([]adapters.ClientStatus, 0, len(clients))
	for _, c := range clients {
		var cs adapters.ClientStatus
		if rep, ok := c.Adapter.(adapters.Reporter); ok {
			cctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			cs = rep.ClientStatus(cctx)
			cancel()
		} else {
			// Don't invent health we never measured.
			cs = adapters.ClientStatus{Name: c.Adapter.Name(), Error: "status not supported by this adapter"}
		}
		cs.ID, cs.Type = c.ID, c.Type
		out = append(out, cs)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleReady is the readiness probe: the process serves and the event
// publisher is up. Having no clients at all is still ready — clients arrive by
// config push after the gateway is reachable.
func (g *gateway) handleReady(w http.ResponseWriter, _ *http.Request) {
	if g.stopping.Load() || !g.pub.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (g *gateway) track(t *tracked) {
	g.mu.Lock()
	g.jobs[t.adapter+":"+t.clientJobID] = t
	g.mu.Unlock()
}

// trackIfAbsent tracks t unless the job is already tracked, and reports whether
// it did. Adoption uses it so re-adopting after a config push never resets a
// live job's state or forgets its wanted item.
func (g *gateway) trackIfAbsent(t *tracked) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := t.adapter + ":" + t.clientJobID
	if _, ok := g.jobs[key]; ok {
		return false
	}
	g.jobs[key] = t
	return true
}

func (g *gateway) untrack(adapter, id string) {
	g.mu.Lock()
	delete(g.jobs, adapter+":"+id)
	g.mu.Unlock()
}

// hasJobs reports whether any job is still tracked on a client. The registry
// calls it with its own lock held (see the lock order on gateway).
func (g *gateway) hasJobs(clientID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, t := range g.jobs {
		if t.adapter == clientID {
			return true
		}
	}
	return false
}

func (g *gateway) snapshot() []*tracked {
	g.mu.Lock()
	out := make([]*tracked, 0, len(g.jobs))
	for _, t := range g.jobs {
		out = append(out, t)
	}
	g.mu.Unlock()
	return out
}

// pollLoop drives each tracked job's status into stube.download.client.*
// events. oDownloader's own webhooks are unreliable (5s poll, fire-and-
// forget), so the gateway poll is the durable signal — Kafka, not the
// client webhook, is the source of truth (ADR-020).
func (g *gateway) pollLoop(ctx context.Context) {
	t := time.NewTicker(g.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.pollAll(ctx)
		}
	}
}

// pollAll polls every tracked job with bounded concurrency, so one slow client
// cannot delay the others' updates. Afterwards, removed clients whose last job
// just finished are dropped.
func (g *gateway) pollAll(ctx context.Context) {
	jobs := g.snapshot()
	sem := make(chan struct{}, pollConcurrency)
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j *tracked) {
			defer wg.Done()
			defer func() { <-sem }()
			g.pollOne(ctx, j)
		}(job)
	}
	wg.Wait()
	for _, id := range g.reg.Reap(g.hasJobs) {
		slog.Info("removed client drained; dropping it", "client", id)
	}
}

func (g *gateway) pollOne(ctx context.Context, job *tracked) {
	ad := g.reg.Get(job.adapter)
	if ad == nil {
		// Its client is gone entirely. Adds lease their client, so only a job
		// adopted in a race with the client's removal can land here.
		slog.Warn("client no longer configured; dropping job", "adapter", job.adapter, "id", job.clientJobID)
		g.untrack(job.adapter, job.clientJobID)
		return
	}
	pctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	v, err := ad.Describe(pctx, job.clientJobID)
	if err != nil {
		slog.Warn("describe failed", "adapter", job.adapter, "id", job.clientJobID, "err", err)
		return
	}
	switch v.State {
	case adapters.StatusCompleted:
		_ = g.pub.EmitCompleted(ctx, events.Completed{
			ClientID: job.clientJobID, Adapter: job.adapter, ClientType: job.clientType,
			WantedItemID: job.wantedItemID, Files: v.Files, SizeBytes: v.BytesTotal,
		})
		g.untrack(job.adapter, job.clientJobID)
	case adapters.StatusFailed:
		_ = g.pub.EmitFailed(ctx, events.Failed{
			ClientID: job.clientJobID, Adapter: job.adapter, ClientType: job.clientType,
			WantedItemID: job.wantedItemID, Title: titleOf(job, v), Error: v.Message,
		})
		g.untrack(job.adapter, job.clientJobID)
	default:
		// A job the client no longer knows about reports as "pending"/queued
		// forever. Give it a grace window, then stop tracking it.
		if v.NativeState == adapters.NotFoundState {
			if g.markMissing(job) {
				slog.Warn("job vanished from client; dropping",
					"adapter", job.adapter, "id", job.clientJobID)
				_ = g.pub.EmitFailed(ctx, events.Failed{
					ClientID: job.clientJobID, Adapter: job.adapter, ClientType: job.clientType,
					WantedItemID: job.wantedItemID,
					Title:        job.title, Error: "job is no longer known to " + job.adapter,
				})
				g.untrack(job.adapter, job.clientJobID)
				return
			}
		} else {
			g.clearMissing(job)
		}
		pb, pt := g.prevSample(job)
		fillDerived(&v, pb, pt)
		_ = g.pub.EmitProgress(ctx, events.Progress{
			ClientID: job.clientJobID, Adapter: job.adapter, ClientType: job.clientType,
			WantedItemID:    job.wantedItemID,
			Title:           titleOf(job, v),
			State:           string(v.State),
			NativeState:     v.NativeState,
			ProgressPct:     pct(v.BytesDone, v.BytesTotal),
			DownloadedBytes: v.BytesDone,
			SizeBytes:       optInt64(v.BytesTotal),
			SpeedBps:        v.SpeedBps,
			EtaSec:          optInt32(v.EtaSec),
			Seeders:         optCount(v.Seeders),
			Leechers:        optCount(v.Leechers),
			Health:          optCount(v.Health),
		})
		g.observe(job, v)
	}
}

// titleOf prefers the client's own title, falling back to what the caller
// supplied at add time (magnets have no name until metadata resolves).
func titleOf(job *tracked, v adapters.JobView) string {
	if v.Title != "" {
		return v.Title
	}
	return job.title
}

// observe records the newest state + snapshot for the list endpoint, and the
// byte sample the next tick derives a rate from.
func (g *gateway) observe(job *tracked, v adapters.JobView) {
	g.mu.Lock()
	if cur := g.jobs[job.adapter+":"+job.clientJobID]; cur != nil {
		cur.last = v.State
		cur.view = v
		cur.lastBytes = v.BytesDone
		cur.lastAt = time.Now()
	}
	g.mu.Unlock()
}

// prevSample returns the previous byte count + when it was taken.
func (g *gateway) prevSample(job *tracked) (int64, time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if cur := g.jobs[job.adapter+":"+job.clientJobID]; cur != nil {
		return cur.lastBytes, cur.lastAt
	}
	return 0, time.Time{}
}

// fillDerived computes the values a client did not report: a transfer rate from
// the byte delta since the previous poll, and an ETA from that rate. NZBGet's
// listgroups carries no per-group DownloadRate at all, so without this a usenet
// download shows no speed and no ETA.
func fillDerived(v *adapters.JobView, prevBytes int64, prevAt time.Time) {
	if v.SpeedBps <= 0 && !prevAt.IsZero() && v.BytesDone > prevBytes {
		if elapsed := time.Since(prevAt).Seconds(); elapsed >= 1 {
			v.SpeedBps = int64(float64(v.BytesDone-prevBytes) / elapsed)
		}
	}
	if v.EtaSec < 0 && v.SpeedBps > 0 && v.BytesTotal > v.BytesDone {
		v.EtaSec = (v.BytesTotal - v.BytesDone) / v.SpeedBps
	}
}

// markMissing starts (or checks) the grace window for a job the client can no
// longer see; it returns true once the job should be given up on.
func (g *gateway) markMissing(job *tracked) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	cur := g.jobs[job.adapter+":"+job.clientJobID]
	if cur == nil {
		return false
	}
	if cur.missingSince.IsZero() {
		cur.missingSince = time.Now()
		return false
	}
	return time.Since(cur.missingSince) > lostAfter
}

func (g *gateway) clearMissing(job *tracked) {
	g.mu.Lock()
	if cur := g.jobs[job.adapter+":"+job.clientJobID]; cur != nil {
		cur.missingSince = time.Time{}
	}
	g.mu.Unlock()
}

// adoptExisting re-attaches to jobs still running in the clients after a
// restart. The tracked map is in-memory only, so without this an in-flight
// download is orphaned: it keeps downloading but never emits progress, and its
// completion is never seen.
func (g *gateway) adoptExisting(ctx context.Context) {
	for _, id := range g.reg.Names() {
		g.adopt(ctx, id)
	}
}

// adopt re-attaches to one client's in-flight jobs. It runs at boot and after a
// config push adds or replaces the client. It holds a lease like an add does,
// so a push racing with it leaves the client draining instead of dropping it
// under the jobs being tracked.
func (g *gateway) adopt(ctx context.Context, id string) {
	c, release, err := g.reg.Lease(id)
	if err != nil {
		return // removed again before adoption ran
	}
	defer release()
	ad, ok := c.Adapter.(adapters.Rehydrator)
	if !ok {
		return
	}
	actx, cancel := context.WithTimeout(ctx, 15*time.Second)
	jobs, err := ad.Adopt(actx)
	cancel()
	if err != nil {
		slog.Warn("adopt failed", "adapter", id, "err", err)
		return
	}
	adopted := 0
	for _, j := range jobs {
		// wantedItemID is unknown after a restart; acquire re-associates by
		// (adapter, client_job_id) from its own grabs table.
		if g.trackIfAbsent(&tracked{
			adapter: id, clientType: c.Type, clientJobID: j.ClientJobID,
			title: j.Title, last: adapters.StatusQueued,
			view: adapters.NewJobView(),
		}) {
			adopted++
		}
	}
	if adopted > 0 {
		slog.Info("adopted in-flight jobs", "adapter", id, "count", adopted)
	}
}

func pct(done, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(done) / float64(total) * 100
}

func optInt64(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func optInt32(v int64) *int32 {
	if v < 0 {
		return nil
	}
	x := int32(v)
	return &x
}

// optCount maps an adapter's "not reported" sentinel (-1) to a nil pointer, so
// a missing seed count is absent from the event rather than a misleading 0.
func optCount(v int32) *int32 {
	if v < 0 {
		return nil
	}
	return &v
}

// admitAdd reserves room in the large-add budget for a body of n declared bytes
// (-1 when unknown, counted as the largest allowed). It returns false when the
// body does not fit next to the adds already in flight; otherwise the caller
// must call release once the add is done.
func (g *gateway) admitAdd(n int64) (release func(), ok bool) {
	if n >= 0 && n <= smallAddBody {
		return func() {}, true
	}
	if n < 0 || n > maxAddBody {
		n = maxAddBody // a longer body is refused by the cap anyway
	}
	g.bodyMu.Lock()
	defer g.bodyMu.Unlock()
	if g.bodyInFlight+n > addBodyBudget {
		return nil, false
	}
	g.bodyInFlight += n
	var once sync.Once
	return func() {
		once.Do(func() {
			g.bodyMu.Lock()
			g.bodyInFlight -= n
			g.bodyMu.Unlock()
		})
	}, true
}

// decodeJSON reads a JSON body capped at limit bytes. When the body is
// unusable it answers the request itself (400, or 413 past the cap) and
// returns false.
func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		var corrupt base64.CorruptInputError
		switch {
		case errors.As(err, &tooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d MiB", limit>>20))
		case errors.As(err, &corrupt):
			writeError(w, http.StatusBadRequest, "payload_b64 is not valid base64")
		default:
			writeError(w, http.StatusBadRequest, "invalid JSON body")
		}
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
