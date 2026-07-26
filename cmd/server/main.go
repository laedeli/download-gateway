package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitlab.nalet.cloud/stube/download-gateway/internal/adapters"
	"gitlab.nalet.cloud/stube/download-gateway/internal/auth"
	"gitlab.nalet.cloud/stube/download-gateway/internal/events"
)

// tracked is one in-flight job the poll loop watches until it reaches a
// terminal state, at which point it is removed.
type tracked struct {
	adapter      string
	clientJobID  string
	title        string
	wantedItemID string
	last         adapters.Status
	view         adapters.JobView // newest snapshot, for GET /api/v1/downloads
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

type gateway struct {
	reg  *adapters.Registry
	pub  *events.Publisher
	poll time.Duration

	mu   sync.Mutex
	jobs map[string]*tracked // key: adapter + ":" + clientJobID
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := envOr("DOWNLOAD_GATEWAY_ADDR", ":8080")

	// Wire only the adapters this deployment is configured for (an adapter with
	// no endpoint is omitted, not stubbed). oDownloader has a live prod instance;
	// qBittorrent is the neutral torrent client for the laedeli acquisition addon.
	var configured []adapters.Adapter
	if base := os.Getenv("ODOWNLOADER_BASE_URL"); base != "" {
		configured = append(configured, adapters.NewODownloader(base, os.Getenv("ODOWNLOADER_TOKEN")))
		slog.Info("odownloader adapter configured", "base_url", base)
	}
	if base := os.Getenv("QBITTORRENT_BASE_URL"); base != "" {
		configured = append(configured, adapters.NewQBittorrent(base,
			os.Getenv("QBITTORRENT_USER"), os.Getenv("QBITTORRENT_PASS")))
		slog.Info("qbittorrent adapter configured", "base_url", base)
	}
	if base := os.Getenv("NZBGET_URL"); base != "" {
		configured = append(configured, adapters.NewNZBGet(base,
			os.Getenv("NZBGET_USER"), os.Getenv("NZBGET_PASS"), os.Getenv("NZBGET_CATEGORY")))
		slog.Info("nzbget adapter configured", "base_url", base)
	}
	reg := adapters.NewRegistry(configured...)

	pub, err := events.NewPublisher(events.ConfigFromEnv())
	if err != nil {
		slog.Error("kafka publisher init failed", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	gw := &gateway{
		reg:  reg,
		pub:  pub,
		poll: envDur("POLL_INTERVAL", 5*time.Second),
		jobs: map[string]*tracked{},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Re-attach to downloads that were already running before this process
	// started, then begin polling.
	gw.adoptExisting(ctx)
	go gw.pollLoop(ctx)

	verifier := auth.NewVerifier(os.Getenv("OIDC_ISSUER"))

	r := chi.NewRouter()
	// Unauthenticated ops endpoints.
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Handle("/metrics", promhttp.Handler())
	// /api/* requires a valid bearer when OIDC_ISSUER is set (else pass-through).
	r.Group(func(r chi.Router) {
		r.Use(verifier.Middleware)
		r.Get("/api/v1/clients", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, reg.Names())
		})
		r.Get("/api/v1/clients/status", gw.handleClientStatus)
		r.Post("/api/v1/downloads", gw.handleAdd)
		r.Delete("/api/v1/downloads/{adapter}/{id}", gw.handleRemove)
		r.Get("/api/v1/downloads", gw.handleList)
		r.Post("/api/v1/downloads/{adapter}/{id}/pause", gw.handleControl("pause"))
		r.Post("/api/v1/downloads/{adapter}/{id}/resume", gw.handleControl("resume"))
	})

	srv := &http.Server{Addr: addr, Handler: r, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("download-gateway listening", "addr", addr, "adapters", reg.Names())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type addRequest struct {
	Adapter      string `json:"adapter"`
	Source       string `json:"source"`
	Title        string `json:"title"`
	SavePath     string `json:"save_path"`
	WantedItemID string `json:"wanted_item_id"`
}

func (g *gateway) handleAdd(w http.ResponseWriter, r *http.Request) {
	var req addRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Adapter == "" || req.Source == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "adapter and source are required"})
		return
	}
	ad := g.reg.Get(req.Adapter)
	if ad == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such adapter: " + req.Adapter})
		return
	}
	clientJobID, err := ad.Add(r.Context(), adapters.Job{
		ID: req.WantedItemID, Source: req.Source, SavePath: req.SavePath, Title: req.Title,
	})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	g.track(&tracked{
		adapter: req.Adapter, clientJobID: clientJobID,
		title: req.Title, wantedItemID: req.WantedItemID, last: adapters.StatusQueued,
		view: adapters.NewJobView(),
	})
	_ = g.pub.EmitStarted(r.Context(), events.Started{
		ClientID: clientJobID, Adapter: req.Adapter, WantedItemID: req.WantedItemID, Title: req.Title,
	})
	writeJSON(w, http.StatusAccepted, map[string]string{"adapter": req.Adapter, "client_job_id": clientJobID})
}

func (g *gateway) handleRemove(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "adapter")
	id := chi.URLParam(r, "id")
	ad := g.reg.Get(name)
	if ad == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such adapter"})
		return
	}
	if err := ad.Remove(r.Context(), id); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
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
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such adapter"})
			return
		}
		ctrl, ok := ad.(adapters.Controller)
		if !ok {
			writeJSON(w, http.StatusNotImplemented,
				map[string]string{"error": name + " cannot " + action})
			return
		}
		var err error
		if action == "pause" {
			err = ctrl.Pause(r.Context(), id)
		} else {
			err = ctrl.Resume(r.Context(), id)
		}
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleClientStatus reports per-client health + aggregate speed, so a console
// can show "qBittorrent 4.2 MB/s · nzbget 18 MB/s, 2/2 news servers".
func (g *gateway) handleClientStatus(w http.ResponseWriter, r *http.Request) {
	names := g.reg.Names()
	out := make([]adapters.ClientStatus, 0, len(names))
	for _, name := range names {
		rep, ok := g.reg.Get(name).(adapters.Reporter)
		if !ok {
			// Don't invent health we never measured.
			out = append(out, adapters.ClientStatus{
				Name: name, Error: "status not supported by this adapter",
			})
			continue
		}
		cctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		out = append(out, rep.ClientStatus(cctx))
		cancel()
	}
	writeJSON(w, http.StatusOK, out)
}

func (g *gateway) track(t *tracked) {
	g.mu.Lock()
	g.jobs[t.adapter+":"+t.clientJobID] = t
	g.mu.Unlock()
}

func (g *gateway) untrack(adapter, id string) {
	g.mu.Lock()
	delete(g.jobs, adapter+":"+id)
	g.mu.Unlock()
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
// cannot delay the others' updates.
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
}

func (g *gateway) pollOne(ctx context.Context, job *tracked) {
	ad := g.reg.Get(job.adapter)
	if ad == nil {
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
			ClientID: job.clientJobID, Adapter: job.adapter, WantedItemID: job.wantedItemID,
			Files: v.Files, SizeBytes: v.BytesTotal,
		})
		g.untrack(job.adapter, job.clientJobID)
	case adapters.StatusFailed:
		_ = g.pub.EmitFailed(ctx, events.Failed{
			ClientID: job.clientJobID, Adapter: job.adapter, WantedItemID: job.wantedItemID,
			Title: titleOf(job, v), Error: v.Message,
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
					ClientID: job.clientJobID, Adapter: job.adapter, WantedItemID: job.wantedItemID,
					Title: job.title, Error: "job is no longer known to " + job.adapter,
				})
				g.untrack(job.adapter, job.clientJobID)
				return
			}
		} else {
			g.clearMissing(job)
		}
		_ = g.pub.EmitProgress(ctx, events.Progress{
			ClientID: job.clientJobID, Adapter: job.adapter,
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

// observe records the newest state + snapshot for the list endpoint.
func (g *gateway) observe(job *tracked, v adapters.JobView) {
	g.mu.Lock()
	if cur := g.jobs[job.adapter+":"+job.clientJobID]; cur != nil {
		cur.last = v.State
		cur.view = v
	}
	g.mu.Unlock()
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
	for _, name := range g.reg.Names() {
		ad, ok := g.reg.Get(name).(adapters.Rehydrator)
		if !ok {
			continue
		}
		actx, cancel := context.WithTimeout(ctx, 15*time.Second)
		jobs, err := ad.Adopt(actx)
		cancel()
		if err != nil {
			slog.Warn("adopt failed", "adapter", name, "err", err)
			continue
		}
		for _, j := range jobs {
			// wantedItemID is unknown after a restart; acquire re-associates by
			// (adapter, client_job_id) from its own grabs table.
			g.track(&tracked{
				adapter: name, clientJobID: j.ClientJobID,
				title: j.Title, last: adapters.StatusQueued,
				view: adapters.NewJobView(),
			})
		}
		if len(jobs) > 0 {
			slog.Info("adopted in-flight jobs after restart", "adapter", name, "count", len(jobs))
		}
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
