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
}

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
		r.Post("/api/v1/downloads", gw.handleAdd)
		r.Delete("/api/v1/downloads/{adapter}/{id}", gw.handleRemove)
		r.Get("/api/v1/downloads", gw.handleList)
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

func (g *gateway) handleList(w http.ResponseWriter, _ *http.Request) {
	g.mu.Lock()
	out := make([]map[string]string, 0, len(g.jobs))
	for _, t := range g.jobs {
		out = append(out, map[string]string{
			"adapter": t.adapter, "client_job_id": t.clientJobID,
			"title": t.title, "state": string(t.last),
		})
	}
	g.mu.Unlock()
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
			for _, job := range g.snapshot() {
				g.pollOne(ctx, job)
			}
		}
	}
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
			ClientID: job.clientJobID, Adapter: job.adapter, Error: v.Message,
		})
		g.untrack(job.adapter, job.clientJobID)
	default:
		_ = g.pub.EmitProgress(ctx, events.Progress{
			ClientID: job.clientJobID, Adapter: job.adapter, State: string(v.State),
			ProgressPct:     pct(v.BytesDone, v.BytesTotal),
			DownloadedBytes: v.BytesDone,
			SizeBytes:       optInt64(v.BytesTotal),
			SpeedBps:        optInt64(v.SpeedBps),
			EtaSec:          optInt32(v.EtaSec),
		})
		g.setLast(job, v.State)
	}
}

func (g *gateway) setLast(job *tracked, s adapters.Status) {
	g.mu.Lock()
	if cur := g.jobs[job.adapter+":"+job.clientJobID]; cur != nil {
		cur.last = s
	}
	g.mu.Unlock()
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
