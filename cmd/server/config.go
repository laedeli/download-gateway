package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/laedeli/download-gateway/internal/adapters"
)

// The client configuration API. The owning application keeps its download
// client configuration (secrets included) in its own store and pushes the full
// set here; the gateway holds it in memory only, so after a restart it reports
// revision 0 and the owner pushes again.

// maxConfigBody caps a config push or test body; hundreds of clients fit.
const maxConfigBody = 1 << 20

// configProbeTimeout bounds each call a client test makes.
const configProbeTimeout = 10 * time.Second

func (g *gateway) handleConfigTypes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, adapters.Types())
}

type configClientsResponse struct {
	Revision int64                 `json:"revision"`
	Clients  []adapters.ClientInfo `json:"clients"`
}

// handleConfigClients lists env and API clients. It never returns a secret,
// only whether one is set.
func (g *gateway) handleConfigClients(w http.ResponseWriter, _ *http.Request) {
	revision, clients := g.reg.Config()
	writeJSON(w, http.StatusOK, configClientsResponse{Revision: revision, Clients: clients})
}

// configPush is the PUT /api/v1/config/clients body. Both fields are pointers
// so a missing field is told apart from a zero one: a body without "clients"
// must not silently remove every client.
type configPush struct {
	Revision *int64                 `json:"revision"`
	Clients  *[]adapters.ClientSpec `json:"clients"`
}

type configApplyResponse struct {
	Revision int64                 `json:"revision"`
	Applied  []string              `json:"applied"`
	Errors   []adapters.ApplyError `json:"errors"`
}

// handleConfigApply atomically replaces every API client with the pushed set.
// Added and replaced clients are adopted in the background, so jobs already
// running in them are tracked without holding up the response. The response
// revision is the one in effect, which stays the previous one when an entry
// failed (see Registry.Apply).
func (g *gateway) handleConfigApply(w http.ResponseWriter, r *http.Request) {
	var req configPush
	if !decodeJSON(w, r, maxConfigBody, &req) {
		return
	}
	if req.Revision == nil || *req.Revision < 0 {
		writeError(w, http.StatusBadRequest, "revision is required and must not be negative")
		return
	}
	if req.Clients == nil {
		writeError(w, http.StatusBadRequest, "clients is required; send [] to remove every client")
		return
	}

	res := g.reg.Apply(*req.Revision, *req.Clients, g.hasJobs)
	started := make([]string, 0, len(res.Started))
	for _, c := range res.Started {
		started = append(started, c.ID)
		g.bg.Add(1)
		go func(id string) {
			defer g.bg.Done()
			g.adopt(g.ctx, id)
		}(c.ID)
	}
	failed := make([]string, 0, len(res.Errors))
	for _, e := range res.Errors {
		failed = append(failed, e.ID)
	}
	// Ids and revisions only: messages could echo configuration.
	slog.Info("client configuration applied", "pushed_revision", *req.Revision, "revision", res.Revision,
		"applied", res.Applied, "started", started, "draining", res.Draining, "removed", res.Removed, "failed", failed)

	writeJSON(w, http.StatusOK, configApplyResponse{Revision: res.Revision, Applied: res.Applied, Errors: res.Errors})
}

type configTestResponse struct {
	Reachable bool                  `json:"reachable"`
	Version   string                `json:"version"`
	Error     string                `json:"error"`
	Status    adapters.ClientStatus `json:"status"`
}

// handleConfigTest builds a client from the posted spec without registering it
// and checks that it reaches its endpoint with the given credentials. A spec
// that does not validate is answered the same way, with the reason in error,
// so a console can show every outcome in one place.
//
// The endpoint can be any address, so what it answered never comes back
// verbatim: errors carry the HTTP status but no body text (PublicError).
func (g *gateway) handleConfigTest(w http.ResponseWriter, r *http.Request) {
	var spec adapters.ClientSpec
	if !decodeJSON(w, r, maxConfigBody, &spec) {
		return
	}
	out := configTestResponse{Status: adapters.ClientStatus{
		ID: strings.TrimSpace(spec.ID), Type: strings.TrimSpace(spec.Type), Name: strings.TrimSpace(spec.Type),
	}}
	fail := func(msg string) {
		out.Error, out.Status.Error = msg, msg
		writeJSON(w, http.StatusOK, out)
	}
	// The id is optional here: a client can be tested before it is named.
	if out.Status.ID != "" && !adapters.ValidClientID(out.Status.ID) {
		fail("id must match ^[a-z0-9][a-z0-9-]{0,39}$")
		return
	}
	ad, _, err := adapters.NewClient(spec)
	if err != nil {
		fail(err.Error())
		return
	}
	if p, ok := ad.(adapters.Prober); ok {
		ctx, cancel := context.WithTimeout(r.Context(), configProbeTimeout)
		out.Version, err = p.Probe(ctx)
		cancel()
		if err != nil {
			fail(adapters.PublicError(err))
			return
		}
		out.Reachable = true
	}
	if rep, ok := ad.(adapters.Reporter); ok {
		ctx, cancel := context.WithTimeout(r.Context(), configProbeTimeout)
		status := rep.ClientStatus(ctx)
		cancel()
		status.ID, status.Type = out.Status.ID, out.Status.Type
		out.Status = status
	} else {
		out.Status.Name = ad.Name()
		out.Status.Reachable = out.Reachable
	}
	writeJSON(w, http.StatusOK, out)
}
