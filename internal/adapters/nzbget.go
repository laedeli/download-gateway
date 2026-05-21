package adapters

import (
	"context"
	"log/slog"
)

// NZBGet talks to NZBGet over its JSON-RPC API.
type NZBGet struct {
	BaseURL  string
	Username string
	Password string
}

func (n *NZBGet) Name() string { return "nzbget" }

func (n *NZBGet) Add(ctx context.Context, j Job) (string, error) {
	slog.Debug("nzbget Add stub", "id", j.ID)
	return "stub-" + j.ID, nil
}

func (n *NZBGet) Status(ctx context.Context, id string) (Status, error) {
	return StatusQueued, nil
}

func (n *NZBGet) Remove(ctx context.Context, id string) error {
	slog.Debug("nzbget Remove stub", "id", id)
	return nil
}
