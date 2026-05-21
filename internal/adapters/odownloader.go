package adapters

import (
	"context"
	"log/slog"
)

// ODownloader talks to the in-cluster oDownloader service
// (cloud-nalet-odownloader namespace) over its static-token authenticated
// direct API.
type ODownloader struct {
	BaseURL string
	Token   string
}

func (o *ODownloader) Name() string { return "odownloader" }

func (o *ODownloader) Add(ctx context.Context, j Job) (string, error) {
	slog.Debug("odownloader Add stub", "id", j.ID)
	return "stub-" + j.ID, nil
}

func (o *ODownloader) Status(ctx context.Context, id string) (Status, error) {
	return StatusQueued, nil
}

func (o *ODownloader) Remove(ctx context.Context, id string) error {
	slog.Debug("odownloader Remove stub", "id", id)
	return nil
}
