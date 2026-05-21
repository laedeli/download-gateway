package adapters

import (
	"context"
	"log/slog"
)

// JDownloader talks to a JDownloader 2 instance over its MyJDownloader
// remote API (or a vendored direct plugin call once oDownloader's
// embedded-headless pattern is generalised).
type JDownloader struct {
	BaseURL  string
	Username string
	Password string
}

func (j *JDownloader) Name() string { return "jdownloader" }

func (j *JDownloader) Add(ctx context.Context, job Job) (string, error) {
	slog.Debug("jdownloader Add stub", "id", job.ID)
	return "stub-" + job.ID, nil
}

func (j *JDownloader) Status(ctx context.Context, id string) (Status, error) {
	return StatusQueued, nil
}

func (j *JDownloader) Remove(ctx context.Context, id string) error {
	slog.Debug("jdownloader Remove stub", "id", id)
	return nil
}
