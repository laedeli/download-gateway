package adapters

import (
	"context"
	"log/slog"
)

// QBittorrent talks to a qBittorrent instance over its Web API.
type QBittorrent struct {
	BaseURL  string
	Username string
	Password string
}

func (q *QBittorrent) Name() string { return "qbittorrent" }

func (q *QBittorrent) Add(ctx context.Context, j Job) (string, error) {
	slog.Debug("qbittorrent Add stub", "id", j.ID)
	return "stub-" + j.ID, nil
}

func (q *QBittorrent) Status(ctx context.Context, id string) (Status, error) {
	return StatusQueued, nil
}

func (q *QBittorrent) Describe(ctx context.Context, id string) (JobView, error) {
	return JobView{State: StatusQueued, EtaSec: -1}, nil
}

func (q *QBittorrent) Remove(ctx context.Context, id string) error {
	slog.Debug("qbittorrent Remove stub", "id", id)
	return nil
}
