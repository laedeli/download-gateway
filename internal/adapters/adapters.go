// Package adapters wraps each download client behind a single Adapter
// interface so acquire can hand a candidate to the gateway without
// knowing which protocol it'll travel over.
package adapters

import "context"

// Job represents one queued download.
type Job struct {
	ID       string
	Source   string // torrent URL / NZB URL / magnet / debrid id
	SavePath string
}

// Status is the lifecycle phase reported by an adapter.
type Status string

const (
	StatusQueued     Status = "queued"
	StatusDownloading Status = "downloading"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Adapter is the contract every download backend implements.
type Adapter interface {
	Name() string
	Add(ctx context.Context, j Job) (clientJobID string, err error)
	Status(ctx context.Context, clientJobID string) (Status, error)
	Remove(ctx context.Context, clientJobID string) error
}

// Registry holds the configured adapters keyed by Adapter.Name().
type Registry struct {
	a map[string]Adapter
}

// NewRegistry returns a registry with all stub adapters wired up.
func NewRegistry() *Registry {
	r := &Registry{a: map[string]Adapter{}}
	for _, ad := range []Adapter{&QBittorrent{}, &NZBGet{}, &JDownloader{}, &ODownloader{}} {
		r.a[ad.Name()] = ad
	}
	return r
}

// Names returns the registered adapter names.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.a))
	for k := range r.a {
		out = append(out, k)
	}
	return out
}

// Get returns the adapter or nil.
func (r *Registry) Get(name string) Adapter {
	return r.a[name]
}
