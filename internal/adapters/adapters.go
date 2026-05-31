// Package adapters wraps each download client behind a single Adapter
// interface so acquire can hand a candidate to the gateway without
// knowing which protocol it'll travel over.
package adapters

import "context"

// Job represents one queued download.
type Job struct {
	ID       string
	Source   string // torrent URL / NZB URL / magnet / hoster link
	SavePath string
	Title    string // human label for the UI / events
}

// Status is the lifecycle phase reported by an adapter.
type Status string

const (
	StatusQueued      Status = "queued"
	StatusDownloading Status = "downloading"
	StatusCompleted   Status = "completed"
	StatusFailed      Status = "failed"
)

// JobView is the rich, per-poll snapshot an adapter exposes so the
// gateway can build stube.download.client.{progress,completed,failed}
// events without each consumer knowing the client API.
type JobView struct {
	State      Status
	Title      string
	BytesDone  int64
	BytesTotal int64    // 0 = unknown
	SpeedBps   int64    // 0 = idle/paused
	EtaSec     int64    // -1 = unknown
	Files      []string // absolute paths, populated on Completed
	Message    string   // error/detail
}

// Adapter is the contract every download backend implements.
type Adapter interface {
	Name() string
	Add(ctx context.Context, j Job) (clientJobID string, err error)
	Status(ctx context.Context, clientJobID string) (Status, error)
	Describe(ctx context.Context, clientJobID string) (JobView, error)
	Remove(ctx context.Context, clientJobID string) error
}

// Registry holds the configured adapters keyed by Adapter.Name().
type Registry struct {
	a map[string]Adapter
}

// NewRegistry returns a registry with the given adapters. Callers wire
// the concrete adapters (with their endpoints/tokens) and pass them in;
// an adapter that is not configured for this deployment is simply
// omitted rather than registered as a stub.
func NewRegistry(adapters ...Adapter) *Registry {
	r := &Registry{a: map[string]Adapter{}}
	for _, ad := range adapters {
		if ad != nil {
			r.a[ad.Name()] = ad
		}
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
func (r *Registry) Get(name string) Adapter { return r.a[name] }
