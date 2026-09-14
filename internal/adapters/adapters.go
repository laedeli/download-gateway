// Package adapters wraps each download client behind a single Adapter
// interface so acquire can hand a candidate to the gateway without
// knowing which protocol it'll travel over.
package adapters

import (
	"context"
	"errors"
	"strings"
)

// Job represents one queued download.
type Job struct {
	ID       string
	Source   string // torrent URL / NZB URL / magnet / hoster link
	SavePath string
	Title    string // human label for the UI / events
	// Category is the client-side category for this job; empty means the
	// client's own default.
	Category string
	// Payload is the NZB or .torrent file content. When set it is handed to the
	// client instead of Source, so the client never needs the indexer's link
	// (or the key embedded in it).
	Payload     []byte
	PayloadName string
}

// ErrPayloadUnsupported is returned by Add when the client type cannot take
// file content, only a link.
var ErrPayloadUnsupported = errors.New("this client type does not accept a payload")

// payloadFileName is the file name a client is given for uploaded content: the
// caller's name (else the title) reduced to its last path element with control
// characters removed, always ending in ext.
func payloadFileName(name, title, ext string) string {
	clean := func(s string) string {
		if i := strings.LastIndexAny(s, `/\`); i >= 0 {
			s = s[i+1:]
		}
		return strings.TrimSpace(strings.Map(func(r rune) rune {
			if isControl(r) {
				return -1
			}
			return r
		}, s))
	}
	out := clean(name)
	if out == "" {
		out = clean(title)
	}
	if out == "" {
		out = "download"
	}
	if !strings.HasSuffix(strings.ToLower(out), ext) {
		out += ext
	}
	return out
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
	State Status
	// NativeState is the client's own state string, kept alongside the coarse
	// State fold so a console can tell "stalled" from "actively downloading"
	// and "repairing" from "fetching".
	NativeState string
	Title       string
	BytesDone   int64
	BytesTotal  int64    // 0 = unknown
	SpeedBps    int64    // 0 = idle/paused (not "unknown")
	EtaSec      int64    // -1 = unknown
	Files       []string // absolute paths, populated on Completed
	Message     string   // error/detail
	// Optional peer/health detail; -1 means the client did not report it.
	Seeders  int32
	Leechers int32
	Health   int32 // usenet article health, 0-1000; -1 when N/A
}

// NotFoundState is the NativeState an adapter reports when the client answered
// successfully but has never heard of this job. The gateway uses it to give up
// on jobs that were deleted out from under it — so an adapter must NOT return it
// for a transport error, or a client outage would look like mass deletion.
const NotFoundState = "not-found"

// NewJobView returns a JobView with the "not reported" sentinels set, so an
// adapter only fills in what its client actually gives it.
func NewJobView() JobView {
	return JobView{EtaSec: -1, Seeders: -1, Leechers: -1, Health: -1}
}

// ClientStatus is the client-wide (not per-job) health snapshot used by the
// console: is the client reachable, how fast is it going overall, and anything
// client-specific worth surfacing (free disk, usenet server connections).
type ClientStatus struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Name      string            `json:"name"`
	Reachable bool              `json:"reachable"`
	Error     string            `json:"error,omitempty"`
	DownBps   int64             `json:"down_bps"`
	UpBps     int64             `json:"up_bps"`
	Paused    bool              `json:"paused"`
	FreeDisk  *int64            `json:"free_disk_bytes,omitempty"`
	Detail    map[string]string `json:"detail,omitempty"`
}

// Adapter is the contract every download backend implements.
type Adapter interface {
	Name() string
	Add(ctx context.Context, j Job) (clientJobID string, err error)
	Status(ctx context.Context, clientJobID string) (Status, error)
	Describe(ctx context.Context, clientJobID string) (JobView, error)
	Remove(ctx context.Context, clientJobID string) error
}

// Controller is implemented by adapters that can pause/resume a job. Optional:
// the gateway feature-detects it, so an adapter without these still works.
type Controller interface {
	Pause(ctx context.Context, clientJobID string) error
	Resume(ctx context.Context, clientJobID string) error
}

// Reporter is implemented by adapters that can report client-wide status.
type Reporter interface {
	ClientStatus(ctx context.Context) ClientStatus
}

// Rehydrator is implemented by adapters that can re-discover the jobs they own
// after a gateway restart (the tracked-job map is in-memory only, so without
// this an in-flight download is orphaned: no progress, no completed, no failed).
type Rehydrator interface {
	Adopt(ctx context.Context) ([]AdoptedJob, error)
}

// AdoptedJob is a job re-discovered from the client at boot.
type AdoptedJob struct {
	ClientJobID string
	Title       string
}

// Prober is implemented by adapters that can check they reach their client and
// that its credentials are accepted, without touching any job. It backs the
// config test endpoint.
type Prober interface {
	Probe(ctx context.Context) (version string, err error)
}
