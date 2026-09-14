package adapters

import (
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Errors returned by Registry.Lease.
var (
	ErrUnknownClient  = errors.New("no such client")
	ErrClientDraining = errors.New("client was removed and accepts no new downloads")
)

// Client is a read-only view of one registered client.
type Client struct {
	ID       string
	Type     string
	Source   string // SourceEnv or SourceAPI
	Category string // default category for new jobs
	SavePath string // default save path for new jobs
	Adapter  Adapter
}

// entry is one registered client. The spec is kept without its secret; the
// adapter holds the only copy, and hash says whether a push changed anything.
type entry struct {
	spec      ClientSpec
	secretSet bool
	source    string
	adapter   Adapter
	hash      [32]byte
	// draining marks an API client that a push removed while the gateway still
	// tracked jobs on it: it keeps being polled but refuses new adds.
	draining bool
}

func (e *entry) client() Client {
	return Client{
		ID: e.spec.ID, Type: e.spec.Type, Source: e.source,
		Category: e.spec.Category, SavePath: e.spec.SavePath, Adapter: e.adapter,
	}
}

// Registry holds the clients the gateway drives, keyed by client id. Env
// clients are registered once at boot; API clients are replaced as a set by
// Apply. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	clients  map[string]*entry
	leases   map[string]int // adds in flight per client id, see Lease
	revision int64
}

// NewRegistry returns a registry holding the given adapters as env clients,
// each with id and type equal to its Name(). An adapter that is not configured
// for this deployment is simply omitted rather than registered as a stub.
func NewRegistry(adapters ...Adapter) *Registry {
	r := &Registry{clients: map[string]*entry{}, leases: map[string]int{}}
	for _, ad := range adapters {
		if ad != nil {
			r.AddEnv(ClientSpec{}, ad)
		}
	}
	return r
}

// AddEnv registers an env-configured client. The adapter is used exactly as
// built; spec only describes it for listing (id and type default to the
// adapter's Name(), protocols to the type's). The secret is not retained.
func (r *Registry) AddEnv(spec ClientSpec, ad Adapter) {
	if ad == nil {
		return
	}
	if spec.ID == "" {
		spec.ID = ad.Name()
	}
	if spec.Type == "" {
		spec.Type = ad.Name()
	}
	if len(spec.Protocols) == 0 {
		if ti, ok := LookupType(spec.Type); ok {
			spec.Protocols = slices.Clone(ti.Protocols)
		}
	}
	e := &entry{secretSet: spec.Secret != "", source: SourceEnv, adapter: ad}
	spec.Secret = ""
	e.spec = spec
	r.mu.Lock()
	r.clients[spec.ID] = e
	r.mu.Unlock()
}

// Get returns the adapter for a client id, including a draining one (its jobs
// are still polled, removed and controlled), or nil.
func (r *Registry) Get(id string) Adapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.clients[id]; e != nil {
		return e.adapter
	}
	return nil
}

// Lookup returns the client for an id, including a draining one.
func (r *Registry) Lookup(id string) (Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.clients[id]; e != nil {
		return e.client(), true
	}
	return Client{}, false
}

// Lease returns a client that accepts new downloads, and a release func the
// caller must call once the added job is tracked (or the add failed). While a
// lease is held Reap will not drop the client, which closes the window between
// "the add reached the client" and "the job is tracked".
func (r *Registry) Lease(id string) (Client, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.clients[id]
	if e == nil {
		return Client{}, nil, ErrUnknownClient
	}
	if e.draining {
		return Client{}, nil, ErrClientDraining
	}
	r.leases[id]++
	var once sync.Once
	return e.client(), func() {
		once.Do(func() {
			r.mu.Lock()
			if r.leases[id]--; r.leases[id] <= 0 {
				delete(r.leases, id)
			}
			r.mu.Unlock()
		})
	}, nil
}

// Active returns the clients that accept new downloads, sorted by id.
func (r *Registry) Active() []Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Client, 0, len(r.clients))
	for _, e := range r.clients {
		if !e.draining {
			out = append(out, e.client())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Names returns the ids of the clients that accept new downloads, sorted.
func (r *Registry) Names() []string {
	active := r.Active()
	out := make([]string, 0, len(active))
	for _, c := range active {
		out = append(out, c.ID)
	}
	return out
}

// List describes the clients that accept new downloads, sorted by id, without
// secrets.
func (r *Registry) List() []ClientInfo {
	_, clients := r.Config()
	return clients
}

// Config returns the revision of the last push together with the client list,
// read under one lock so the two always match.
func (r *Registry) Config() (int64, []ClientInfo) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ClientInfo, 0, len(r.clients))
	for _, e := range r.clients {
		if e.draining {
			continue
		}
		info := e.spec.info(e.source)
		info.SecretSet = e.secretSet
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return r.revision, out
}

// Revision is the revision of the last push that applied without errors; 0 when
// no push has.
func (r *Registry) Revision() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revision
}

// ApplyError is a per-client failure of a push.
type ApplyError struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

// ApplyResult reports what a push changed.
type ApplyResult struct {
	// Revision is the revision in effect after the push: the pushed one when
	// every entry applied, otherwise the one before it.
	Revision int64
	// Applied lists every pushed client now in effect: added, replaced, or
	// unchanged.
	Applied []string
	Errors  []ApplyError
	// Started are the added or replaced clients, whose in-flight jobs the
	// gateway should adopt.
	Started []Client
	// Draining are the API clients this push removed that still have jobs.
	Draining []string
	// Removed are the API clients this push removed outright.
	Removed []string
}

// Apply atomically replaces every API client with the pushed set, diffing by
// id and spec hash: an unchanged client keeps its running adapter (and
// session), a changed one gets a new adapter, and a client missing from the
// push is removed — or, when inUse reports jobs still tracked on it, left
// draining until Reap drops it.
//
// Errors are per client. An invalid entry is not applied; if a client with
// that id is already running it is kept as it was, so a bad edit cannot take
// down a working client. An id owned by an env client is an error too.
//
// The valid entries take effect either way, but the pushed revision is only
// recorded when there were no errors. Otherwise the gateway keeps reporting the
// previous revision, so the owner can tell from a later GET alone — after its
// own restart, or when the push response was lost — that the configuration it
// holds is not fully in effect, and push again.
//
// inUse is called with the registry lock held; it must not call back into the
// registry.
func (r *Registry) Apply(revision int64, specs []ClientSpec, inUse func(id string) bool) ApplyResult {
	res := ApplyResult{Applied: []string{}, Errors: []ApplyError{}}

	counts := map[string]int{}
	for _, s := range specs {
		counts[strings.TrimSpace(s.ID)]++
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	keep := map[string]bool{} // API ids that stay registered after this push
	reported := map[string]bool{}
	fail := func(id, msg string) {
		if reported[id] {
			return
		}
		reported[id] = true
		res.Errors = append(res.Errors, ApplyError{ID: id, Message: msg})
		if e := r.clients[id]; e != nil && e.source == SourceAPI {
			keep[id] = true // keep what already runs under this id
		}
	}

	for _, raw := range specs {
		id := strings.TrimSpace(raw.ID)
		switch {
		case !ValidClientID(id):
			fail(id, "id must match ^[a-z0-9][a-z0-9-]{0,39}$")
			continue
		case counts[id] > 1:
			fail(id, "id appears more than once in the push")
			continue
		}
		if e := r.clients[id]; e != nil && e.source == SourceEnv {
			fail(id, "id is taken by a client configured in the environment")
			continue
		}
		spec, err := PrepareSpec(raw)
		if err != nil {
			fail(id, err.Error())
			continue
		}
		hash := specHash(spec)
		keep[id] = true
		res.Applied = append(res.Applied, id)

		if e := r.clients[id]; e != nil && e.hash == hash {
			e.draining = false // re-added unchanged: its jobs are still tracked
			continue
		}
		e := &entry{secretSet: spec.Secret != "", source: SourceAPI, adapter: buildClient(spec), hash: hash}
		spec.Secret = ""
		e.spec = spec
		r.clients[id] = e
		res.Started = append(res.Started, e.client())
	}

	for id, e := range r.clients {
		if e.source != SourceAPI || keep[id] {
			continue
		}
		if r.leases[id] > 0 || (inUse != nil && inUse(id)) {
			if !e.draining {
				e.draining = true
				res.Draining = append(res.Draining, id)
			}
			continue
		}
		delete(r.clients, id)
		res.Removed = append(res.Removed, id)
	}

	if len(res.Errors) == 0 {
		r.revision = revision
	}
	res.Revision = r.revision
	sort.Strings(res.Draining)
	sort.Strings(res.Removed)
	return res
}

// Reap drops draining clients that no longer have an add in flight or, per
// inUse, a tracked job, and returns their ids. inUse is called with the
// registry lock held; it must not call back into the registry.
func (r *Registry) Reap(inUse func(id string) bool) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var dropped []string
	for id, e := range r.clients {
		if !e.draining || r.leases[id] > 0 || (inUse != nil && inUse(id)) {
			continue
		}
		delete(r.clients, id)
		dropped = append(dropped, id)
	}
	sort.Strings(dropped)
	return dropped
}
