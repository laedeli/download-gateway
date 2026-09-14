package adapters

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

func nzbSpec(id, secret string) ClientSpec {
	return ClientSpec{ID: id, Type: "nzbget", BaseURL: "http://" + id + ".example:6789", Auth: "basic",
		Username: "nzbget", Secret: secret, Category: "acquire"}
}

func qbSpec(id, secret string) ClientSpec {
	return ClientSpec{ID: id, Type: "qbittorrent", BaseURL: "http://" + id + ".example:8080", Auth: "basic",
		Username: "admin", Secret: secret, SavePath: "/downloads"}
}

func startedIDs(res ApplyResult) []string {
	var out []string
	for _, c := range res.Started {
		out = append(out, c.ID)
	}
	slices.Sort(out)
	return out
}

func notInUse(string) bool { return false }

// An unchanged client keeps its running adapter; a changed one is rebuilt; a
// new one is added; one missing from the push is removed.
func TestRegistryApplyDiff(t *testing.T) {
	r := NewRegistry()
	if r.Revision() != 0 {
		t.Fatalf("fresh revision = %d, want 0", r.Revision())
	}

	res := r.Apply(1, []ClientSpec{nzbSpec("usenet", "a"), qbSpec("torrents", "a")}, notInUse)
	if len(res.Errors) != 0 {
		t.Fatalf("errors = %+v", res.Errors)
	}
	if !slices.Equal(startedIDs(res), []string{"torrents", "usenet"}) || len(res.Applied) != 2 {
		t.Fatalf("first push = %+v", res)
	}
	usenet, torrents := r.Get("usenet"), r.Get("torrents")

	res = r.Apply(2, []ClientSpec{nzbSpec("usenet", "a"), qbSpec("torrents", "changed"), qbSpec("more", "x")}, notInUse)
	if !slices.Equal(startedIDs(res), []string{"more", "torrents"}) {
		t.Fatalf("started = %v, want only the changed and the new client", startedIDs(res))
	}
	if !slices.Equal(res.Applied, []string{"usenet", "torrents", "more"}) {
		t.Errorf("applied = %v, want every client in effect", res.Applied)
	}
	if r.Get("usenet") != usenet {
		t.Error("an unchanged client was rebuilt")
	}
	if r.Get("torrents") == torrents {
		t.Error("a client with a changed secret kept its old adapter")
	}
	if c, _ := r.Lookup("torrents"); c.Type != "qbittorrent" || c.SavePath != "/downloads" || c.Source != SourceAPI {
		t.Errorf("lookup = %+v", c)
	}

	res = r.Apply(3, []ClientSpec{nzbSpec("usenet", "a")}, notInUse)
	if !slices.Equal(res.Removed, []string{"more", "torrents"}) || len(res.Draining) != 0 {
		t.Fatalf("removed = %v, draining = %v", res.Removed, res.Draining)
	}
	if !slices.Equal(r.Names(), []string{"usenet"}) || r.Revision() != 3 {
		t.Errorf("names = %v, revision = %d", r.Names(), r.Revision())
	}
}

// Env clients are fixed: a push can neither replace nor remove them, and an
// API id colliding with one is a per-client error.
func TestRegistryEnvCollision(t *testing.T) {
	env := NewNZBGet("http://env.example", "u", "p", "c")
	r := NewRegistry(env)

	res := r.Apply(1, []ClientSpec{nzbSpec("nzbget", "x"), qbSpec("torrents", "x")}, notInUse)
	if len(res.Errors) != 1 || res.Errors[0].ID != "nzbget" || !strings.Contains(res.Errors[0].Message, "environment") {
		t.Fatalf("errors = %+v, want one env collision", res.Errors)
	}
	if !slices.Equal(res.Applied, []string{"torrents"}) {
		t.Errorf("applied = %v", res.Applied)
	}
	if r.Get("nzbget") != env {
		t.Fatal("the env client was replaced")
	}

	r.Apply(2, nil, notInUse)
	if r.Get("nzbget") != env {
		t.Fatal("an empty push removed the env client")
	}
	list := r.List()
	if len(list) != 1 || list[0].Source != SourceEnv || list[0].ID != "nzbget" || list[0].Type != "nzbget" {
		t.Fatalf("list = %+v", list)
	}
	if !slices.Equal(list[0].Protocols, []string{"usenet"}) {
		t.Errorf("env protocols = %v, want the type's", list[0].Protocols)
	}
}

// A removed client with tracked jobs keeps being reachable for polling but
// refuses new adds, until Reap sees its last job gone.
func TestRegistryDraining(t *testing.T) {
	r := NewRegistry()
	r.Apply(1, []ClientSpec{qbSpec("torrents", "x")}, notInUse)
	busy := true
	inUse := func(id string) bool { return id == "torrents" && busy }

	res := r.Apply(2, []ClientSpec{}, inUse)
	if !slices.Equal(res.Draining, []string{"torrents"}) || len(res.Removed) != 0 {
		t.Fatalf("draining = %v, removed = %v", res.Draining, res.Removed)
	}
	if r.Get("torrents") == nil {
		t.Fatal("a draining client must still be pollable")
	}
	if _, _, err := r.Lease("torrents"); !errors.Is(err, ErrClientDraining) {
		t.Fatalf("lease on a draining client = %v, want ErrClientDraining", err)
	}
	if len(r.Names()) != 0 || len(r.List()) != 0 || len(r.Active()) != 0 {
		t.Error("a draining client is still listed as configured")
	}

	if got := r.Reap(inUse); len(got) != 0 {
		t.Fatalf("reaped %v while jobs are still tracked", got)
	}
	busy = false
	if got := r.Reap(inUse); !slices.Equal(got, []string{"torrents"}) {
		t.Fatalf("reaped %v, want the drained client", got)
	}
	if r.Get("torrents") != nil {
		t.Fatal("a reaped client is still registered")
	}
	if _, _, err := r.Lease("torrents"); !errors.Is(err, ErrUnknownClient) {
		t.Fatalf("lease on a reaped client = %v, want ErrUnknownClient", err)
	}
}

// An add in flight holds a lease: the client cannot disappear between the add
// reaching it and the job being tracked.
func TestRegistryLeaseBlocksRemoval(t *testing.T) {
	r := NewRegistry()
	r.Apply(1, []ClientSpec{nzbSpec("usenet", "x")}, notInUse)
	c, release, err := r.Lease("usenet")
	if err != nil || c.ID != "usenet" || c.Category != "acquire" || c.Adapter == nil {
		t.Fatalf("lease = (%+v, %v)", c, err)
	}

	res := r.Apply(2, nil, notInUse)
	if !slices.Equal(res.Draining, []string{"usenet"}) {
		t.Fatalf("a leased client was not left draining: %+v", res)
	}
	if got := r.Reap(notInUse); len(got) != 0 {
		t.Fatalf("reaped %v while an add is in flight", got)
	}
	release()
	release() // idempotent
	if got := r.Reap(notInUse); !slices.Equal(got, []string{"usenet"}) {
		t.Fatalf("reaped %v after release", got)
	}
}

// Re-adding a draining client unchanged reactivates it without a rebuild.
func TestRegistryReAddDrainingClient(t *testing.T) {
	r := NewRegistry()
	r.Apply(1, []ClientSpec{qbSpec("torrents", "x")}, notInUse)
	ad := r.Get("torrents")
	r.Apply(2, nil, func(string) bool { return true })

	res := r.Apply(3, []ClientSpec{qbSpec("torrents", "x")}, notInUse)
	if len(res.Started) != 0 || r.Get("torrents") != ad {
		t.Fatalf("an unchanged re-add rebuilt the client: %+v", res)
	}
	if _, release, err := r.Lease("torrents"); err != nil {
		t.Fatalf("re-added client refuses adds: %v", err)
	} else {
		release()
	}
}

// A bad edit must not take down a client that already works.
func TestRegistryInvalidEditKeepsRunningClient(t *testing.T) {
	r := NewRegistry()
	r.Apply(1, []ClientSpec{nzbSpec("usenet", "x")}, notInUse)
	ad := r.Get("usenet")

	bad := nzbSpec("usenet", "x")
	bad.BaseURL = "not a url"
	res := r.Apply(2, []ClientSpec{bad}, notInUse)
	if len(res.Errors) != 1 || res.Errors[0].ID != "usenet" || len(res.Applied) != 0 {
		t.Fatalf("result = %+v", res)
	}
	if r.Get("usenet") != ad || len(res.Removed) != 0 {
		t.Fatal("the running client was replaced or removed by an invalid edit")
	}
	// The push did not fully take effect, so its revision is not reported.
	if r.Revision() != 1 || res.Revision != 1 {
		t.Errorf("revision = %d (result %d), want the previous one", r.Revision(), res.Revision)
	}
	if res := r.Apply(3, []ClientSpec{nzbSpec("usenet", "y")}, notInUse); res.Revision != 3 || r.Revision() != 3 {
		t.Errorf("a clean push after the failed one: revision = %d (result %d), want 3", r.Revision(), res.Revision)
	}
}

// Entries that do validate take effect even when others in the same push fail;
// only the revision waits for a clean push.
func TestRegistryPartialPushAppliesValidEntries(t *testing.T) {
	r := NewRegistry()
	bad := qbSpec("torrents", "x")
	bad.Type = "nope"
	res := r.Apply(5, []ClientSpec{nzbSpec("usenet", "x"), bad}, notInUse)
	if !slices.Equal(res.Applied, []string{"usenet"}) || len(res.Errors) != 1 || r.Get("usenet") == nil {
		t.Fatalf("result = %+v", res)
	}
	if res.Revision != 0 || r.Revision() != 0 {
		t.Errorf("revision = %d (result %d), want 0 until a push applies cleanly", r.Revision(), res.Revision)
	}
}

func TestRegistryRejectsBadAndDuplicateIDs(t *testing.T) {
	r := NewRegistry()
	res := r.Apply(1, []ClientSpec{
		nzbSpec("twice", "a"), qbSpec("twice", "b"), nzbSpec("Bad.ID", "c"), nzbSpec("fine", "d"),
	}, notInUse)
	if len(res.Errors) != 2 {
		t.Fatalf("errors = %+v, want one for the duplicate and one for the bad id", res.Errors)
	}
	if !slices.Equal(res.Applied, []string{"fine"}) || r.Get("twice") != nil {
		t.Fatalf("applied = %v; a duplicated id must apply neither entry", res.Applied)
	}
}

func TestRegistryListHasNoSecrets(t *testing.T) {
	r := NewRegistry()
	r.AddEnv(ClientSpec{Auth: AuthToken, Secret: testSecret, BaseURL: "http://odl.example"},
		NewODownloader("http://odl.example", testSecret))
	r.Apply(1, []ClientSpec{nzbSpec("usenet", testSecret)}, notInUse)

	b, err := json.Marshal(r.List())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), testSecret) {
		t.Fatalf("list carries a secret: %s", b)
	}
	var list []ClientInfo
	_ = json.Unmarshal(b, &list)
	if len(list) != 2 || !list[0].SecretSet || !list[1].SecretSet {
		t.Fatalf("list = %+v, want both clients with secretSet", list)
	}
	if list[0].ID != "odownloader" || list[0].Source != SourceEnv || list[1].Source != SourceAPI {
		t.Errorf("list = %+v", list)
	}
}

// Pushes, adds, polls and listings race in production (a config push lands
// while the poll loop runs). Run with -race.
func TestRegistryConcurrentUse(t *testing.T) {
	r := NewRegistry(NewNZBGet("http://env.example", "u", "p", ""))
	sets := [][]ClientSpec{
		{nzbSpec("a", "1"), qbSpec("b", "1")},
		{nzbSpec("a", "2"), qbSpec("c", "1")},
		{},
	}
	var jobs sync.Map // id -> bool, standing in for the gateway's tracked jobs
	inUse := func(id string) bool { _, ok := jobs.Load(id); return ok }

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				r.Apply(int64(i), sets[(i+w)%len(sets)], inUse)
			}
		}(w)
	}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id := string(rune('a' + (i+w)%3))
				if c, release, err := r.Lease(id); err == nil {
					jobs.Store(c.ID, true)
					release()
				}
				if i%7 == 0 {
					jobs.Delete(id)
				}
				_ = r.Get(id)
				_, _ = r.Lookup(id)
				_ = r.Names()
				_ = r.List()
				_ = r.Revision()
				r.Reap(inUse)
			}
		}(w)
	}
	wg.Wait()

	if r.Get("nzbget") == nil {
		t.Fatal("the env client did not survive concurrent pushes")
	}
	// Whatever the interleaving, a final push converges.
	jobs.Range(func(k, _ any) bool { jobs.Delete(k); return true })
	r.Apply(9999, sets[0], inUse)
	r.Reap(inUse)
	if got := fmt.Sprint(r.Names()); got != "[a b nzbget]" {
		t.Fatalf("names after the final push = %s", got)
	}
}
