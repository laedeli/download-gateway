package adapters

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// A package id is sent as what it is: one query value when the package is
// described, one path segment when it is removed — never a parameter or a
// path of its own.
func TestODownloaderEscapesThePackageID(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			queries = append(queries, r.URL.RawQuery)
			_, _ = w.Write([]byte(`{"content":[]}`))
		case http.MethodDelete:
			paths = append(paths, r.URL.EscapedPath())
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	o := NewODownloader(srv.URL, "")
	const id = "pkg/../all&size=1#x?y"
	if _, err := o.Describe(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := o.Remove(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 1 || queries[0] != "packageId=pkg%2F..%2Fall%26size%3D1%23x%3Fy&size=1000" {
		t.Errorf("describe asked %q", queries)
	}
	if len(paths) != 1 || paths[0] != "/api/v1/packages/pkg%2F..%2Fall&size=1%23x%3Fy" {
		t.Errorf("remove asked %q", paths)
	}
}
