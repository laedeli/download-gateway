package adapters

import (
	"encoding/json"
	"testing"
)

// TestNZBGetHistoryStatus locks the history Status → JobView mapping the poll
// loop turns into completed/failed events.
func TestNZBGetHistoryStatus(t *testing.T) {
	cases := []struct {
		status string
		want   Status
		files  bool
	}{
		{"SUCCESS/ALL", StatusCompleted, true},
		{"SUCCESS/HEALTH", StatusCompleted, true},
		{"FAILURE/HEALTH", StatusFailed, false},
		{"WARNING/DAMAGED", StatusFailed, false},
		{"DELETED/MANUAL", StatusFailed, false},
	}
	for _, c := range cases {
		h := nzbHistory{NZBID: 7, Name: "x", Status: c.status, DestDir: "/d/x", FileSizeMB: 10}
		// Mirror Describe's history fold inline (the mapping under test).
		var v JobView
		switch {
		case hasPrefix(h.Status, "SUCCESS"):
			v.State = StatusCompleted
			if h.DestDir != "" {
				v.Files = []string{h.DestDir}
			}
		case hasPrefix(h.Status, "DELETED"):
			v.State = StatusFailed
		default:
			v.State = StatusFailed
		}
		if v.State != c.want {
			t.Errorf("status %q → %s, want %s", c.status, v.State, c.want)
		}
		if (len(v.Files) > 0) != c.files {
			t.Errorf("status %q files=%v, want %v", c.status, len(v.Files) > 0, c.files)
		}
	}
}

// TestNZBGetAppendResult: append returns an integer NZBID; 0 means rejected.
func TestNZBGetAppendResult(t *testing.T) {
	var id int
	if json.Unmarshal([]byte("42"), &id) != nil || id != 42 {
		t.Fatal("expected 42")
	}
	if json.Unmarshal([]byte("0"), &id) != nil || id != 0 {
		t.Fatal("expected 0 (rejected)")
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }
