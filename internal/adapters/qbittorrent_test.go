package adapters

import "testing"

// TestFoldQB locks the qB state → JobView mapping the poll loop turns into
// events: seeding/UP states mean the download completed (the signal acquire
// waits on), error states fail, everything else is in-progress.
func TestFoldQB(t *testing.T) {
	cases := []struct {
		name  string
		in    qbTorrent
		want  Status
		files bool
	}{
		{"downloading", qbTorrent{State: "downloading", Progress: 0.4, Eta: 120}, StatusDownloading, false},
		{"metadata", qbTorrent{State: "metaDL", Progress: 0}, StatusDownloading, false},
		{"seeding-uploading", qbTorrent{State: "uploading", Progress: 1, ContentPath: "/x/a.mkv"}, StatusCompleted, true},
		{"seeding-stalledUP", qbTorrent{State: "stalledUP", Progress: 1, ContentPath: "/x/a.mkv"}, StatusCompleted, true},
		{"complete-by-progress", qbTorrent{State: "pausedDL", Progress: 1, ContentPath: "/x/a.mkv"}, StatusCompleted, true},
		{"error", qbTorrent{State: "error"}, StatusFailed, false},
		{"missing", qbTorrent{State: "missingFiles"}, StatusFailed, false},
	}
	for _, c := range cases {
		got := foldQB(c.in)
		if got.State != c.want {
			t.Errorf("%s: state = %s, want %s", c.name, got.State, c.want)
		}
		if (len(got.Files) > 0) != c.files {
			t.Errorf("%s: files present = %v, want %v", c.name, len(got.Files) > 0, c.files)
		}
	}
}

// TestEtaInfinity: qB's 100-day sentinel becomes -1 (unknown).
func TestEtaInfinity(t *testing.T) {
	if v := foldQB(qbTorrent{State: "downloading", Eta: 8640000}); v.EtaSec != -1 {
		t.Errorf("eta sentinel not normalized: %d", v.EtaSec)
	}
	if v := foldQB(qbTorrent{State: "downloading", Eta: 90}); v.EtaSec != 90 {
		t.Errorf("finite eta changed: %d", v.EtaSec)
	}
}

// TestNewJobTag: unique, prefixed, non-empty.
func TestNewJobTag(t *testing.T) {
	a, b := newJobTag(), newJobTag()
	if a == b {
		t.Fatal("tags collided")
	}
	for _, tag := range []string{a, b} {
		if len(tag) < 5 || tag[:4] != "dlg-" {
			t.Errorf("bad tag: %q", tag)
		}
	}
}
