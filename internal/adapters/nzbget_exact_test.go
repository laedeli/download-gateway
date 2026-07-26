package adapters

import "testing"

// NZBGet reports sizes both MB-rounded and as an exact 64-bit Lo/Hi pair. The
// exact pair is what keeps a progress bar smooth instead of stepping by 1 MiB.
func TestExact64PrefersLoHiPair(t *testing.T) {
	const twoGiB = int64(2) << 30
	lo := twoGiB & 0xFFFFFFFF
	hi := twoGiB >> 32
	if got := exact64(lo, hi, 2048); got != twoGiB {
		t.Fatalf("exact64(lo,hi) = %d, want %d", got, twoGiB)
	}
}

func TestExact64FallsBackToMB(t *testing.T) {
	// Older NZBGet builds (and history entries) may omit the Lo/Hi pair.
	if got, want := exact64(0, 0, 10), int64(10)*1024*1024; got != want {
		t.Fatalf("exact64 fallback = %d, want %d", got, want)
	}
}

func TestExact64AboveFourGiB(t *testing.T) {
	// Regression guard: a naive lo|hi without masking sign-extends and breaks
	// past 4 GiB, which is exactly the size range these downloads live in.
	const size = int64(6) << 30 // 6 GiB
	lo := size & 0xFFFFFFFF
	hi := size >> 32
	if got := exact64(lo, hi, 6144); got != size {
		t.Fatalf("exact64 6GiB = %d, want %d", got, size)
	}
}

// A queued or paused group must not be reported as actively downloading — the
// console distinguishes them, and the old fold hard-coded "downloading".
func TestFoldNZBGroupStates(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   Status
	}{
		{"DOWNLOADING", StatusDownloading},
		{"QUEUED", StatusQueued},
		{"PAUSED", StatusQueued},
		{"REPAIRING", StatusDownloading},
		{"UNPACKING", StatusDownloading},
	} {
		v := foldNZBGroup(nzbGroup{NZBID: 1, NZBName: "x", Status: tc.status})
		if v.State != tc.want {
			t.Errorf("status %q folded to %q, want %q", tc.status, v.State, tc.want)
		}
		if v.NativeState != tc.status {
			t.Errorf("native state lost for %q: got %q", tc.status, v.NativeState)
		}
	}
}

// The group fold must use the exact byte pair and compute an ETA from the rate.
func TestFoldNZBGroupTelemetry(t *testing.T) {
	const total = int64(6) << 30 // 6 GiB
	const done = int64(2) << 30
	g := nzbGroup{
		Status:           "DOWNLOADING",
		FileSizeLo:       total & 0xFFFFFFFF,
		FileSizeHi:       total >> 32,
		DownloadedSizeLo: done & 0xFFFFFFFF,
		DownloadedSizeHi: done >> 32,
		RemainingSizeLo:  (total - done) & 0xFFFFFFFF,
		RemainingSizeHi:  (total - done) >> 32,
		DownloadRate:     1 << 20, // 1 MiB/s
		Health:           980,
	}
	v := foldNZBGroup(g)
	if v.BytesTotal != total || v.BytesDone != done {
		t.Fatalf("bytes = %d/%d, want %d/%d", v.BytesDone, v.BytesTotal, done, total)
	}
	if v.EtaSec != (total-done)/(1<<20) {
		t.Errorf("eta = %d", v.EtaSec)
	}
	if v.Health != 980 {
		t.Errorf("health = %d, want 980", v.Health)
	}
}

// History drives the terminal states acquire reacts to.
func TestFoldNZBHistory(t *testing.T) {
	ok := foldNZBHistory(nzbHistory{Status: "SUCCESS/ALL", DestDir: "/data/x", FileSizeMB: 10})
	if ok.State != StatusCompleted || len(ok.Files) != 1 || ok.Files[0] != "/data/x" {
		t.Fatalf("success fold = %+v", ok)
	}
	if ok.BytesDone != ok.BytesTotal {
		t.Errorf("a completed job should report full bytes: %d/%d", ok.BytesDone, ok.BytesTotal)
	}
	for _, st := range []string{"FAILURE/PAR", "WARNING/HEALTH", "DELETED/MANUAL"} {
		if v := foldNZBHistory(nzbHistory{Status: st}); v.State != StatusFailed {
			t.Errorf("%q folded to %q, want failed", st, v.State)
		}
	}
}

// NewJobView must start with "not reported" sentinels so an adapter that never
// sets seeders doesn't publish a misleading 0.
func TestNewJobViewSentinels(t *testing.T) {
	v := NewJobView()
	if v.EtaSec != -1 || v.Seeders != -1 || v.Leechers != -1 || v.Health != -1 {
		t.Fatalf("NewJobView sentinels = %+v", v)
	}
}
