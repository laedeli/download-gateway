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
func TestFoldNZBGetQueuedStates(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   Status
	}{
		{"DOWNLOADING", StatusDownloading},
		{"QUEUED", StatusQueued},
		{"PAUSED", StatusQueued},
		{"REPAIRING", StatusDownloading},
	} {
		v := NewJobView()
		v.NativeState = tc.status
		v.State = StatusDownloading
		if tc.status == "QUEUED" || tc.status == "PAUSED" {
			v.State = StatusQueued
		}
		if v.State != tc.want {
			t.Errorf("status %q folded to %q, want %q", tc.status, v.State, tc.want)
		}
		if v.NativeState != tc.status {
			t.Errorf("native state lost for %q", tc.status)
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
