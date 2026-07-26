package main

import (
	"testing"
	"time"

	"gitlab.nalet.cloud/stube/download-gateway/internal/adapters"
)

// NZBGet's listgroups carries no per-group DownloadRate, so a usenet download
// would show no speed at all. Derive it from the byte delta between polls.
func TestFillDerivedComputesSpeedAndEta(t *testing.T) {
	v := adapters.NewJobView()
	v.BytesDone = 10 << 20 // 10 MiB
	v.BytesTotal = 110 << 20
	fillDerived(&v, 5<<20, time.Now().Add(-5*time.Second))

	want := int64((5 << 20) / 5) // 5 MiB over 5s
	if v.SpeedBps < want-want/10 || v.SpeedBps > want+want/10 {
		t.Fatalf("derived speed = %d, want ~%d", v.SpeedBps, want)
	}
	if v.EtaSec <= 0 {
		t.Fatalf("eta should be derived from the rate, got %d", v.EtaSec)
	}
}

// A rate the client DID report must win — never overwrite real data.
func TestFillDerivedKeepsReportedSpeed(t *testing.T) {
	v := adapters.NewJobView()
	v.SpeedBps = 1234
	v.BytesDone = 10 << 20
	fillDerived(&v, 0, time.Now().Add(-5*time.Second))
	if v.SpeedBps != 1234 {
		t.Fatalf("speed = %d, want the reported 1234", v.SpeedBps)
	}
}

// No previous sample, no movement, or a byte counter that went backwards must
// not invent a speed.
func TestFillDerivedNoFabrication(t *testing.T) {
	for _, tc := range []struct {
		name      string
		bytesDone int64
		prevBytes int64
		prevAt    time.Time
	}{
		{"no previous sample", 100, 0, time.Time{}},
		{"no movement", 100, 100, time.Now().Add(-5 * time.Second)},
		{"counter reset", 50, 100, time.Now().Add(-5 * time.Second)},
		{"sample too fresh", 200, 100, time.Now()},
	} {
		v := adapters.NewJobView()
		v.BytesDone = tc.bytesDone
		fillDerived(&v, tc.prevBytes, tc.prevAt)
		if v.SpeedBps != 0 {
			t.Errorf("%s: speed = %d, want 0", tc.name, v.SpeedBps)
		}
	}
}
