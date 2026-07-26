package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ODownloader drives the in-cluster oDownloader daemon over its
// static-token-authenticated direct API (verified against the daemon-api
// controllers): POST /api/v1/links/add -> AddLinksResult{packageId,...};
// GET /api/v1/downloads/{id} -> DownloadLinkView; DELETE /api/v1/packages/{id}.
//
// One Add creates one package containing N download links. The adapter's
// clientJobID is the packageId; Describe aggregates the package's links
// into a single JobView the gateway turns into events.
type ODownloader struct {
	BaseURL string // e.g. http://odownloader.cloud-nalet-odownloader.svc:80
	Token   string // bearer; from secret odownloader-api key "token"
	HTTP    *http.Client
}

func NewODownloader(baseURL, token string) *ODownloader {
	return &ODownloader{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    &http.Client{Timeout: 20 * time.Second},
	}
}

func (o *ODownloader) Name() string { return "odownloader" }

func (o *ODownloader) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, o.BaseURL+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if o.Token != "" {
		req.Header.Set("Authorization", "Bearer "+o.Token)
	}
	req.Header.Set("Accept", "application/json")
	return o.HTTP.Do(req)
}

type odlAddRequest struct {
	URLs        []string `json:"urls"`
	PackageName string   `json:"packageName,omitempty"`
	Comment     string   `json:"comment,omitempty"`
	Autostart   bool     `json:"autostart"`
}

type odlAddResult struct {
	PackageID   string   `json:"packageId"`
	DownloadIDs []string `json:"downloadIds"`
	Rejected    []string `json:"rejected"`
}

func (o *ODownloader) Add(ctx context.Context, j Job) (string, error) {
	resp, err := o.do(ctx, http.MethodPost, "/api/v1/links/add", odlAddRequest{
		URLs:        []string{j.Source},
		PackageName: j.Title,
		Autostart:   true,
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("odownloader add: %d %s", resp.StatusCode, string(b))
	}
	var out odlAddResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.PackageID == "" {
		return "", fmt.Errorf("odownloader add: empty packageId (rejected=%v)", out.Rejected)
	}
	return out.PackageID, nil
}

// DownloadState mirrors core-engine/DownloadState.java.
type odlDownloadView struct {
	ID                  string `json:"id"`
	PackageID           string `json:"packageId"`
	Name                string `json:"name"`
	BytesDone           int64  `json:"bytesDone"`
	BytesTotal          int64  `json:"bytesTotal"`
	SpeedBytesPerSecond int64  `json:"speedBytesPerSecond"`
	EtaSeconds          *int64 `json:"etaSeconds"`
	State               string `json:"state"`
	Message             string `json:"message"`
	OutputPath          string `json:"outputPath"`
}

type odlPage struct {
	Content []odlDownloadView `json:"content"`
}

// Describe lists the package's downloads and folds them into one JobView.
func (o *ODownloader) Describe(ctx context.Context, packageID string) (JobView, error) {
	resp, err := o.do(ctx, http.MethodGet, "/api/v1/downloads?packageId="+packageID+"&size=1000", nil)
	if err != nil {
		return JobView{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return JobView{}, fmt.Errorf("odownloader describe: %d %s", resp.StatusCode, string(b))
	}
	var page odlPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return JobView{}, err
	}
	return foldODL(page.Content), nil
}

// foldODL aggregates per-link states into one job-level JobView.
//
// Terminal rule: a package is completed only when every link is FINISHED;
// it is failed if any link is FAILED/CANCELLED and none is still running;
// otherwise it is downloading (or queued if nothing has started).
func foldODL(links []odlDownloadView) JobView {
	if len(links) == 0 {
		jv := NewJobView()
		jv.State = StatusQueued
		return jv
	}
	jv := NewJobView()
	allFinished := true
	anyRunning := false
	anyFailed := false
	anyStarted := false
	files := make([]string, 0, len(links))
	for _, l := range links {
		jv.BytesDone += l.BytesDone
		jv.BytesTotal += l.BytesTotal
		jv.SpeedBps += l.SpeedBytesPerSecond
		if jv.Title == "" {
			jv.Title = l.Name
		}
		if l.Message != "" {
			jv.Message = l.Message
		}
		if l.OutputPath != "" {
			files = append(files, l.OutputPath)
		}
		switch l.State {
		case "FINISHED":
			anyStarted = true
		case "RUNNING", "PAUSED", "WAITING_FOR_CHALLENGE":
			allFinished = false
			anyRunning = true
			anyStarted = true
		case "FAILED", "CANCELLED":
			allFinished = false
			anyFailed = true
		default: // QUEUED and unknowns
			allFinished = false
		}
	}
	switch {
	case allFinished:
		jv.State = StatusCompleted
		jv.Files = files
	case anyFailed && !anyRunning:
		jv.State = StatusFailed
	case anyStarted:
		jv.State = StatusDownloading
	default:
		jv.State = StatusQueued
	}
	return jv
}

func (o *ODownloader) Status(ctx context.Context, packageID string) (Status, error) {
	v, err := o.Describe(ctx, packageID)
	if err != nil {
		return "", err
	}
	return v.State, nil
}

func (o *ODownloader) Remove(ctx context.Context, packageID string) error {
	resp, err := o.do(ctx, http.MethodDelete, "/api/v1/packages/"+packageID, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("odownloader remove: %d %s", resp.StatusCode, string(b))
	}
	return nil
}
