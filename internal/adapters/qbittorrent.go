package adapters

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// QBittorrent drives a stock qbittorrent-nox instance over its WebUI API v2
// (docs: /api/v2/{auth,torrents}/…). qB's add endpoint returns only "Ok." with
// no id, and torrents are keyed by infohash which we don't know upfront for a
// .torrent URL — so on Add we attach a UNIQUE TAG and use that tag as the
// stable clientJobID; Describe/Status/Remove filter torrents/info by it. The
// SID cookie from auth/login is held in a cookie jar and re-minted on a 403.
type QBittorrent struct {
	BaseURL  string // e.g. http://qbittorrent.zaentrum-beta.svc:8080
	Username string
	Password string
	HTTP     *http.Client
}

// NewQBittorrent builds the adapter with a cookie-jar HTTP client (the jar
// carries the SID session across calls).
func NewQBittorrent(baseURL, user, pass string) *QBittorrent {
	jar, _ := cookiejar.New(nil)
	return &QBittorrent{
		BaseURL:  strings.TrimRight(baseURL, "/"),
		Username: user,
		Password: pass,
		HTTP:     &http.Client{Timeout: 30 * time.Second, Jar: jar},
	}
}

func (q *QBittorrent) Name() string { return "qbittorrent" }

// post issues a form POST. qB's CSRF guard rejects requests whose Referer/Origin
// doesn't match the WebUI host, so we always set Referer to the base URL.
func (q *QBittorrent) post(ctx context.Context, path string, form url.Values) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.BaseURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", q.BaseURL)
	return q.HTTP.Do(req)
}

func (q *QBittorrent) get(ctx context.Context, path string, query url.Values) (*http.Response, error) {
	u := q.BaseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Referer", q.BaseURL)
	return q.HTTP.Do(req)
}

// login mints a fresh SID cookie. qB returns 200 "Ok." on success and 403
// "Fails." on bad credentials. When the WebUI subnet whitelist is enabled the
// caller is pre-authenticated, so login can return 200 without "Ok." or 204 No
// Content and no SID cookie — all of which are success (subsequent API calls
// work without a session). Only a 403 (or other non-2xx) is a real failure.
func (q *QBittorrent) login(ctx context.Context) error {
	resp, err := q.post(ctx, "/api/v2/auth/login", url.Values{
		"username": {q.Username}, "password": {q.Password},
	})
	if err != nil {
		return fmt.Errorf("qbittorrent login: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("qbittorrent login failed: %d %q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// authed runs fn, logging in first if we hold no session and retrying once on a
// 403 (an expired SID).
func (q *QBittorrent) authed(ctx context.Context, fn func() (*http.Response, error)) (*http.Response, error) {
	if !q.hasSession() {
		if err := q.login(ctx); err != nil {
			return nil, err
		}
	}
	resp, err := fn()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusForbidden {
		resp.Body.Close()
		if err := q.login(ctx); err != nil {
			return nil, err
		}
		return fn()
	}
	return resp, nil
}

func (q *QBittorrent) hasSession() bool {
	u, err := url.Parse(q.BaseURL)
	if err != nil || q.HTTP.Jar == nil {
		return false
	}
	for _, c := range q.HTTP.Jar.Cookies(u) {
		if c.Name == "SID" && c.Value != "" {
			return true
		}
	}
	return false
}

// Add attaches the torrent/magnet with a unique tag and returns that tag as the
// stable clientJobID.
func (q *QBittorrent) Add(ctx context.Context, j Job) (string, error) {
	tag := newJobTag()
	form := url.Values{
		"urls": {j.Source},
		"tags": {tag},
	}
	if j.SavePath != "" {
		form.Set("savepath", j.SavePath)
		form.Set("autoTMM", "false") // honor the explicit save path, not a category default
	}
	resp, err := q.authed(ctx, func() (*http.Response, error) {
		return q.post(ctx, "/api/v2/torrents/add", form)
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("qbittorrent add: %d %q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	// qB replies 200 "Ok." on accept, 200 "Fails." when the source is unusable.
	if strings.TrimSpace(string(body)) == "Fails." {
		return "", fmt.Errorf("qbittorrent add rejected the source")
	}
	slog.Debug("qbittorrent add", "tag", tag, "save_path", j.SavePath)
	return tag, nil
}

// qbTorrent is the subset of /api/v2/torrents/info we read.
type qbTorrent struct {
	Hash        string  `json:"hash"`
	Name        string  `json:"name"`
	State       string  `json:"state"`
	Progress    float64 `json:"progress"`
	DlSpeed     int64   `json:"dlspeed"`
	Eta         int64   `json:"eta"`
	Size        int64   `json:"size"`
	Completed   int64   `json:"completed"`
	ContentPath string  `json:"content_path"`
	SavePath    string  `json:"save_path"`
}

// byTag returns the torrent carrying our tag, or ok=false when it isn't present
// yet (a fresh magnet resolves metadata before info lists it).
func (q *QBittorrent) byTag(ctx context.Context, tag string) (qbTorrent, bool, error) {
	resp, err := q.authed(ctx, func() (*http.Response, error) {
		return q.get(ctx, "/api/v2/torrents/info", url.Values{"tag": {tag}})
	})
	if err != nil {
		return qbTorrent{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return qbTorrent{}, false, fmt.Errorf("qbittorrent info: %d %q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var list []qbTorrent
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return qbTorrent{}, false, fmt.Errorf("qbittorrent info decode: %w", err)
	}
	if len(list) == 0 {
		return qbTorrent{}, false, nil
	}
	return list[0], true, nil
}

func (q *QBittorrent) Status(ctx context.Context, tag string) (Status, error) {
	v, err := q.Describe(ctx, tag)
	if err != nil {
		return "", err
	}
	return v.State, nil
}

func (q *QBittorrent) Describe(ctx context.Context, tag string) (JobView, error) {
	t, ok, err := q.byTag(ctx, tag)
	if err != nil {
		return JobView{}, err
	}
	if !ok {
		// Accepted but not yet visible (metadata fetch) — report queued.
		return JobView{State: StatusQueued, EtaSec: -1}, nil
	}
	return foldQB(t), nil
}

func (q *QBittorrent) Remove(ctx context.Context, tag string) error {
	t, ok, err := q.byTag(ctx, tag)
	if err != nil {
		return err
	}
	if !ok {
		return nil // already gone
	}
	// Non-destructive: drop the torrent from qB but keep the downloaded bytes
	// (acquire owns cleanup of the inbox after packaging).
	resp, err := q.authed(ctx, func() (*http.Response, error) {
		return q.post(ctx, "/api/v2/torrents/delete", url.Values{
			"hashes": {t.Hash}, "deleteFiles": {"false"},
		})
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("qbittorrent delete: %d %q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// foldQB maps a qB torrent snapshot to the gateway's JobView. A *UP / seeding
// state (or progress==1) means the download completed — that is the signal
// acquire waits on to stage + package.
func foldQB(t qbTorrent) JobView {
	v := JobView{
		Title:      t.Name,
		BytesDone:  t.Completed,
		BytesTotal: t.Size,
		SpeedBps:   t.DlSpeed,
		EtaSec:     t.Eta,
	}
	// qB reports eta 8640000 (100 days) for "unknown/infinite".
	if t.Eta >= 8640000 || t.Eta < 0 {
		v.EtaSec = -1
	}
	switch t.State {
	case "error", "missingFiles":
		v.State = StatusFailed
		v.Message = "qbittorrent state: " + t.State
	case "uploading", "pausedUP", "queuedUP", "stalledUP", "checkingUP", "forcedUP":
		v.State = StatusCompleted
		v.Files = contentFiles(t)
	default:
		if t.Progress >= 1.0 {
			v.State = StatusCompleted
			v.Files = contentFiles(t)
		} else {
			v.State = StatusDownloading
		}
	}
	return v
}

// contentFiles reports the completed content path (a file, or the torrent's
// folder) so acquire can stage the video. content_path is absolute; fall back
// to save_path when qB hasn't populated it.
func contentFiles(t qbTorrent) []string {
	if t.ContentPath != "" {
		return []string{t.ContentPath}
	}
	if t.SavePath != "" {
		return []string{t.SavePath}
	}
	return nil
}

// newJobTag returns a collision-resistant tag used as the clientJobID.
func newJobTag() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read failing is effectively impossible; fall back to time.
		return "dlg-" + fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return "dlg-" + hex.EncodeToString(b[:])
}
