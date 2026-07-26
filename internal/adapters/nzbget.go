package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NZBGet drives an NZBGet instance over its JSON-RPC API
// (POST {BaseURL}/jsonrpc, HTTP basic auth ControlUsername/Password). It adds an
// NZB by handing NZBGet the release's URL (NZBGet fetches it — Prowlarr download
// URLs embed the apikey), and folds listgroups (active) + history (terminal)
// into the gateway's JobView. The clientJobID is NZBGet's integer NZBID.
type NZBGet struct {
	BaseURL  string
	Username string
	Password string
	Category string
	HTTP     *http.Client
}

func NewNZBGet(baseURL, user, pass, category string) *NZBGet {
	return &NZBGet{
		BaseURL:  strings.TrimRight(baseURL, "/"),
		Username: user,
		Password: pass,
		Category: category,
		HTTP:     &http.Client{Timeout: 30 * time.Second},
	}
}

func (n *NZBGet) Name() string { return "nzbget" }

// call issues one JSON-RPC request and returns the raw `result` value.
func (n *NZBGet) call(ctx context.Context, method string, params ...any) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]any{"method": method, "params": params, "id": 1})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.BaseURL+"/jsonrpc", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if n.Username != "" {
		req.SetBasicAuth(n.Username, n.Password)
	}
	resp, err := n.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("nzbget %s: %d %q", method, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var rpc struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpc); err != nil {
		return nil, err
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("nzbget %s: %s", method, rpc.Error.Message)
	}
	return rpc.Result, nil
}

// Add appends the NZB by URL. NZBGet's append takes the URL as NZBContent and
// fetches it. Returns the new NZBID as the clientJobID.
func (n *NZBGet) Add(ctx context.Context, j Job) (string, error) {
	name := strings.TrimSpace(j.Title)
	if name == "" {
		name = "download"
	}
	// append(NZBFilename, NZBContent(URL|base64), Category, Priority, AddToTop,
	//        AddPaused, DupeKey, DupeScore, DupeMode). DupeMode must be a valid
	// enum — FORCE always adds (a request should never be silently deduped away).
	res, err := n.call(ctx, "append",
		name+".nzb", j.Source, n.Category, 0, false, false, "", 0, "FORCE")
	if err != nil {
		return "", err
	}
	var nzbID int
	if err := json.Unmarshal(res, &nzbID); err != nil {
		return "", fmt.Errorf("nzbget append: bad result %s", string(res))
	}
	if nzbID <= 0 {
		return "", fmt.Errorf("nzbget append rejected the source (id=%d)", nzbID)
	}
	slog.Debug("nzbget append", "nzbid", nzbID, "name", name)
	return strconv.Itoa(nzbID), nil
}

type nzbGroup struct {
	NZBID   int    `json:"NZBID"`
	NZBName string `json:"NZBName"`
	Status  string `json:"Status"`
	// NZBGet reports every size twice: an MB-rounded field and an exact 64-bit
	// Lo/Hi pair. Use the exact pair so progress doesn't jump in 1 MiB steps.
	FileSizeMB        int64 `json:"FileSizeMB"`
	FileSizeLo        int64 `json:"FileSizeLo"`
	FileSizeHi        int64 `json:"FileSizeHi"`
	DownloadedSizeMB  int64 `json:"DownloadedSizeMB"`
	DownloadedSizeLo  int64 `json:"DownloadedSizeLo"`
	DownloadedSizeHi  int64 `json:"DownloadedSizeHi"`
	RemainingSizeMB   int64 `json:"RemainingSizeMB"`
	RemainingSizeLo   int64 `json:"RemainingSizeLo"`
	RemainingSizeHi   int64 `json:"RemainingSizeHi"`
	DownloadRate      int64 `json:"DownloadRate"` // bytes/sec
	Health            int32 `json:"Health"`       // 0-1000
	CriticalHealth    int32 `json:"CriticalHealth"`
	PostStageProgress int32 `json:"PostStageProgress"`
}

// exact64 rebuilds a 64-bit size from NZBGet's Lo/Hi pair, falling back to the
// MB-rounded value when the pair is absent.
func exact64(lo, hi, mb int64) int64 {
	if v := hi<<32 | (lo & 0xFFFFFFFF); v > 0 {
		return v
	}
	return mb * 1024 * 1024
}

type nzbHistory struct {
	NZBID      int    `json:"NZBID"`
	Name       string `json:"Name"`
	Status     string `json:"Status"` // SUCCESS/…, FAILURE/…, WARNING/…, DELETED/…
	DestDir    string `json:"DestDir"`
	FileSizeMB int64  `json:"FileSizeMB"`
}

func (n *NZBGet) Status(ctx context.Context, id string) (Status, error) {
	v, err := n.Describe(ctx, id)
	if err != nil {
		return "", err
	}
	return v.State, nil
}

// Describe folds the active queue (listgroups) then history into a JobView.
func (n *NZBGet) Describe(ctx context.Context, id string) (JobView, error) {
	want, _ := strconv.Atoi(id)

	if res, err := n.call(ctx, "listgroups", 0); err == nil {
		var groups []nzbGroup
		if json.Unmarshal(res, &groups) == nil {
			for _, g := range groups {
				if g.NZBID != want {
					continue
				}
				remaining := exact64(g.RemainingSizeLo, g.RemainingSizeHi, g.RemainingSizeMB)
				v := NewJobView()
				v.Title = g.NZBName
				v.NativeState = g.Status
				v.BytesDone = exact64(g.DownloadedSizeLo, g.DownloadedSizeHi, g.DownloadedSizeMB)
				v.BytesTotal = exact64(g.FileSizeLo, g.FileSizeHi, g.FileSizeMB)
				v.SpeedBps = g.DownloadRate
				v.Health = g.Health
				if g.DownloadRate > 0 {
					v.EtaSec = remaining / g.DownloadRate
				}
				// Everything in the queue is "not done yet", but keep the native
				// status so a console can show paused / repairing / unpacking.
				v.State = StatusDownloading
				if g.Status == "QUEUED" || g.Status == "PAUSED" {
					v.State = StatusQueued
				}
				return v, nil
			}
		}
	}

	if res, err := n.call(ctx, "history", false); err == nil {
		var hist []nzbHistory
		if json.Unmarshal(res, &hist) == nil {
			for _, h := range hist {
				if h.NZBID != want {
					continue
				}
				v := NewJobView()
				v.Title = h.Name
				v.NativeState = h.Status
				v.BytesTotal = h.FileSizeMB * 1024 * 1024
				switch {
				case strings.HasPrefix(h.Status, "SUCCESS"):
					v.State = StatusCompleted
					v.BytesDone = v.BytesTotal
					if h.DestDir != "" {
						v.Files = []string{h.DestDir}
					}
				case strings.HasPrefix(h.Status, "DELETED"):
					v.State = StatusFailed
					v.Message = "removed: " + h.Status
				default: // FAILURE/…, WARNING/…
					v.State = StatusFailed
					v.Message = "nzbget: " + h.Status
				}
				return v, nil
			}
		}
	}

	// Accepted but not yet visible in either list.
	v := NewJobView()
	v.State = StatusQueued
	v.NativeState = "pending"
	return v, nil
}

// Pause pauses one queue group.
func (n *NZBGet) Pause(ctx context.Context, id string) error {
	return n.edit(ctx, id, "GroupPause")
}

// Resume resumes one queue group.
func (n *NZBGet) Resume(ctx context.Context, id string) error {
	return n.edit(ctx, id, "GroupResume")
}

func (n *NZBGet) edit(ctx context.Context, id, command string) error {
	want, err := strconv.Atoi(id)
	if err != nil {
		return fmt.Errorf("nzbget %s: bad id %q", command, id)
	}
	_, err = n.call(ctx, "editqueue", command, 0, "", []int{want})
	return err
}

// ClientStatus reports NZBGet's global download state, including whether any
// usenet server is actually connected — the usual cause of a stuck queue.
func (n *NZBGet) ClientStatus(ctx context.Context) ClientStatus {
	cs := ClientStatus{Name: n.Name()}
	res, err := n.call(ctx, "status")
	if err != nil {
		cs.Error = err.Error()
		return cs
	}
	var st struct {
		DownloadRate    int64 `json:"DownloadRate"`
		ServerPaused    bool  `json:"ServerPaused"`
		Download2Paused bool  `json:"Download2Paused"`
		FreeDiskSpaceLo int64 `json:"FreeDiskSpaceLo"`
		FreeDiskSpaceHi int64 `json:"FreeDiskSpaceHi"`
		FreeDiskSpaceMB int64 `json:"FreeDiskSpaceMB"`
		RemainingSizeMB int64 `json:"RemainingSizeMB"`
		PostJobCount    int32 `json:"PostJobCount"`
		ArticleCacheMB  int64 `json:"ArticleCacheMB"`
		QuotaReached    bool  `json:"QuotaReached"`
		NewsServers     []struct {
			ID     int  `json:"ID"`
			Active bool `json:"Active"`
		} `json:"NewsServers"`
	}
	if err := json.Unmarshal(res, &st); err != nil {
		cs.Error = err.Error()
		return cs
	}
	free := exact64(st.FreeDiskSpaceLo, st.FreeDiskSpaceHi, st.FreeDiskSpaceMB)
	active := 0
	for _, s := range st.NewsServers {
		if s.Active {
			active++
		}
	}
	cs.Reachable = true
	cs.DownBps = st.DownloadRate
	cs.Paused = st.ServerPaused || st.Download2Paused
	cs.FreeDisk = &free
	cs.Detail = map[string]string{
		"news_servers_active": strconv.Itoa(active) + "/" + strconv.Itoa(len(st.NewsServers)),
		"post_jobs":           strconv.Itoa(int(st.PostJobCount)),
	}
	if st.QuotaReached {
		cs.Detail["quota"] = "reached"
	}
	return cs
}

// Adopt re-discovers this gateway's jobs after a restart: everything in our
// category that is still in the queue, plus anything already in history.
func (n *NZBGet) Adopt(ctx context.Context) ([]AdoptedJob, error) {
	var out []AdoptedJob
	res, err := n.call(ctx, "listgroups", 0)
	if err != nil {
		return nil, err
	}
	var groups []struct {
		NZBID    int    `json:"NZBID"`
		NZBName  string `json:"NZBName"`
		Category string `json:"Category"`
	}
	if err := json.Unmarshal(res, &groups); err != nil {
		return nil, err
	}
	for _, g := range groups {
		if n.Category != "" && g.Category != n.Category {
			continue
		}
		out = append(out, AdoptedJob{ClientJobID: strconv.Itoa(g.NZBID), Title: g.NZBName})
	}
	return out, nil
}

// Remove drops the item from the active queue or history.
func (n *NZBGet) Remove(ctx context.Context, id string) error {
	want, err := strconv.Atoi(id)
	if err != nil {
		return fmt.Errorf("nzbget remove: bad id %q", id)
	}
	// Try the active queue first, then history. editqueue returns a bool.
	if _, err := n.call(ctx, "editqueue", "GroupFinalDelete", 0, "", []int{want}); err == nil {
		return nil
	}
	if _, err := n.call(ctx, "editqueue", "HistoryDelete", 0, "", []int{want}); err != nil {
		return err
	}
	return nil
}
