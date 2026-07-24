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
	//        AddPaused, DupeKey, DupeScore, DupeMode)
	res, err := n.call(ctx, "append",
		name+".nzb", j.Source, n.Category, 0, false, false, "", 0, "")
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
	NZBID            int    `json:"NZBID"`
	NZBName          string `json:"NZBName"`
	Status           string `json:"Status"`
	FileSizeMB       int64  `json:"FileSizeMB"`
	DownloadedSizeMB int64  `json:"DownloadedSizeMB"`
	RemainingSizeMB  int64  `json:"RemainingSizeMB"`
	DownloadRate     int64  `json:"DownloadRate"` // bytes/sec
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
				v := JobView{
					Title:      g.NZBName,
					BytesDone:  g.DownloadedSizeMB * 1024 * 1024,
					BytesTotal: g.FileSizeMB * 1024 * 1024,
					SpeedBps:   g.DownloadRate,
					EtaSec:     -1,
				}
				if g.DownloadRate > 0 {
					v.EtaSec = g.RemainingSizeMB * 1024 * 1024 / g.DownloadRate
				}
				v.State = StatusDownloading // queued/paused/pp are all "not done yet"
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
				v := JobView{Title: h.Name, BytesTotal: h.FileSizeMB * 1024 * 1024, EtaSec: -1}
				switch {
				case strings.HasPrefix(h.Status, "SUCCESS"):
					v.State = StatusCompleted
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
	return JobView{State: StatusQueued, EtaSec: -1}, nil
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
