package adapters

import (
	"bytes"
	"context"
	"encoding/base64"
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
// NZB either by uploading the file content (the caller fetched it, so the
// indexer link and its key never reach NZBGet) or by handing NZBGet a URL to
// fetch, and folds listgroups (active) + history (terminal) into the gateway's
// JobView. The clientJobID is NZBGet's integer NZBID.
type NZBGet struct {
	BaseURL  string
	Username string
	Password string
	Category string
	HTTP     *http.Client
}

// LogValue keeps the password out of structured logs.
func (n *NZBGet) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("base_url", RedactURL(n.BaseURL)),
		slog.String("category", n.Category),
		slog.Bool("password_set", n.Password != ""),
	)
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
	body, err := json.Marshal(map[string]any{"method": method, "params": params, "id": 1})
	if err != nil {
		return nil, err
	}
	return n.send(ctx, method, int64(len(body)), func() io.Reader { return bytes.NewReader(body) })
}

// send posts a JSON-RPC request body of exactly length bytes; body returns a
// fresh reader each time it is called.
func (n *NZBGet) send(ctx context.Context, method string, length int64, body func() io.Reader) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.BaseURL+"/jsonrpc", body())
	if err != nil {
		return nil, err
	}
	// Set explicitly so a streamed body is sent with a Content-Length, not
	// chunked, and can be re-sent on a same-origin redirect.
	req.ContentLength = length
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(body()), nil }
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
		return nil, newStatusError("nzbget "+method, resp)
	}
	var rpc struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	limit := int64(maxResponse)
	if method == "listgroups" || method == "history" {
		limit = maxListResponse
	}
	if err := decodeCapped(resp.Body, limit, &rpc); err != nil {
		return nil, err
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("nzbget %s: %s", method, truncate(rpc.Error.Message, maxErrorDetail))
	}
	return rpc.Result, nil
}

// Add appends the NZB. NZBGet's append takes NZBContent as either base64 file
// content or a URL it fetches itself; a payload wins over the source. Returns
// the new NZBID as the clientJobID.
func (n *NZBGet) Add(ctx context.Context, j Job) (string, error) {
	name := strings.TrimSpace(j.Title)
	if name == "" {
		name = "download"
	}
	category := j.Category
	if category == "" {
		category = n.Category
	}
	// append(NZBFilename, NZBContent(URL|base64), Category, Priority, AddToTop,
	//        AddPaused, DupeKey, DupeScore, DupeMode). DupeMode must be a valid
	// enum — FORCE always adds (a request should never be silently deduped away).
	rest := []any{category, 0, false, false, "", 0, "FORCE"}
	var res json.RawMessage
	var err error
	if len(j.Payload) > 0 {
		// NZBGet requires the filename's extension to match the content.
		res, err = n.appendContent(ctx, payloadFileName(j.PayloadName, name, ".nzb"), j.Payload, rest)
	} else {
		res, err = n.call(ctx, "append", append([]any{name + ".nzb", j.Source}, rest...)...)
	}
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

// appendContent calls append with the NZB content. The body is byte for byte
// what call would marshal, but the base64 content is encoded from the payload
// while the body is sent, so a large NZB is not held again as a base64 string
// and once more as the marshalled request. Base64 needs no JSON escaping.
func (n *NZBGet) appendContent(ctx context.Context, filename string, content []byte, rest []any) (json.RawMessage, error) {
	name, err := json.Marshal(filename)
	if err != nil {
		return nil, err
	}
	tail, err := json.Marshal(rest)
	if err != nil {
		return nil, err
	}
	// Keys in the order json.Marshal gives a map: id, method, params.
	head := `{"id":1,"method":"append","params":[` + string(name) + `,"`
	end := `",` + string(tail[1:]) + `}` // tail without its opening bracket
	length := int64(len(head) + base64.StdEncoding.EncodedLen(len(content)) + len(end))
	return n.send(ctx, "append", length, func() io.Reader {
		return io.MultiReader(strings.NewReader(head), &base64Reader{src: content}, strings.NewReader(end))
	})
}

// base64Reader reads src as standard base64, encoding one chunk at a time.
type base64Reader struct {
	src []byte
	out []byte // encoded, not yet read
	buf []byte
}

// base64Chunk is a multiple of 3, so only the last chunk carries padding.
const base64Chunk = 48 << 10

func (r *base64Reader) Read(p []byte) (int, error) {
	if len(r.out) == 0 {
		if len(r.src) == 0 {
			return 0, io.EOF
		}
		n := min(len(r.src), base64Chunk)
		r.buf = base64.StdEncoding.AppendEncode(r.buf[:0], r.src[:n])
		r.out, r.src = r.buf, r.src[n:]
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
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

// foldNZBGroup maps one active-queue entry to a JobView.
func foldNZBGroup(g nzbGroup) JobView {
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
	// Everything in the queue is "not done yet", but keep the native status so a
	// console can show paused / repairing / unpacking.
	v.State = StatusDownloading
	if g.Status == "QUEUED" || g.Status == "PAUSED" {
		v.State = StatusQueued
	}
	return v
}

// foldNZBHistory maps one history entry to a terminal JobView.
func foldNZBHistory(h nzbHistory) JobView {
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
	return v
}

// Describe folds the active queue (listgroups) then history into a JobView.
//
// A transport/RPC failure must surface as an error, NOT as the "job is not
// there" answer: the gateway treats a persistently unknown job as lost and
// gives up on it, so swallowing an outage here would delete live downloads.
func (n *NZBGet) Describe(ctx context.Context, id string) (JobView, error) {
	want, _ := strconv.Atoi(id)

	res, listErr := n.call(ctx, "listgroups", 0)
	if listErr == nil {
		var groups []nzbGroup
		if listErr = json.Unmarshal(res, &groups); listErr == nil {
			for _, g := range groups {
				if g.NZBID == want {
					return foldNZBGroup(g), nil
				}
			}
		}
	}

	res, histErr := n.call(ctx, "history", false)
	if histErr == nil {
		var hist []nzbHistory
		if histErr = json.Unmarshal(res, &hist); histErr == nil {
			for _, h := range hist {
				if h.NZBID == want {
					return foldNZBHistory(h), nil
				}
			}
		}
	}

	// Only claim "not there" when we actually got both lists back.
	if listErr != nil || histErr != nil {
		err := listErr
		if err == nil {
			err = histErr
		}
		return JobView{}, fmt.Errorf("nzbget describe %s: %w", id, err)
	}

	v := NewJobView()
	v.State = StatusQueued
	v.NativeState = NotFoundState
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

// Probe asks NZBGet for its version, which proves both the endpoint and the
// control credentials.
func (n *NZBGet) Probe(ctx context.Context) (string, error) {
	res, err := n.call(ctx, "version")
	if err != nil {
		return "", err
	}
	// NZBGet answers a short token like "24.3". Anything else did not come from
	// NZBGet, and a version is echoed back to whoever ran the test.
	var version string
	if err := json.Unmarshal(res, &version); err != nil || version == "" || len(version) > 32 ||
		strings.ContainsFunc(version, func(r rune) bool { return r <= ' ' || strings.ContainsRune("<>{", r) }) {
		return "", fmt.Errorf("nzbget version: the endpoint did not answer like NZBGet")
	}
	return version, nil
}

// ClientStatus reports NZBGet's global download state, including whether any
// usenet server is actually connected — the usual cause of a stuck queue.
func (n *NZBGet) ClientStatus(ctx context.Context) ClientStatus {
	cs := ClientStatus{Name: n.Name()}
	res, err := n.call(ctx, "status")
	if err != nil {
		cs.Error = PublicError(err)
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

// Adopt re-discovers this gateway's still-running jobs after a restart: the
// groups in our category that are still in the queue. History is deliberately
// NOT adopted — those jobs already reached a terminal state, and re-adopting
// them would replay a completed/failed event on every restart.
//
// Ownership is the category, so without one we cannot tell our downloads from
// anyone else's and adopt nothing rather than hijacking the whole queue.
func (n *NZBGet) Adopt(ctx context.Context) ([]AdoptedJob, error) {
	if n.Category == "" {
		return nil, nil
	}
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
		if g.Category != n.Category {
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
