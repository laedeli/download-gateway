package adapters

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Response size caps. Client endpoints are outside the gateway: without a cap a
// broken or hostile one streaming an endless JSON value holds memory until the
// request times out.
const (
	maxResponse     = 1 << 20  // one object: a version, a status, an add result
	maxListResponse = 32 << 20 // job and history lists, which grow with the client's queue
	maxErrorDetail  = 256      // body excerpt kept on a StatusError
)

// decodeCapped decodes one JSON value from r, failing once the body passes
// limit bytes instead of reading on.
func decodeCapped(r io.Reader, limit int64, v any) error {
	lr := &io.LimitedReader{R: r, N: limit + 1}
	err := json.NewDecoder(lr).Decode(v)
	if err != nil && lr.N <= 0 {
		return fmt.Errorf("response exceeds %d MiB", limit>>20)
	}
	return err
}

// StatusError is a client endpoint answering with an HTTP status the adapter did
// not expect. Detail is the start of the response body, kept for logs.
type StatusError struct {
	Op     string // what was being done, e.g. "qbittorrent add"
	Code   int
	Detail string
}

func (e *StatusError) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("%s: %d", e.Op, e.Code)
	}
	return fmt.Sprintf("%s: %d %q", e.Op, e.Code, e.Detail)
}

// newStatusError builds a StatusError from resp, reading at most
// maxErrorDetail bytes of its body.
func newStatusError(op string, resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorDetail))
	return &StatusError{Op: op, Code: resp.StatusCode, Detail: strings.TrimSpace(string(b))}
}

// PublicError renders err for a response to the application that configures
// clients. The endpoint chose the body text a StatusError carries, and a test
// can aim at any address, so that text is left out; any message is cut to
// maxErrorDetail bytes and loses control characters.
func PublicError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	var se *StatusError
	if errors.As(err, &se) {
		msg = fmt.Sprintf("%s: HTTP %d", se.Op, se.Code)
	}
	return truncate(strings.Map(func(r rune) rune {
		if isControl(r) {
			return -1
		}
		return r
	}, msg), maxErrorDetail)
}

// truncate cuts s to at most n bytes without splitting a UTF-8 sequence.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
