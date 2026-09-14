package adapters

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"syscall"
	"time"
)

// Pushed clients point the gateway at whatever address the pushing application
// stored. The application checks an address when it is saved, but the gateway
// is what connects, so it enforces what a saved URL cannot guarantee on its
// own: a name may resolve somewhere else later, and an endpoint may redirect.
// Environment clients are configured by whoever deploys the gateway and are
// built exactly as before.

var (
	linkLocal4    = netip.MustParsePrefix("169.254.0.0/16")
	linkLocal6    = netip.MustParsePrefix("fe80::/10")
	metadataAddrs = []netip.Addr{
		netip.MustParseAddr("100.100.100.200"),
		netip.MustParseAddr("fd00:ec2::254"),
	}
)

// RefusedAddr reports whether a pushed client must never connect to a:
// link-local ranges (which hold the common cloud metadata address) and the
// metadata addresses outside them. An IPv4-mapped IPv6 address counts as the
// IPv4 address it maps.
func RefusedAddr(a netip.Addr) bool {
	a = a.Unmap()
	if linkLocal4.Contains(a) || linkLocal6.Contains(a) {
		return true
	}
	for _, m := range metadataAddrs {
		if a == m {
			return true
		}
	}
	return false
}

// refuseDial is the dialer's Control hook. It sees the address actually being
// connected to, after DNS resolution, so no name can smuggle a request there.
func refuseDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	if a, err := netip.ParseAddr(host); err == nil && RefusedAddr(a) {
		return fmt.Errorf("refusing to connect to %s: link-local or metadata address", a)
	}
	return nil
}

// errRedirectRefused is returned for a redirect that leaves the client's origin.
var errRedirectRefused = errors.New("refusing a redirect to another host or scheme")

// sameOrigin allows a redirect only within the scheme and host (port included)
// of the first request. Go re-sends a POST body — a qBittorrent login form, with
// its password — on a 307 or 308, and a redirect from https to http would send
// the next request in the clear, so following one elsewhere hands credentials
// to wherever the endpoint points.
func sameOrigin(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	first := via[0].URL
	if !strings.EqualFold(req.URL.Scheme, first.Scheme) || !strings.EqualFold(req.URL.Host, first.Host) {
		return errRedirectRefused
	}
	return nil
}

// pushedTransport carries every pushed client's requests, test probes included.
// It ignores the proxy environment variables on purpose: through a proxy the
// dial check would see the proxy's address, not the client's.
var pushedTransport = &http.Transport{
	Proxy: nil,
	DialContext: (&net.Dialer{
		Timeout: 10 * time.Second, KeepAlive: 30 * time.Second, Control: refuseDial,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: time.Second,
}

// guard applies the pushed-client endpoint policy to an adapter's HTTP client,
// keeping its timeout and cookie jar.
func guard(c *http.Client) {
	c.Transport = pushedTransport
	c.CheckRedirect = sameOrigin
}
