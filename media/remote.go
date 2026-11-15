// Package cursormedia resolves image inputs without allowing access to private services.
package cursormedia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

/**
 * @brief Maximum decoded bytes accepted for one inline or downloaded image.
 */
const MaxImageBytes = 1 << 20

/**
 * @brief Accept HTTP image origins without embedded credentials.
 * @param raw Client-supplied URL.
 * @return Invalid URL error, or nil; destination addresses are checked when dialing.
 */
func ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("image URL must use HTTP or HTTPS without embedded credentials")
	}
	return nil
}

/**
 * @brief Exclude private, loopback, shared and reserved destinations.
 * @param ip Resolved address, including IPv4-mapped IPv6.
 * @return Whether the destination can be contacted by the image downloader.
 */
func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}

/**
 * @brief Pin each connection to an address already validated as public.
 * @param ctx Download context.
 * @param network Transport network.
 * @param address Host and port supplied by the HTTP transport.
 * @return Connected socket, or nil on failure.
 * @return DNS, destination-policy or connection error; nil on success.
 */
func dialPublic(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("invalid image host")
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("cannot resolve image host")
	}
	for _, ip := range addresses {
		if !publicIP(ip) {
			return nil, errors.New("image URL resolves to a private or reserved address")
		}
	}
	dialer := net.Dialer{Timeout: 10 * time.Second}
	var last error
	for _, ip := range addresses {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}

/**
 * @brief Create a bounded image client that validates every redirected connection.
 * @return Client with DNS-pinned dialing and a fifteen-second deadline.
 */
func newPublicClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = dialPublic
	return &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 3 {
			return errors.New("too many image redirects")
		}
		return ValidateURL(req.URL.String())
	}}
}

/**
 * @brief Shared immutable downloader and connection pool for public images.
 */
var publicClient = newPublicClient()

/**
 * @brief Download one public image with bounded size and sanitized errors.
 * @param ctx Caller context; cancellation stops DNS and HTTP activity.
 * @param raw Public HTTP image URL.
 * @return Image bytes, or nil on failure.
 * @return Image MIME type, or empty on failure.
 * @return URL, download or unsupported-media error; cancellation preserves the context cause.
 */
func Fetch(ctx context.Context, raw string) ([]byte, string, error) {
	return fetch(ctx, publicClient, raw)
}

/**
 * @brief Fetch and validate image data using the selected transport.
 * @param ctx Download lifetime.
 * @param client HTTP client, replaceable only by local tests.
 * @param raw Image URL.
 * @return At most MaxImageBytes, or nil on failure.
 * @return Image MIME type, or empty on failure.
 * @return Download, status, size or MIME error; nil on success.
 */
func fetch(ctx context.Context, client *http.Client, raw string) ([]byte, string, error) {
	if err := ValidateURL(raw); err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, "", errors.New("invalid image URL")
	}
	response, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", context.Cause(ctx)
		}
		return nil, "", errors.New("cannot fetch public image URL")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("image URL returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxImageBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", context.Cause(ctx)
		}
		return nil, "", errors.New("cannot read image response")
	}
	if len(data) == 0 || len(data) > MaxImageBytes {
		return nil, "", errors.New("remote image exceeds the size limit or is empty")
	}
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if !strings.HasPrefix(media, "image/") {
		media = http.DetectContentType(data)
	}
	if !strings.HasPrefix(media, "image/") {
		return nil, "", errors.New("remote URL did not return an image")
	}
	return data, media, nil
}
