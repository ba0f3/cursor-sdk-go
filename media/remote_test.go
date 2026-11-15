package cursormedia

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

/**
 * @brief Verify public image destination policy.
 * @param t Test harness.
 */
func TestPublicImageDestinationPolicy(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.64.1.1", "192.168.1.1", "0.1.2.3", "::1", "fc00::1", "::ffff:127.0.0.1"} {
		if publicIP(netip.MustParseAddr(ip)) {
			t.Fatalf("private address accepted: %s", ip)
		}
	}
	if !publicIP(netip.MustParseAddr("1.1.1.1")) {
		t.Fatal("public address rejected")
	}
	for _, uri := range []string{"file:///etc/passwd", "https://user:pass@example.com/image.png", "http:///image.png"} {
		if ValidateURL(uri) == nil {
			t.Fatal("invalid image URL accepted")
		}
	}
}

/**
 * @brief Canceling a download interrupts a blocked image body and preserves its cause.
 * @param t Test harness with a private injected HTTP client.
 */
func TestFetchCancellationClosesResponse(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := fetch(ctx, server.Client(), server.URL); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("image request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("download cancellation cause lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("image download remained blocked")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("image connection remained open")
	}
}

/**
 * @brief Verify fetch bounds and private network blocking.
 * @param t Test harness.
 */
func TestFetchBoundsAndPrivateNetworkBlocking(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "image/png")
		if r.URL.Path == "/large" {
			_, _ = w.Write([]byte(strings.Repeat("x", MaxImageBytes+1)))
			return
		}
		_, _ = w.Write([]byte{137, 80, 78, 71, 13, 10, 26, 10})
	}))
	defer server.Close()
	if _, _, err := Fetch(context.Background(), server.URL); err == nil {
		t.Fatal("private-network image was fetched")
	}
	if requests.Load() != 0 {
		t.Fatal("request reached private host")
	}
	data, mime, err := fetch(context.Background(), server.Client(), server.URL)
	if err != nil || len(data) != 8 || mime != "image/png" {
		t.Fatalf("image fetch result: bytes=%d type=%q error=%v", len(data), mime, err)
	}
	if _, _, err := fetch(context.Background(), server.Client(), server.URL+"/large"); err == nil {
		t.Fatal("oversized image was accepted")
	}
}
