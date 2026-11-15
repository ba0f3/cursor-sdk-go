package cursorapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

func TestServerConfigUsesUnaryConnectContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/proto" {
			t.Errorf("content type = %q", got)
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := NewClient(server.Client())
	client.APIBaseURL = server.URL
	_, err := client.fetchAgentURLs(context.Background(), "fake")
	if err == nil || err.Error() != "cursor server config returned HTTP 503" {
		t.Fatalf("error = %v", err)
	}
}

/**
 * @brief Preserve absent versus explicitly empty native tool headers over HTTP/2.
 * @param t Private upstream harness with fake credentials.
 */
func TestRunStreamToolPolicyHeaders(t *testing.T) {
	for _, tc := range []struct {
		name              string
		policy            agentwire.ToolPolicy
		present           bool
		allowed, excluded string
	}{
		{"default", agentwire.ToolPolicy{}, false, "", ""},
		{"none", agentwire.ToolPolicy{Allowed: []string{}}, true, "", ""},
		{"restricted", agentwire.ToolPolicy{Allowed: []string{"mcp_tool_call"}, Excluded: []string{"shell_tool_call"}}, true, "mcp_tool_call", "shell_tool_call"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, present := r.Header[http.CanonicalHeaderKey("x-cursor-agent-allowed-tools")]
				if present != tc.present || r.Header.Get("x-cursor-agent-allowed-tools") != tc.allowed || r.Header.Get("x-cursor-agent-exclude-tools") != tc.excluded {
					t.Error("native tool policy headers were changed")
				}
				if _, err := io.ReadFull(r.Body, make([]byte, 6)); err != nil {
					t.Error(err)
					return
				}
				w.Header().Set("Content-Type", "application/connect+proto")
				_, _ = w.Write([]byte{2, 0, 0, 0, 2, '{', '}'})
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			body, writer, err := NewClient(server.Client()).OpenRunStream(ctx, server.URL, "fake", []byte{1}, tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = body.Close(); _ = writer.Close() }()
			if _, err := io.ReadAll(body); err != nil {
				t.Fatal(err)
			}
		})
	}
}

/**
 * @brief Cancel a run waiting for response headers and stop its heartbeat sender.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunStreamCancellationBeforeHeaders(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 6)
		if _, err := io.ReadFull(r.Body, buf); err != nil {
			t.Error(err)
			return
		}
		close(started)
		<-r.Context().Done()
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := NewClient(server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := client.OpenRunStream(ctx, server.URL, "fake", []byte{1}, agentwire.ToolPolicy{})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive the first frame")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled run unexpectedly opened")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled run or heartbeat sender remained blocked")
	}
}
