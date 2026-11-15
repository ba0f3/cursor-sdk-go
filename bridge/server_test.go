package cursorbridgeruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
)

func TestNativeBridgePingAndVersion(t *testing.T) {
	dir := t.TempDir()
	server, err := Start(context.Background(), Options{
		Host:      "127.0.0.1",
		Port:      0,
		Workspace: dir,
		StateRoot: dir,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = server.Close(context.Background()) })

	httpClient := &http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}
	control := sdkv1connect.NewSdkBridgeControlServiceClient(httpClient, server.BaseURL())
	ping, err := control.Ping(context.Background(), connect.NewRequest(&sdkv1.PingRequest{}))
	if err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if ping.Msg.GetMessage() != "pong" {
		t.Fatalf("Ping() = %q, want pong", ping.Msg.GetMessage())
	}
	version, err := control.GetVersion(context.Background(), connect.NewRequest(&sdkv1.GetVersionRequest{}))
	if err != nil {
		t.Fatalf("GetVersion() error = %v", err)
	}
	if version.Msg.GetProtocolVersion() != ProtocolVersion {
		t.Fatalf("protocol = %q", version.Msg.GetProtocolVersion())
	}
}

/**
 * @brief Expose real account and repository results through authenticated Connect RPCs.
 * @param t Test harness with private bridge and upstream servers.
 */
func TestNativeBridgeAccountRPCs(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fake" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/me":
			_, _ = w.Write([]byte(`{"userId":42}`))
		case "/v1/repositories":
			_, _ = w.Write([]byte(`{"items":[{"url":"https://example.invalid/repository"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	server.engine.API.CloudBaseURL = upstream.URL
	client := sdkv1connect.NewSdkCursorServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	options := &sdkv1.CursorRequestOptions{ApiKey: "fake"}
	user, err := client.Me(context.Background(), connect.NewRequest(&sdkv1.MeRequest{Options: options}))
	if err != nil || user.Msg.GetUser().GetUserId() != 42 {
		t.Fatalf("account RPC failed: %v", err)
	}
	repos, err := client.ListRepositories(context.Background(), connect.NewRequest(&sdkv1.ListRepositoriesRequest{Options: options}))
	if err != nil || len(repos.Msg.GetItems()) != 1 {
		t.Fatalf("repository RPC failed: %v", err)
	}
	if _, err := client.Me(context.Background(), connect.NewRequest(&sdkv1.MeRequest{})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing account key: %v", err)
	}
	if _, err := client.Me(context.Background(), connect.NewRequest(&sdkv1.MeRequest{Options: &sdkv1.CursorRequestOptions{ApiKey: "rejected-fake"}})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("rejected account key: %v", err)
	}
}

/**
 * @brief Acknowledge Shutdown before closing the listener and rejecting new agents.
 * @param t Test harness with a private loopback server and no upstream traffic.
 */
func TestNativeBridgeShutdownRPC(t *testing.T) {
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	client := &http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}
	control := sdkv1connect.NewSdkBridgeControlServiceClient(client, server.BaseURL())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := control.Shutdown(ctx, connect.NewRequest(&sdkv1.ShutdownRequest{})); err != nil {
		t.Fatalf("shutdown acknowledgement failed: %v", err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := control.Ping(ctx, connect.NewRequest(&sdkv1.PingRequest{})); err != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Shutdown acknowledged but the listener is still running")
		case <-ticker.C:
		}
	}
	if _, _, err := server.engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"}); err == nil {
		t.Fatal("Shutdown did not close the engine")
	}
	select {
	case <-server.Done():
	case <-ctx.Done():
		t.Fatal("bridge did not report completed shutdown")
	}
	if err := server.Close(ctx); err != nil {
		t.Fatalf("repeated shutdown failed: %v", err)
	}
}

type testBridgeAuthTransport struct {
	token string
}

func (t *testBridgeAuthTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(clone)
}
