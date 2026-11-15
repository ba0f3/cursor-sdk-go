package cursorbridgeruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
)

/**
 * @brief Query backend-reported usage and cost through the public SDK RPC without substituting estimates.
 * @param t Private bridge and fake public API using synthetic credentials.
 */
func TestNativeBridgeUsageRPC(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		nativeID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/agents/"), "/usage")
		if !strings.HasPrefix(nativeID, "agent-") || uuid.Validate(strings.TrimPrefix(nativeID, "agent-")) != nil {
			t.Error("usage requires the native SDK agent-UUID identity")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer fake" || !strings.HasPrefix(r.URL.Path, "/v1/agents/") || !strings.HasSuffix(r.URL.Path, "/usage") || r.URL.Query().Get("runId") != "backend-usage-id" {
			t.Error("invalid usage route, query or authentication")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"totalUsage":{"inputTokens":120,"outputTokens":8,"cacheReadTokens":100,"cacheWriteTokens":10,"totalTokens":128},"cost":{"rawCostCents":1.25,"chargedCents":0.5},"runs":[{"id":"backend-usage-id","usage":{"inputTokens":120,"outputTokens":8,"cacheReadTokens":100,"totalTokens":128}}]}`))
	}))
	defer upstream.Close()
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	server.engine.API.CloudBaseURL = upstream.URL
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx := context.Background()
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake"}}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	filter := "backend-usage-id"
	response, err := client.GetUsage(ctx, connect.NewRequest(&sdkv1.GetUsageRequest{AgentId: id, RunId: &filter}))
	if err != nil {
		t.Fatalf("usage RPC: %v", err)
	}
	u := response.Msg.GetUsage()
	if u.GetUsage().GetInputTokens() != 120 || u.GetUsage().GetCacheReadTokens() != 100 || u.GetCost().GetChargedCents() != 0.5 || len(u.GetRuns()) != 1 || u.GetRuns()[0].GetRunId() != filter || u.GetRuns()[0].Cost != nil {
		t.Fatal("usage counters, actual cost or absent run cost were changed")
	}
	filter = "run-client-only"
	if _, err := client.GetUsage(ctx, connect.NewRequest(&sdkv1.GetUsageRequest{AgentId: id, RunId: &filter})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("client-only run filter: %v", err)
	}
	if _, err := client.GetUsage(ctx, connect.NewRequest(&sdkv1.GetUsageRequest{AgentId: "unknown"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown agent usage: %v", err)
	}
	if _, err := client.CloseAgent(ctx, connect.NewRequest(&sdkv1.CloseAgentRequest{AgentId: id})); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetUsage(ctx, connect.NewRequest(&sdkv1.GetUsageRequest{AgentId: id})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("closed agent must require a fresh credential: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("invalid usage requests reached the upstream")
	}
}

/**
 * @brief Match local SDK artifact semantics and validate agent identity before responding.
 * @param t Private loopback bridge without provider network traffic.
 */
func TestNativeBridgeLocalArtifactsRPC(t *testing.T) {
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx := context.Background()
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake"}}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	response, err := client.ListArtifacts(ctx, connect.NewRequest(&sdkv1.ListArtifactsRequest{AgentId: id}))
	if err != nil || len(response.Msg.GetArtifacts()) != 0 {
		t.Fatalf("local artifacts: %v", err)
	}
	if _, err := client.ListArtifacts(ctx, connect.NewRequest(&sdkv1.ListArtifactsRequest{AgentId: "unknown"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown agent artifacts: %v", err)
	}
	stream, err := client.DownloadArtifact(ctx, connect.NewRequest(&sdkv1.DownloadArtifactRequest{AgentId: id, Path: "/etc/passwd"}))
	if err == nil {
		if stream.Receive() {
			t.Fatal("local artifact download exposed host data")
		}
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("local artifact capability error: %v", err)
	}
}
