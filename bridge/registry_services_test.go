package cursorbridgeruntime

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
)

/**
 * @brief Verify management RPCs are wired to the registry rather than inherited unimplemented handlers.
 * @param t Test harness using a private bridge and fake credentials without upstream traffic.
 */
func TestNativeBridgeRegistryRPCs(t *testing.T) {
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx := context.Background()
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake", Name: "registry-test"}}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	info, err := client.GetAgent(ctx, connect.NewRequest(&sdkv1.GetAgentRequest{AgentId: id}))
	if err != nil || info.Msg.GetAgent().GetName() != "registry-test" {
		t.Fatalf("metadata RPC: %v", err)
	}
	if _, err := client.ArchiveAgent(ctx, connect.NewRequest(&sdkv1.ArchiveAgentRequest{AgentId: id})); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListAgents(ctx, connect.NewRequest(&sdkv1.ListAgentsRequest{}))
	if err != nil || len(listed.Msg.GetItems()) != 0 {
		t.Fatalf("archive filter RPC: %v", err)
	}
	if _, err := client.UnarchiveAgent(ctx, connect.NewRequest(&sdkv1.UnarchiveAgentRequest{AgentId: id})); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListAgents(ctx, connect.NewRequest(&sdkv1.ListAgentsRequest{}))
	if err != nil || len(listed.Msg.GetItems()) != 1 {
		t.Fatalf("unarchive RPC: %v", err)
	}
	runs, err := client.ListRuns(ctx, connect.NewRequest(&sdkv1.ListRunsRequest{AgentId: id}))
	if err != nil || len(runs.Msg.GetItems()) != 0 {
		t.Fatalf("empty run list RPC: %v", err)
	}
	if _, err := client.GetAgent(ctx, connect.NewRequest(&sdkv1.GetAgentRequest{AgentId: "unknown"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("missing agent RPC: %v", err)
	}
}

/**
 * @brief Keep CloseAgent distinct from deletion and resume the same identity after bridge restart.
 * @param t Private state root and loopback servers with fake credentials.
 */
func TestNativeBridgeResumeAfterRestartRPC(t *testing.T) {
	root := t.TempDir()
	server, err := Start(context.Background(), Options{Workspace: t.TempDir(), StateRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	created, err := client.CreateAgent(context.Background(), connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake", Name: "restart-rpc"}}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	if _, err := client.CloseAgent(context.Background(), connect.NewRequest(&sdkv1.CloseAgentRequest{AgentId: id})); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	server, err = Start(context.Background(), Options{Workspace: t.TempDir(), StateRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	client = sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	resumed, err := client.ResumeAgent(context.Background(), connect.NewRequest(&sdkv1.ResumeAgentRequest{AgentId: id, Options: &sdkv1.AgentOptions{ApiKey: "fresh-fake"}}))
	if err != nil || resumed.Msg.GetAgentId() != id {
		t.Fatalf("resume RPC: %v", err)
	}
	info, err := client.GetAgent(context.Background(), connect.NewRequest(&sdkv1.GetAgentRequest{AgentId: id}))
	if err != nil || info.Msg.GetAgent().GetName() != "restart-rpc" {
		t.Fatalf("resume lost metadata: %v", err)
	}
}
