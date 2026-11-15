package cursorbridgeruntime

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
)

/**
 * @brief Reload is available through authenticated Connect RPC without replacing agent metadata.
 * @param t Private loopback bridge without upstream requests or real credentials.
 */
func TestNativeBridgeReloadAgentRPC(t *testing.T) {
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake-reload-key", Name: "retained"}}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	before, err := server.engine.AgentInfo(id)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := client.ReloadAgent(ctx, connect.NewRequest(&sdkv1.ReloadAgentRequest{AgentId: id})); err != nil {
			t.Fatalf("reload RPC: %v", err)
		}
	}
	after, err := server.engine.AgentInfo(id)
	if err != nil || !proto.Equal(before, after) {
		t.Fatalf("reload changed agent metadata: %v", err)
	}
	if _, err := client.ReloadAgent(ctx, connect.NewRequest(&sdkv1.ReloadAgentRequest{AgentId: "unknown"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown agent RPC error=%v", err)
	}
	unauthorized := sdkv1connect.NewSdkAgentServiceClient(http.DefaultClient, server.BaseURL())
	if _, err := unauthorized.ReloadAgent(ctx, connect.NewRequest(&sdkv1.ReloadAgentRequest{AgentId: id})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("unauthenticated reload error=%v", err)
	}
	server.engine.Close()
	if _, err := client.ReloadAgent(ctx, connect.NewRequest(&sdkv1.ReloadAgentRequest{AgentId: id})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("closed engine RPC error=%v", err)
	}
}
