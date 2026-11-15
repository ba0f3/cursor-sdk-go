package localrun

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Reject explicit host/cloud execution and storage options before creating local state.
 * @param t Private engines with fake credentials and no upstream transport.
 */
func TestAgentCapabilitiesRejectUnavailableOptions(t *testing.T) {
	for name, options := range map[string]*sdkv1.AgentOptions{
		"cloud":        {Cloud: &sdkv1.CloudAgentOptions{}},
		"identity":     {AgentId: "caller-selected"},
		"mcp_server":   {McpServers: map[string]*sdkv1.McpServerConfig{"external": {}}},
		"subagents":    {Agents: map[string]*sdkv1.AgentDefinition{"worker": {}}},
		"settings":     {Local: &sdkv1.LocalAgentOptions{SettingSources: []sdkv1.SettingSource{1}}},
		"sandbox":      {Local: &sdkv1.LocalAgentOptions{SandboxOptions: &sdkv1.SandboxOptions{}}},
		"auto_review":  {Local: &sdkv1.LocalAgentOptions{AutoReview: new(true)}},
		"multi_root":   {Local: &sdkv1.LocalAgentOptions{Dirs: []string{"/one", "/two"}}},
		"custom_store": {Local: &sdkv1.LocalAgentOptions{Store: &sdkv1.LocalAgentStoreConfig{Type: "custom"}}},
		"jsonl_store":  {Local: &sdkv1.LocalAgentOptions{Store: &sdkv1.LocalAgentStoreConfig{Type: "jsonl", RootDir: "/unused"}}},
	} {
		t.Run(name, func(t *testing.T) {
			e := NewEngine(nil)
			defer e.Close()
			options.ApiKey = "fake-key"
			if _, _, err := e.CreateAgent(options); connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Fatalf("unsupported configuration was accepted: %v", err)
			}
			if len(e.agents) != 0 {
				t.Fatal("rejected configuration created an agent")
			}
			id := forceTestAgent(t, e)
			options.AgentId = ""
			if name != "identity" {
				if _, err := e.ResumeAgent(id, options); connect.CodeOf(err) != connect.CodeUnimplemented {
					t.Fatalf("resume ignored unsupported configuration: %v", err)
				}
			}
		})
	}
}

/**
 * @brief Per-send external servers must not disappear while an unrelated active run is force-canceled.
 * @param t Private engine without a usable network client.
 */
func TestSendRejectsUnavailableMCPBeforeForce(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	id := forceTestAgent(t, e)
	err := e.Send(context.Background(), id, "synthetic", nil, &sdkv1.SendOptions{McpServers: map[string]*sdkv1.McpServerConfig{"external": {}}}, func(*sdkv1.RunStreamMessage) error { return nil })
	if connect.CodeOf(err) != connect.CodeUnimplemented || len(e.snapshots) != 0 {
		t.Fatalf("unsupported per-send servers reached execution: %v", err)
	}
}
