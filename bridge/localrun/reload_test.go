package localrun

import (
	"bytes"
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Reload acknowledges the stateless client-tool handle without changing context or canceling work.
 * @param t Memory-only engine and synthetic active run harness.
 */
func TestReloadAgentPreservesStateAndActiveRun(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake-reload-key", Name: "retained", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"client_owned": {}}}})
	if err != nil {
		t.Fatal(err)
	}
	rec := e.agents[id]
	rec.checkpoint = []byte("retained-checkpoint")
	rec.controls = &agentwire.ControlState{}
	controls, conversationID := rec.controls, rec.conversationID
	info := proto.Clone(rec.info)
	history := newRunHistory()
	if _, err := history.append(&sdkv1.RunStreamMessage{}, false); err != nil {
		t.Fatal(err)
	}
	e.histories["retained-run"] = history
	e.snapshots["retained-run"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "retained-run"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.activeRuns["retained-run"], e.runAgents["retained-run"] = cancel, id
	for range 2 {
		if err := e.ReloadAgent(id); err != nil {
			t.Fatal(err)
		}
	}
	if e.agents[id] != rec || rec.id != id || rec.conversationID != conversationID || !bytes.Equal(rec.checkpoint, []byte("retained-checkpoint")) || rec.controls != controls || !proto.Equal(rec.info, info) {
		t.Fatal("reload changed agent identity, metadata or native context")
	}
	if _, exists := rec.customTools["client_owned"]; !exists || len(rec.customTools) != 1 || rec.apiKey != "fake-reload-key" {
		t.Fatal("reload changed client tool declarations or runtime credentials")
	}
	if e.histories["retained-run"] != history || len(history.events) != 1 || e.snapshots["retained-run"].GetAgentId() != id {
		t.Fatal("reload reset retained run history")
	}
	if ctx.Err() != nil || e.activeRuns["retained-run"] == nil || e.runAgents["retained-run"] != id {
		t.Fatal("reload canceled or detached the active run")
	}
	if !e.CancelRun("retained-run") || ctx.Err() != context.Canceled {
		t.Fatal("reload changed subsequent run cancellation")
	}
}

/**
 * @brief Reload rejects unknown agents and closed engines with stable Connect error codes.
 * @param t Isolated memory-only lifecycle harness.
 */
func TestReloadAgentLifecycleErrors(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	for _, unknown := range []string{"", "unknown"} {
		if err := e.ReloadAgent(unknown); connect.CodeOf(err) != connect.CodeNotFound {
			t.Fatalf("unknown agent error=%v", err)
		}
	}
	e.Close()
	for _, agent := range []string{id, "unknown"} {
		if err := e.ReloadAgent(agent); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("closed engine error=%v", err)
		}
	}
}
