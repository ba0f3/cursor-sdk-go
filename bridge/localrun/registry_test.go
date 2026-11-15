package localrun

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Keep agent snapshots isolated, paginate deterministically, and preserve archive filtering.
 * @param t Test harness with an isolated registry and no upstream traffic.
 */
func TestAgentRegistryLifecycle(t *testing.T) {
	e := NewEngine(cursorapi.NewClient(nil))
	defer e.Close()
	var ids []string
	for _, name := range []string{"first", "second", "third"} {
		id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Name: name})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	info, err := e.AgentInfo(ids[0])
	if err != nil || info.GetName() != "first" {
		t.Fatalf("agent metadata: %v", err)
	}
	info.Name = "mutated"
	info, _ = e.AgentInfo(ids[0])
	if info.GetName() != "first" {
		t.Fatal("returned metadata aliases registry")
	}
	first, next, err := e.ListAgents(&sdkv1.ListAgentsOptions{Limit: 2})
	if err != nil || len(first) != 2 || next == "" {
		t.Fatalf("first page: %v", err)
	}
	second, end, err := e.ListAgents(&sdkv1.ListAgentsOptions{Limit: 2, Cursor: next})
	if err != nil || len(second) != 1 || end != "" {
		t.Fatalf("second page: %v", err)
	}
	for _, a := range first {
		if a.GetAgentId() == second[0].GetAgentId() {
			t.Fatal("duplicate page entry")
		}
	}
	if err := e.ArchiveAgent(ids[0], true); err != nil {
		t.Fatal(err)
	}
	items, _, _ := e.ListAgents(nil)
	if len(items) != 2 {
		t.Fatal("archived agent was not filtered")
	}
	include := true
	items, _, _ = e.ListAgents(&sdkv1.ListAgentsOptions{IncludeArchived: &include})
	if len(items) != 3 {
		t.Fatal("include archived was ignored")
	}
	if err := e.ArchiveAgent(ids[0], false); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteAgent(ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := e.AgentInfo(ids[0]); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("deleted agent: %v", err)
	}
	if err := e.ArchiveAgent("unknown", true); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown agent: %v", err)
	}
	if _, _, err := e.ListAgents(&sdkv1.ListAgentsOptions{Cursor: "invalid"}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid cursor: %v", err)
	}
}

/**
 * @brief Reject overlapping turns before replacing an agent's checkpoint or active run ownership.
 * @param t Test harness with a synthetic active owner and no upstream traffic.
 */
func TestAgentRejectsConcurrentContextMutation(t *testing.T) {
	e := NewEngine(cursorapi.NewClient(nil))
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	e.runAgents["existing-run"] = id
	e.agents[id].checkpoint = []byte("unchanged")
	err = e.Send(context.Background(), id, "another turn", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil })
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("overlapping turn: %v", err)
	}
	if string(e.agents[id].checkpoint) != "unchanged" || e.runAgents["existing-run"] != id || len(e.snapshots) != 0 {
		t.Fatal("rejected turn modified existing conversation or lifecycle state")
	}
}

/**
 * @brief Paginate run snapshots without leaking another agent's runs or aliasing usage counters.
 * @param t Test harness with private retained snapshots.
 */
func TestRunRegistryPagination(t *testing.T) {
	e := NewEngine(cursorapi.NewClient(nil))
	defer e.Close()
	a, _, _ := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	b, _, _ := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	e.snapshots["one"] = &sdkv1.RunSnapshot{RunId: "one", AgentId: a, CreatedAt: &timestamppb.Timestamp{Seconds: 1}, Usage: &sdkv1.TokenUsage{InputTokens: 42}}
	e.snapshots["two"] = &sdkv1.RunSnapshot{RunId: "two", AgentId: a, CreatedAt: &timestamppb.Timestamp{Seconds: 2}}
	e.snapshots["other"] = &sdkv1.RunSnapshot{RunId: "other", AgentId: b, CreatedAt: &timestamppb.Timestamp{Seconds: 3}}
	items, next, err := e.ListRuns(a, &sdkv1.ListRunsOptions{Limit: 1})
	if err != nil || len(items) != 1 || next == "" {
		t.Fatalf("first run page: %v", err)
	}
	last, done, err := e.ListRuns(a, &sdkv1.ListRunsOptions{Limit: 1, Cursor: next})
	if err != nil || len(last) != 1 || done != "" || last[0].GetRunId() == items[0].GetRunId() {
		t.Fatalf("last run page: %v", err)
	}
	if _, _, err := e.ListRuns(b, &sdkv1.ListRunsOptions{Cursor: next}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("cross-agent cursor: %v", err)
	}
	last[0].Usage.InputTokens = 999
	if e.GetRun("one").GetUsage().GetInputTokens() != 42 {
		t.Fatal("run list returned shared usage")
	}
	if _, _, err := e.ListRuns(a, &sdkv1.ListRunsOptions{Limit: 1001}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("unbounded page limit: %v", err)
	}
	if err := e.DeleteAgent(a); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ListRuns(a, nil); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("deleted run owner: %v", err)
	}
}
