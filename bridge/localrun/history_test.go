package localrun

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Keep replay offsets exclusive, results detached and waiting cancellation local to the observer.
 * @param t Private registry and synthetic run events.
 */
func TestRunHistoryReplayAndWait(t *testing.T) {
	e := NewEngine(cursorapi.NewClient(nil))
	defer e.Close()
	agent, _, _ := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	e.snapshots["run"] = &sdkv1.RunSnapshot{RunId: "run", AgentId: agent, Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["run"] = newRunHistory()
	first, _ := sdkMessage("assistant", map[string]any{"message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "first"}}}})
	second, _ := sdkMessage("assistant", map[string]any{"message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "second"}}}})
	if _, err := e.recordRunEvent("run", first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.WaitRun(ctx, "run"); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation: %v", err)
	}
	if e.GetRun("run").GetStatus() != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING {
		t.Fatal("observer canceled the generation")
	}
	if _, err := e.recordRunEvent("run", second); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.snapshots["run"].Result = "firstsecond"
	e.snapshots["run"].Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED
	_, err := e.sealRunHistoryLocked("run")
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var events []*sdkv1.RunStreamMessage
	if err := e.ObserveRun(context.Background(), "run", "1", func(event *sdkv1.RunStreamMessage) error { events = append(events, event); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].GetOffset() != "2" || events[1].GetResult() == nil || events[2].GetDone() == nil {
		t.Fatal("exclusive replay lost or duplicated terminal events")
	}
	*events[0].Offset = "mutated"
	result, err := e.WaitRun(context.Background(), "run")
	if err != nil || result.GetResult() != "firstsecond" {
		t.Fatalf("terminal wait: %v", err)
	}
	result.Result = "mutated"
	if e.GetRun("run").GetResult() != "firstsecond" {
		t.Fatal("wait result aliases registry state")
	}
	if _, err := e.WaitRun(context.Background(), "unknown"); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown wait: %v", err)
	}
}

/**
 * @brief Recover an interrupted journal as an error result with partial conversation, never as live or successful.
 * @param t Private state directory and synthetic events.
 */
func TestRunHistoryInterruptedRecovery(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	agent, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	e.snapshots["pending"] = &sdkv1.RunSnapshot{RunId: "pending", AgentId: agent, Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["pending"] = newRunHistory()
	message, _ := sdkMessage("thinking", map[string]any{"text": "partial synthetic thought"})
	if _, err := e.recordRunEvent("pending", message); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	err = e.saveAgentLocked(e.agents[agent])
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := e.WaitRun(ctx, "pending")
	if err != nil || result.GetStatus() != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_ERROR || result.GetErrorCode() != "unavailable" {
		t.Fatalf("interrupted recovery: %v", err)
	}
	conversation, err := e.RunConversation(ctx, "pending")
	if err != nil || !strings.Contains(conversation, "partial synthetic thought") {
		t.Fatalf("partial history recovery: %v", err)
	}
	var count int
	if err := e.ObserveRun(ctx, "pending", "1", func(*sdkv1.RunStreamMessage) error { count++; return nil }); err != nil || count != 2 {
		t.Fatalf("recovered terminal replay: %v", err)
	}
}

/**
 * @brief Reserve terminal journal capacity and reject oversized events without silently truncating history.
 * @param t Private memory-only journal.
 */
func TestRunHistoryCapacity(t *testing.T) {
	h := newRunHistory()
	message, _ := sdkMessage("assistant", map[string]any{"text": strings.Repeat("x", maxRunHistoryBytes)})
	if _, err := h.append(message, false); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("history overflow: %v", err)
	}
	if h.bytes != 0 || len(h.events) != 0 {
		t.Fatal("rejected event partially modified history")
	}
}

/**
 * @brief Merge visible deltas into SDK conversation turns and retain structured tool IDs and results.
 * @param t Private registry and synthetic conversation events.
 */
func TestRunConversationAndAgentMessages(t *testing.T) {
	e := NewEngine(cursorapi.NewClient(nil))
	defer e.Close()
	agent, _, _ := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	e.snapshots["run"] = &sdkv1.RunSnapshot{RunId: "run", AgentId: agent, Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED}
	e.histories["run"] = newRunHistory()
	for _, entry := range []struct {
		kind    string
		payload map[string]any
	}{
		{"user", map[string]any{"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "question"}}}}},
		{"thinking", map[string]any{"text": "think"}},
		{"thinking", map[string]any{"text": " more"}},
		{"thinking", map[string]any{"text": "", "thinking_duration_ms": 25}},
		{"tool_call", map[string]any{"call_id": "call-1", "name": "marker", "args": map[string]any{"value": "hello"}, "status": "running"}},
		{"tool_call", map[string]any{"call_id": "call-1", "name": "marker", "result": map[string]any{"value": "world"}, "status": "completed"}},
		{"assistant", map[string]any{"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "answer"}}}}},
		{"assistant", map[string]any{"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": " complete"}}}}},
	} {
		message, _ := sdkMessage(entry.kind, entry.payload)
		if _, err := e.recordRunEvent("run", message); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	_, err := e.sealRunHistoryLocked("run")
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := e.RunConversation(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	var turns []conversationTurn
	if err := json.Unmarshal([]byte(encoded), &turns); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 1 || turns[0].Turn.UserMessage["text"] != "question" || len(turns[0].Turn.Steps) != 3 {
		t.Fatal("conversation roles or merged steps were lost")
	}
	steps := turns[0].Turn.Steps
	if steps[0].Type != "thinkingMessage" || steps[0].Message["text"] != "think more" || steps[0].Message["thinkingDurationMs"] != float64(25) || steps[1].Type != "toolCall" || steps[1].Message["type"] != "marker" || steps[1].Message["call_id"] != "call-1" || steps[1].Message["status"] != "completed" || steps[2].Message["text"] != "answer complete" {
		t.Fatal("conversation deltas or tool identity were changed")
	}
	messages, err := e.AgentMessages(agent, nil)
	if err != nil || len(messages) != 1 || messages[0].GetUuid() != agent+":0" || messages[0].GetType() != "user" {
		t.Fatalf("SDK turn wrapper: %v", err)
	}
	body := messages[0].GetMessage().AsMap()
	if _, ok := body["agentConversationTurn"]; !ok {
		t.Fatal("agent messages used the wrong SDK oneof shape")
	}
	page, err := e.AgentMessages(agent, &sdkv1.GetAgentMessagesOptions{Offset: 1})
	if err != nil || len(page) != 0 {
		t.Fatalf("turn offset: %v", err)
	}
}

/**
 * @brief Wake active observers on completion or deletion without polling or holding the registry lock during callbacks.
 * @param t Private registry and bounded observer context.
 */
func TestRunHistoryObserverWakeup(t *testing.T) {
	e := NewEngine(cursorapi.NewClient(nil))
	defer e.Close()
	agent, _, _ := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	e.snapshots["run"] = &sdkv1.RunSnapshot{RunId: "run", AgentId: agent, Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["run"] = newRunHistory()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- e.ObserveRun(ctx, "run", "", func(*sdkv1.RunStreamMessage) error { _ = e.GetRun("run"); return nil })
	}()
	e.mu.Lock()
	e.snapshots["run"].Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED
	_, err := e.sealRunHistoryLocked("run")
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	e.snapshots["pending"] = &sdkv1.RunSnapshot{RunId: "pending", AgentId: agent, Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["pending"] = newRunHistory()
	go func() { _, err := e.WaitRun(ctx, "pending"); done <- err }()
	if err := e.DeleteAgent(agent); err != nil {
		t.Fatal(err)
	}
	if err := <-done; connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("deleted observer: %v", err)
	}
}
