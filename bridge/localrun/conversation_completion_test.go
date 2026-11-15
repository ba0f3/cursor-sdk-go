package localrun

import (
	"testing"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Keep running/incomplete tool metadata out of completed SDK conversation steps.
 * @param t Synthetic text and callback-start events retained in the public run journal.
 */
func TestConversationExcludesRunningToolSteps(t *testing.T) {
	var events []*sdkv1.RunStreamMessage
	appendEvent := func(kind string, payload map[string]any) {
		message, err := sdkMessage(kind, payload)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, message)
	}
	appendEvent("user", map[string]any{"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "question"}}}})
	appendEvent("assistant", map[string]any{"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "first"}}}})
	appendEvent("tool_call", map[string]any{"call_id": "pending", "name": "client-tool", "status": "running", "args": map[string]any{"value": "input"}})
	appendEvent("assistant", map[string]any{"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": " second"}}}})
	turns := conversationFromEvents(events)
	if len(turns) != 1 || len(turns[0].Turn.Steps) != 1 || turns[0].Turn.Steps[0].Message["text"] != "first second" {
		t.Fatal("running tool call became a completed step or split adjacent assistant text")
	}
	if pending := conversationFromEvents(events[2:3]); len(pending) != 0 {
		t.Fatal("an incomplete tool without a user or completed step created a synthetic conversation turn")
	}
}

/**
 * @brief Order parallel tools by first completion while preserving arguments seen only in their running metadata.
 * @param t Two callbacks started in A/B order and completed in B/A order.
 */
func TestConversationParallelToolCompletionOrder(t *testing.T) {
	var events []*sdkv1.RunStreamMessage
	for _, payload := range []map[string]any{
		{"call_id": "a", "name": "tool-a", "status": "running", "args": map[string]any{"input": "a"}},
		{"call_id": "b", "name": "tool-b", "status": "running", "args": map[string]any{"input": "b"}},
		{"call_id": "b", "name": "tool-b", "status": "completed", "result": map[string]any{"output": "b"}},
		{"call_id": "a", "name": "tool-a", "status": "error", "error": "callback failed", "is_error": true},
	} {
		message, err := sdkMessage("tool_call", payload)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, message)
	}
	turns := conversationFromEvents(events)
	if len(turns) != 1 || len(turns[0].Turn.Steps) != 2 {
		t.Fatal("completed callback metadata was dropped or duplicated")
	}
	steps := turns[0].Turn.Steps
	if steps[0].Message["call_id"] != "b" || steps[1].Message["call_id"] != "a" {
		t.Fatal("parallel tools retained start order instead of completion order")
	}
	for i, id := range []string{"b", "a"} {
		args, _ := steps[i].Message["args"].(map[string]any)
		if args["input"] != id {
			t.Fatal("completion lost arguments retained by the running callback metadata")
		}
	}
	result, _ := steps[0].Message["result"].(map[string]any)
	if result["output"] != "b" || steps[1].Message["status"] != "error" || steps[1].Message["error"] != "callback failed" || steps[1].Message["is_error"] != true {
		t.Fatal("actual completed callback results or errors were discarded")
	}
}

/**
 * @brief Merge later normalized native completion metadata into one already-completed callback step.
 * @param t A durable callback receipt followed by its correlated native tool completion.
 */
func TestConversationCorrelatedCompletionPreservesCallbackInfo(t *testing.T) {
	var events []*sdkv1.RunStreamMessage
	for _, payload := range []map[string]any{
		{"call_id": "same", "name": "client-tool", "exec_id": "actual-exec", "status": "completed", "args": map[string]any{"input": "original"}, "result": map[string]any{"answer": "actual callback"}},
		{"call_id": "same", "name": "mcp", "status": "completed", "result": map[string]any{"status": "success", "value": map[string]any{"answer": "native normalized"}}},
	} {
		message, err := sdkMessage("tool_call", payload)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, message)
	}
	turns := conversationFromEvents(events)
	if len(turns) != 1 || len(turns[0].Turn.Steps) != 1 {
		t.Fatal("one native callback became multiple conversation tools")
	}
	step := turns[0].Turn.Steps[0].Message
	args, _ := step["args"].(map[string]any)
	result, _ := step["result"].(map[string]any)
	if step["call_id"] != "same" || step["exec_id"] != "actual-exec" || args["input"] != "original" || result["status"] != "success" || step["type"] != "mcp" {
		t.Fatal("correlated completion lost callback identity, retained args or final native result")
	}
}
