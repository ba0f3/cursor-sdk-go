package agentwire

import (
	"testing"
)

/**
 * @brief Match the pinned SDK's normalized interaction callback vocabulary and lifecycle values.
 * @param t Synthetic native protobuf frames, without executable tools or provider access.
 */
func TestNormalizedSDKInteractionUpdates(t *testing.T) {
	for _, test := range []struct {
		field int
		body  []byte
		kind  string
		key   string
		value any
	}{
		{1, encodeString(1, "visible"), "text-delta", "text", "visible"},
		{4, encodeString(1, "reasoning"), "thinking-delta", "text", "reasoning"},
		{5, encodeUInt32Field(1, 25), "thinking-completed", "thinkingDurationMs", int32(25)},
		{8, encodeUInt32Field(1, 42), "token-delta", "tokens", int32(42)},
		{9, encodeString(1, "summary"), "summary", "summary", "summary"},
		{10, nil, "summary-started", "type", "summary-started"},
		{11, encodeUInt32Field(2, 1), "summary-completed", "failed", true},
		{16, encodeUInt32Field(1, 8), "step-started", "stepId", uint64(8)},
		{17, append(encodeUInt32Field(1, 8), encodeUInt32Field(2, 12)...), "step-completed", "stepDurationMs", int64(12)},
		{27, encodeUInt32Field(1, 2), "tool-requests-listed", "callCount", uint32(2)},
	} {
		update, err := DecodeSDKInteractionUpdate(encodeMessage(1, encodeMessage(test.field, test.body)), "session")
		if err != nil || update["type"] != test.kind || update[test.key] != test.value {
			t.Fatalf("variant=%d kind=%v value=%v error=%v", test.field, update["type"], update[test.key], err)
		}
	}
	user, err := DecodeSDKInteractionUpdate(encodeMessage(1, encodeMessage(6, encodeMessage(1, encodeString(1, "question")))), "session")
	if err != nil {
		t.Fatal(err)
	}
	message, _ := user["userMessage"].(map[string]any)
	if user["type"] != "user-message-appended" || message["session_id"] != "session" || message["text"] != "question" {
		t.Fatal("native user callback lost its session or text")
	}
}

/**
 * @brief Expose only the nested task updates supported by the official SDK and reject malformed recursion.
 * @param t Synthetic task delta wrappers and non-exposable heartbeat events.
 */
func TestNormalizedSDKNestedTaskUpdates(t *testing.T) {
	nested := encodeMessage(1, encodeString(1, "child text"))
	task := encodeMessage(2, encodeMessage(1, nested))
	frame := encodeMessage(1, encodeMessage(15, encodeString(1, "call"), encodeMessage(2, task), encodeString(3, "model-call")))
	update, err := DecodeSDKInteractionUpdate(frame, "session")
	if err != nil || update["type"] != "tool-call-delta" || update["callId"] != "call" {
		t.Fatalf("nested callback: %v", err)
	}
	child, _ := update["taskUpdate"].(map[string]any)
	if child["text"] != "child text" {
		t.Fatal("nested task text was lost")
	}
	ignored, err := DecodeSDKInteractionUpdate(encodeMessage(1, encodeMessage(13)), "session")
	if err != nil || ignored != nil {
		t.Fatal("native heartbeats became synthetic SDK updates")
	}
	if _, err := DecodeSDKInteractionUpdate([]byte{0x0a, 0x80}, "session"); err == nil {
		t.Fatal("malformed callback envelope was accepted")
	}
}

/**
 * @brief Respect protobuf oneof overwrite semantics and reject wrong known-field wire types.
 * @param t Synthetic conflicting or malformed native updates.
 */
func TestSDKInteractionWireAndOneof(t *testing.T) {
	frame := encodeMessage(1, encodeMessage(1, encodeString(1, "discarded")), encodeMessage(4, encodeString(1, "last")))
	update, err := DecodeSDKInteractionUpdate(frame, "session")
	if err != nil || update["type"] != "thinking-delta" || update["text"] != "last" {
		t.Fatalf("wrong normalized oneof: %v", err)
	}
	deltas := DecodeStreamDeltas(frame)
	if len(deltas) != 1 || deltas[0].Thinking != "last" || deltas[0].Text != "" {
		t.Fatal("SDK and primary text decoders disagree about the active oneof")
	}
	for _, malformed := range [][]byte{
		encodeMessage(1, encodeMessage(1, encodeUInt32Field(1, 1))),
		encodeMessage(1, encodeMessage(5, encodeString(1, "bad"))),
	} {
		if _, err := DecodeSDKInteractionUpdate(malformed, "session"); err == nil {
			t.Fatal("wrong known native field wire type was accepted")
		}
	}
}
