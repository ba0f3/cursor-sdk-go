package agentwire

import (
	"testing"
)

/**
 * @brief Pin the official AgentClientMessage and UserMessageAction envelopes so malformed runs cannot pass CI.
 * @param t Test harness using the pinned SDK v1.2.0 protobuf field descriptors.
 */
func TestRunRequestUsesNativeEnvelope(t *testing.T) {
	payload := EncodeRunRequest(RunInput{ConversationID: "conversation-test", MessageID: "message-test", UserText: "hello", ModelID: "composer-2.5", RunID: "request-test"})
	field := func(data []byte, number int) Field {
		t.Helper()
		fields, err := decodeFields(data)
		if err != nil {
			t.Fatal(err)
		}
		value, ok := findField(fields, number)
		if !ok {
			t.Fatalf("missing native field %d; AgentRunRequest or user action may be wrapped incorrectly", number)
		}
		return value
	}
	run := field(payload, 1).Bytes
	if string(field(run, 25).Bytes) != "request-test" {
		t.Fatal("native per-generation request identity was discarded")
	}
	action := field(run, 2).Bytes
	userAction := field(action, 1).Bytes
	message := field(userAction, 1).Bytes
	if string(field(message, 1).Bytes) != "hello" || string(field(message, 2).Bytes) != "message-test" {
		t.Fatal("native UserMessageAction lost its nested user message")
	}
	if string(field(field(run, 9).Bytes, 1).Bytes) != "composer-2.5" {
		t.Fatal("requested model was not encoded in the native model envelope")
	}
	_ = field(run, 4) // Cursor requires the MCP envelope even when no tools are declared.
	if string(field(run, 5).Bytes) != "conversation-test" || string(field(run, 16).Bytes) != "conversation-test" {
		t.Fatal("native conversation identifiers were not preserved")
	}
	if field(run, 12).Varint != 0 || field(run, 19).Varint != 1 {
		t.Fatal("native context and inline image capabilities were not declared")
	}
}
