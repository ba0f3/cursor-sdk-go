package agentwire

import (
	"bytes"
	"strings"
	"testing"
)

/**
 * @brief Exercise every pinned exec oneof member while excluding correlation metadata from operations.
 * @param t Generated descriptor inventory and synthetic frames retaining both exec correlation identifiers.
 */
func TestExecPinnedInventoryCoverage(t *testing.T) {
	operations := []int{2, 3, 4, 5, 7, 8, 9, 10, 11, 14, 16, 17, 18, 20, 21, 22, 23, 27, 28, 29, 30, 31, 36, 37, 38, 40, 41, 42, 43, 44, 52, 53, 54}
	if len(nativeExecVariants) != len(operations) {
		t.Fatal("pinned native exec inventory changed without updating explicit operation coverage")
	}
	for _, metadata := range []int{1, 15, 19, 55, 57} {
		if _, ok := nativeExecVariants[metadata]; ok {
			t.Fatalf("correlation metadata %d was classified as an operation", metadata)
		}
	}
	for _, operation := range operations {
		variant, ok := nativeExecVariants[operation]
		if !ok || variant.name == "" || !strings.Contains(nativeExecResultSchema, variant.arguments+"|") || !strings.Contains(nativeExecResultSchema, variant.resultType+"|") {
			t.Fatalf("operation %d has no resolved pinned argument/result schema", operation)
		}
		resultField := operation
		if operation == 52 {
			resultField = 55
		}
		if variant.resultField != resultField {
			t.Fatalf("operation %d has the wrong pinned result field", operation)
		}
		request := encodeMessage(2, encodeUInt32Field(1, 93), encodeString(15, "inventory"), encodeMessage(19, encodeString(1, "trace")), encodeString(57, "machine"), encodeUInt32Field(55, 1), encodeMessage(operation, nil))
		var state ControlState
		response := state.ReplyControl(request, nil)
		if operation == 11 || operation == 27 || operation == 44 {
			if response != nil {
				t.Fatalf("operation %d bypassed its callback or throw/close boundary", operation)
			}
			continue
		}
		if len(response) < 5 {
			t.Fatalf("pinned operation %d was silently dropped", operation)
		}
		exec, _ := findField(decodeAll(response[5:]), 2)
		fields := decodeAll(exec.Bytes)
		id, _ := findField(fields, 1)
		execID, _ := findField(fields, 15)
		if id.Varint != 93 || string(execID.Bytes) != "inventory" {
			t.Fatalf("operation %d lost exec correlation", operation)
		}
		if _, ok := findField(fields, resultField); !ok {
			t.Fatalf("operation %d has no matching typed result", operation)
		}
	}
}

/**
 * @brief Verify pinned nested result tags for native operations requiring unavailable local services.
 * @param t Synthetic control requests; no host resource or provider is contacted.
 */
func TestExecUnavailableResultsPreservePinnedSchemas(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		operation, branch, reason    int
		identityField, argumentField int
	}{
		{"read_mcp_resource", 18, 2, 2, 1, 2},
		{"record_screen", 21, 4, 1, 0, 0},
		{"computer_use", 22, 2, 1, 0, 0},
		{"subagent", 28, 2, 2, 0, 0},
		{"smart_mode_classifier", 38, 2, 1, 0, 0},
		{"canvas_diagnostics", 40, 2, 2, 1, 1},
		{"conversation_search", 53, 2, 1, 0, 0},
		{"agent_store_conflict", 54, 2, 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := encodeString(tc.argumentField, "resource")
			if tc.argumentField == 0 {
				args = nil
			}
			request := encodeMessage(2, encodeUInt32Field(1, 73), encodeMessage(19, encodeString(1, "trace")), encodeString(15, "exec-correlation"), encodeString(57, "machine"), encodeUInt32Field(55, 1), encodeMessage(tc.operation, args))
			var state ControlState
			response := state.ReplyControl(request, nil)
			if len(response) < 5 {
				t.Fatal("known operation has no typed unavailable result")
			}
			exec, ok := findField(decodeAll(response[5:]), 2)
			if !ok {
				t.Fatal("missing exec response envelope")
			}
			fields := decodeAll(exec.Bytes)
			id, _ := findField(fields, 1)
			execID, _ := findField(fields, 15)
			if id.Varint != 73 || string(execID.Bytes) != "exec-correlation" {
				t.Fatal("typed result lost exec correlation")
			}
			result, ok := findField(fields, tc.operation)
			if !ok {
				t.Fatal("missing pinned result field")
			}
			failure, ok := findField(decodeAll(result.Bytes), tc.branch)
			if !ok {
				t.Fatal("missing pinned unavailable branch")
			}
			detail := decodeAll(failure.Bytes)
			reason, ok := findField(detail, tc.reason)
			if !ok || reason.WireType != wireLength || len(reason.Bytes) == 0 {
				t.Fatal("missing pinned error string field")
			}
			if tc.identityField != 0 {
				identity, _ := findField(detail, tc.identityField)
				if string(identity.Bytes) != "resource" {
					t.Fatal("result lost requested resource identity")
				}
			}
		})
	}
}

/**
 * @brief Answer ownership and allowlist controls from actual no-services local state.
 * @param t Synthetic shell, subagent, resource and permission control requests.
 */
func TestExecNoServicesControlResults(t *testing.T) {
	for _, tc := range []struct {
		operation int
		want      []byte
	}{
		{30, encodeUInt32Field(1, 2)}, // ForceBackgroundShellStatus.NOT_FOUND.
		{31, encodeUInt32Field(1, 2)}, // ForceBackgroundSubagentStatus.NOT_FOUND.
		{37, encodeMessage(3, encodeString(1, "unowned-agent"))},
		{41, encodeUInt32Field(1, 0)},
		{42, encodeUInt32Field(1, 0)},
		{43, encodeUInt32Field(1, 0)},
		{17, encodeMessage(1)}, // No resource providers registered in this runtime.
	} {
		request := encodeMessage(2, encodeUInt32Field(1, 91), encodeString(15, "control"), encodeMessage(tc.operation, encodeString(1, "unowned-agent")))
		var state ControlState
		response := state.ReplyControl(request, nil)
		if len(response) < 5 {
			t.Fatalf("operation %d has no local control response", tc.operation)
		}
		exec, _ := findField(decodeAll(response[5:]), 2)
		result, ok := findField(decodeAll(exec.Bytes), tc.operation)
		if !ok || !bytes.Equal(result.Bytes, tc.want) {
			t.Fatalf("operation %d does not preserve pinned no-services semantics", tc.operation)
		}
	}
}

/**
 * @brief Keep native MCP callbacks executable only through their dedicated callback decoder.
 * @param t Known callback and failure-less result schemas requiring the engine's throw/close fallback.
 */
func TestExecCallbackAndFailurelessResultsRemainExplicitlyUnavailable(t *testing.T) {
	for _, operation := range []int{11, 27, 44, 999} {
		request := encodeMessage(2, encodeUInt32Field(1, 92), encodeString(15, "unavailable"), encodeMessage(operation, nil))
		var state ControlState
		if response := state.ReplyControl(request, nil); response != nil {
			t.Fatalf("operation %d fabricated a success or bypassed the native callback boundary", operation)
		}
	}
}
