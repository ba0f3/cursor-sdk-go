package agentwire

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

/**
 * @brief Decode native MCP tool metadata without treating it as an executable callback.
 * @param t Synthetic protobuf Values, result text and image bytes.
 */
func TestSDKToolCallMCPMetadata(t *testing.T) {
	value, err := structpb.NewValue(map[string]any{"items": []any{true, nil, 2.5}, "name": "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := proto.Marshal(value)
	args := encodeMessage(2, encodeString(1, "input"), encodeBytes(2, wire))
	args = append(args, encodeString(4, "provider")...)
	args = append(args, encodeString(5, "lookup")...)
	content := encodeMessage(1, encodeMessage(1, encodeString(1, "answer")))
	content = append(content, encodeMessage(1, encodeMessage(2, encodeBytes(1, []byte{0, 0xff}), encodeString(2, "image/png")))...)
	result := encodeMessage(1, content, encodeUInt32Field(2, 1))
	metadata, err := DecodeSDKToolCall(encodeMessage(15, encodeMessage(1, args), encodeMessage(2, result)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"type": "mcp", "args": map[string]any{"providerIdentifier": "provider", "toolName": "lookup", "args": map[string]any{"input": value.AsInterface()}}, "result": map[string]any{"status": "success", "value": map[string]any{"content": []any{map[string]any{"text": map[string]any{"text": "answer"}}, map[string]any{"image": map[string]any{"data": map[string]any{"0": float64(0), "1": float64(255)}, "mimeType": "image/png"}}}, "isError": true}}}
	if !reflect.DeepEqual(metadata, want) {
		t.Fatalf("native MCP metadata differs from the pinned SDK converter: %#v", metadata)
	}
	encoded, err := structpb.NewStruct(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded.AsMap(), want) {
		t.Fatal("MCP image bytes became base64 instead of the SDK's numeric-key Struct object")
	}
}

/**
 * @brief Preserve the official distinction between shell process failure and tool transport failure.
 * @param t Synthetic failure result retaining process output and numeric defaults.
 */
func TestSDKToolCallShellFailureMetadata(t *testing.T) {
	args := encodeString(1, "synthetic command")
	failure := encodeMessage(2, encodeUInt32Field(3, 7), encodeString(5, "partial output"), encodeString(6, "process error"))
	got, err := DecodeSDKToolCall(encodeMessage(1, encodeMessage(1, args), encodeMessage(2, failure)))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	want := `{"args":{"command":"synthetic command"},"result":{"status":"success","value":{"executionTime":0,"exitCode":7,"signal":"","stderr":"process error","stdout":"partial output"}},"type":"shell"}`
	if string(data) != want {
		t.Fatalf("shell failure metadata changed: %s", data)
	}
}

/**
 * @brief Retain optional zero values and the converter's exact default and omission semantics.
 * @param t Native arguments and results which need no thinking or interaction-update envelope.
 */
func TestSDKToolCallDefaultsAndOptionalPresence(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
		want string
	}{
		{"mcp_empty", encodeMessage(15, encodeMessage(1)), `{"args":{"args":{}},"type":"mcp"}`},
		{"grep_offset", encodeMessage(5, encodeMessage(1, encodeUInt32Field(16, 0))), `{"args":{"offset":0,"pattern":""},"type":"grep"}`},
		{"task", encodeMessage(19, encodeMessage(1), encodeMessage(2, encodeMessage(1, encodeUInt32Field(4, 0)))), `{"args":{"description":"","mode":"unspecified","prompt":""},"result":{"status":"success","value":{"backgroundReason":"unspecified","durationMs":0,"isBackground":false}},"type":"task"}`},
		{"read_bytes", encodeMessage(8, encodeMessage(1), encodeMessage(2, encodeMessage(1, encodeBytes(6, []byte("opaque"))))), `{"args":{"path":""},"result":{"status":"success","value":{"content":"","fileSize":0,"totalLines":0}},"type":"read"}`},
		{"edit", encodeMessage(12, encodeMessage(1), encodeMessage(2, encodeMessage(1, encodeUInt32Field(3, 0)))), `{"args":{"path":""},"result":{"status":"success","value":{"linesAdded":0}},"type":"edit"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeSDKToolCall(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(got)
			if string(data) != tc.want {
				t.Fatalf("default or optional value changed: %s", data)
			}
		})
	}
}

/**
 * @brief Ignore explicitly unsupported metadata families while rejecting malformed known protobuf fields.
 * @param t Native metadata, unknown fields and malformed wire values; no tool callback is invoked.
 */
func TestSDKToolCallNullAndMalformedMetadata(t *testing.T) {
	for _, wire := range [][]byte{nil, encodeMessage(999, []byte{0xff}), encodeMessage(24, encodeMessage(1)), encodeMessage(1)} {
		got, err := DecodeSDKToolCall(wire)
		if err != nil || got != nil {
			t.Fatalf("explicitly absent/unsupported metadata was promoted: %#v, %v", got, err)
		}
	}
	for _, wire := range [][]byte{{0}, {0x0a, 0x02, 0xff}, encodeMessage(1, encodeUInt32Field(1, 7)), encodeMessage(15, encodeMessage(1, encodeMessage(2, encodeString(1, "bad"), encodeBytes(2, []byte{0xff}))))} {
		if _, err := DecodeSDKToolCall(wire); err == nil {
			t.Fatal("malformed known tool metadata was silently accepted")
		}
	}
}

/**
 * @brief Verify every pinned family and explicit-null variant has deterministic metadata behavior.
 * @param t Generated inventory and empty native argument/result messages.
 */
func TestSDKToolCallPinnedFamilyCoverage(t *testing.T) {
	supported := map[int]string{1: "shell", 3: "delete", 4: "glob", 5: "grep", 8: "read", 9: "updateTodos", 12: "edit", 13: "ls", 14: "readLints", 15: "mcp", 16: "semSearch", 17: "createPlan", 19: "task", 28: "generateImage", 29: "recordScreen", 30: "computerUse"}
	if len(sdkToolMetadataVariants) != 72 {
		t.Fatal("pinned metadata variants changed without converter coverage")
	}
	for tag, family := range sdkToolMetadataVariants {
		if supported[tag] != family {
			t.Fatalf("variant %d has the wrong official supported/null classification", tag)
		}
		args := []byte(nil)
		if tag == 29 {
			args = encodeUInt32Field(1, 1)
		}
		got, err := DecodeSDKToolCall(encodeMessage(tag, encodeMessage(1, args)))
		if err != nil {
			t.Fatalf("family %s: %v", family, err)
		}
		if family == "" {
			if got != nil {
				t.Fatalf("official null variant %d produced metadata", tag)
			}
		} else if got == nil || got["type"] != family {
			t.Fatalf("supported family %s was lost", family)
		}
	}
	for _, tag := range []int{3, 4, 5, 8, 9, 12, 13, 14, 15, 16, 17, 19, 28, 30} {
		if _, err := DecodeSDKToolCall(encodeMessage(tag, encodeMessage(1), encodeMessage(2, encodeMessage(1)))); err != nil {
			t.Fatalf("empty success message for tag %d cannot be decoded: %v", tag, err)
		}
	}
}

/**
 * @brief Preserve recorded computer actions as metadata with enum and optional-field normalization.
 * @param t Synthetic click, key and unsupported action variants.
 */
func TestSDKToolCallComputerMetadata(t *testing.T) {
	click := encodeMessage(2, encodeMessage(1, encodeUInt32Field(1, 3), encodeUInt32Field(2, 4)), encodeUInt32Field(2, 5), encodeUInt32Field(3, 2))
	key := encodeMessage(8, encodeString(1, "Enter"), encodeUInt32Field(3, 2))
	args := encodeMessage(2, click)
	args = append(args, encodeMessage(2, key)...)
	args = append(args, encodeMessage(2, encodeMessage(999))...)
	got, err := DecodeSDKToolCall(encodeMessage(30, encodeMessage(1, args), encodeMessage(2, encodeMessage(2, encodeString(1, "synthetic failure")))))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	want := `{"args":{"actions":[{"button":"forward","coordinate":{"x":3,"y":4},"count":2,"type":"click"},{"key":"Enter","stroke":"down","type":"key"}]},"result":{"error":{"actionCount":0,"durationMs":0,"message":"synthetic failure"},"status":"error"},"type":"computerUse"}`
	if string(data) != want {
		t.Fatalf("computer-use metadata differs from the pinned adapter: %s", data)
	}
}

/**
 * @brief Keep raw task conversation steps in protobuf JSON form rather than recursively normalizing them.
 * @param t Recorded thinking and nested todo steps with 64-bit timestamps and enum values.
 */
func TestSDKToolCallTaskConversationProtoJSON(t *testing.T) {
	thinking := encodeMessage(3, encodeString(1, "recorded thought"), encodeUInt32Field(3, 123))
	todo := encodeMessage(1, encodeMessage(1, encodeString(2, "recorded todo"), encodeUInt32Field(3, 3)))
	toolStep := encodeMessage(2, encodeMessage(9, todo))
	success := encodeMessage(1, thinking)
	success = append(success, encodeMessage(1, toolStep)...)
	got, err := DecodeSDKToolCall(encodeMessage(19, encodeMessage(1), encodeMessage(2, encodeMessage(1, success))))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	want := `{"args":{"description":"","mode":"unspecified","prompt":""},"result":{"status":"success","value":{"backgroundReason":"unspecified","conversationSteps":[{"thinkingMessage":{"startedAtMs":"123","text":"recorded thought"}},{"toolCall":{"updateTodosToolCall":{"args":{"todos":[{"content":"recorded todo","status":"TODO_STATUS_COMPLETED"}]}}}}],"isBackground":false}},"type":"task"}`
	if string(data) != want {
		t.Fatalf("task steps lost protobuf JSON semantics: %s", data)
	}
}

/**
 * @brief Match protobuf merging for repeated singular argument messages and bound recursive task metadata.
 * @param t Synthetic duplicate argument fields and nested task results.
 */
func TestSDKToolCallMergingAndBounds(t *testing.T) {
	args := encodeMessage(1, encodeString(1, "first command"))
	args = append(args, encodeMessage(1, encodeString(2, "second directory"))...)
	got, err := DecodeSDKToolCall(encodeMessage(1, args))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	if string(data) != `{"args":{"command":"first command","workingDirectory":"second directory"},"type":"shell"}` {
		t.Fatalf("duplicate singular messages did not merge: %s", data)
	}
	nested := encodeMessage(1, encodeMessage(1))
	for range 70 {
		nested = encodeMessage(19, encodeMessage(1), encodeMessage(2, encodeMessage(1, encodeMessage(1, encodeMessage(2, nested)))))
	}
	if _, err := DecodeSDKToolCall(nested); err == nil {
		t.Fatal("recursive task metadata bypassed the depth bound")
	}
	if _, err := DecodeSDKToolCall(make([]byte, (8<<20)+1)); err == nil {
		t.Fatal("oversized tool metadata was accepted")
	}
	if _, err := DecodeSDKToolCall(encodeMessage(29, encodeMessage(1))); err == nil {
		t.Fatal("unspecified recording mode diverged from the official converter's error")
	}
}

/**
 * @brief Match raw shell-output event protobuf JSON with explicit optional zero values preserved.
 * @param t Native stdout, exit and absent event payloads.
 */
func TestSDKShellOutputProtoJSON(t *testing.T) {
	for _, tc := range []struct {
		wire []byte
		want string
	}{
		{nil, `{}`},
		{encodeMessage(1, encodeString(1, "recorded stdout")), `{"case":"stdout","value":{"data":"recorded stdout"}}`},
		{encodeMessage(3, encodeUInt32Field(1, 0), encodeUInt32Field(5, 0), encodeUInt32Field(6, 0)), `{"case":"exit","value":{"abortReason":"SHELL_ABORT_REASON_UNSPECIFIED","localExecutionTimeMs":0}}`},
		{encodeMessage(4, encodeMessage(1, encodeUInt32Field(2, 0)), encodeString(2, "")), `{"case":"start","value":{"command":"","sandboxPolicy":{"networkAccess":false}}}`},
	} {
		got, err := DecodeSDKShellOutput(tc.wire)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(got)
		if string(data) != tc.want {
			t.Fatalf("shell event protobuf JSON changed: %s", data)
		}
	}
	if _, err := DecodeSDKShellOutput(encodeUInt32Field(1, 7)); err == nil {
		t.Fatal("malformed stdout message was accepted")
	}
}

/**
 * @brief Match explicit JavaScript Number conversion and reject invalid protobuf Value JSON metadata.
 * @param t Synthetic wide duration and invalid native MCP argument values.
 */
func TestSDKToolCallNumberAndValueJSONSemantics(t *testing.T) {
	duration := append(encodeTag(4, wireVarint), encodeVarint(9007199254740993)...)
	got, err := DecodeSDKToolCall(encodeMessage(19, encodeMessage(1), encodeMessage(2, encodeMessage(1, duration))))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	if !strings.Contains(string(data), `"durationMs":9007199254740992`) {
		t.Fatalf("task duration did not match the SDK's Number conversion: %s", data)
	}
	for _, value := range []*structpb.Value{{}, structpb.NewNumberValue(math.NaN()), structpb.NewNumberValue(math.Inf(1))} {
		wire, _ := proto.Marshal(value)
		args := encodeMessage(2, encodeString(1, "invalid"), encodeBytes(2, wire))
		if _, err := DecodeSDKToolCall(encodeMessage(15, encodeMessage(1, args))); err == nil {
			t.Fatal("invalid google.protobuf.Value JSON metadata was accepted")
		}
	}
	group := protowire.AppendTag(nil, 999, protowire.StartGroupType)
	group = protowire.AppendTag(group, 999, protowire.EndGroupType)
	if _, err := DecodeSDKToolCall(group); err == nil {
		t.Fatal("unknown recursive protobuf groups bypassed metadata bounds")
	}
}

/**
 * @brief Retain the pinned converter's selected grep properties and diagnostic fallback behavior.
 * @param t Native result fields whose descriptor names differ from the converter's SDK properties.
 */
func TestSDKToolCallPinnedGrepAndDiagnosticSemantics(t *testing.T) {
	match := encodeMessage(1, encodeString(1, "recorded.go"), encodeMessage(2, encodeUInt32Field(1, 7), encodeString(2, "recorded line")))
	content := encodeMessage(3, match, encodeUInt32Field(3, 12))
	workspace := encodeMessage(4, encodeString(1, "workspace"), encodeMessage(2, content))
	got, err := DecodeSDKToolCall(encodeMessage(5, encodeMessage(1), encodeMessage(2, encodeMessage(1, workspace))))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	want := `{"args":{"pattern":""},"result":{"status":"success","value":{"workspaceResults":{"workspace":{"output":{"matches":[{"file":"recorded.go"}],"totalMatches":0},"type":"content"}}}},"type":"grep"}`
	if string(data) != want {
		t.Fatalf("grep metadata was guessed from native fields instead of the pinned converter: %s", data)
	}
	got, err = DecodeSDKToolCall(encodeMessage(14, encodeMessage(1), encodeMessage(2, encodeMessage(2, encodeString(1, "native error_message")))))
	if err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(got)
	if string(data) != `{"args":{"paths":[]},"result":{"error":{"message":"error"},"status":"error"},"type":"readLints"}` {
		t.Fatalf("read-lints fallback changed: %s", data)
	}
}

/**
 * @brief Match protobuf JSON special floats and the pinned readMapEntry's last-value-wins behavior.
 * @param t Synthetic well-known JSON object values and non-finite protobuf scalar values.
 */
func TestSDKToolCallMapValuesAndProtoJSONScalars(t *testing.T) {
	for _, tc := range []struct {
		value float64
		want  string
	}{{math.Inf(1), "Infinity"}, {math.Inf(-1), "-Infinity"}, {math.NaN(), "NaN"}} {
		if got := sdkMetadataJSONScalar("1", tc.value); got != tc.want {
			t.Fatalf("protobuf JSON special float differs: %v", got)
		}
	}
	first, _ := structpb.NewStruct(map[string]any{"first": true})
	second, _ := structpb.NewStruct(map[string]any{"second": true})
	firstWire, _ := proto.Marshal(structpb.NewStructValue(first))
	secondWire, _ := proto.Marshal(structpb.NewStructValue(second))
	entry := encodeString(1, "input")
	entry = append(entry, encodeBytes(2, firstWire)...)
	entry = append(entry, encodeBytes(2, secondWire)...)
	for _, duplicateKey := range []bool{false, true} {
		args := encodeMessage(2, entry)
		if duplicateKey {
			args = append(args, encodeMessage(2, encodeString(1, "input"), encodeBytes(2, secondWire))...)
		}
		got, err := DecodeSDKToolCall(encodeMessage(15, encodeMessage(1, args)))
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(got)
		want := `{"args":{"args":{"input":{"second":true}}},"type":"mcp"}`
		if string(data) != want {
			t.Fatalf("protobuf map value/key semantics changed: %s", data)
		}
	}
}

/**
 * @brief Preserve true native BigInt provenance while leaving int32, enum and root SDK Number fields numeric.
 * @param t Native uint64/int32 sibling fields passed through the real protobuf Struct boundary.
 */
func TestSDKMetadataForStructNativeBigIntProvenance(t *testing.T) {
	sdkMetadataSchemaOnce.Do(initSDKMetadataSchema)
	wide := append(encodeTag(10, wireVarint), encodeVarint(9007199254740993)...)
	budget := 100000
	native, err := decodeSDKMetadata("ShellArgs", append(encodeUInt32Field(3, 7), wide...), 0, &budget)
	if err != nil {
		t.Fatal(err)
	}
	safe, err := SDKMetadataForStruct(map[string]any{"native": native.copy("timeout", "fileOutputThresholdBytes", "timeoutBehavior"), "stepId": int64(12), "bytes": []byte{0, 255}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := structpb.NewStruct(safe.(map[string]any))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"native": map[string]any{"timeout": float64(7), "fileOutputThresholdBytes": "9007199254740993", "timeoutBehavior": float64(0)}, "stepId": float64(12), "bytes": map[string]any{"0": float64(0), "1": float64(255)}}
	if !reflect.DeepEqual(encoded.AsMap(), want) {
		t.Fatalf("native BigInt/Uint8Array provenance changed at Struct boundary: %#v", encoded.AsMap())
	}
	negative := append(encodeTag(2, wireVarint), encodeVarint(^uint64(6))...)
	native, err = decodeSDKMetadata("RecordScreenSaveSuccess", negative, 0, &budget)
	if err != nil {
		t.Fatal(err)
	}
	safe, err = SDKMetadataForStruct(native.copy("recordingDurationMs"))
	if err != nil {
		t.Fatal(err)
	}
	if safe.(map[string]any)["recordingDurationMs"] != "-7" {
		t.Fatal("signed native BigInt was not retained as a decimal string")
	}
}

/**
 * @brief Keep legitimate image bytes separate from the ordinary decoded-field count limit.
 * @param t A synthetic image below the existing tool-image byte bound, without provider or host access.
 */
func TestSDKMetadataForStructByteExpansionBudget(t *testing.T) {
	image := bytes.Repeat([]byte{255}, 128<<10)
	safe, err := SDKMetadataForStruct(map[string]any{"data": image})
	if err != nil {
		t.Fatal("ordinary metadata count budget rejected valid bounded image bytes", err)
	}
	data := safe.(map[string]any)["data"].(map[string]any)
	if len(data) != len(image) || data["131071"] != float64(255) {
		t.Fatal("bounded byte metadata was truncated or encoded differently")
	}
}

/**
 * @brief Compare allocation-free projected Struct bytes with real protobuf encoding across map/list envelopes.
 * @param t Synthetic values, decimal-index boundaries and native BigInt/byte representations.
 */
func TestSDKMetadataStructProjectionMatchesEncoding(t *testing.T) {
	values := []any{nil, true, false, float64(0), "", "recorded text", sdkMetadataInt64(-7), map[string]any{"": nil, "list": []any{true, "", map[string]any{"nested": float64(2)}}}, []any{nil, "value"}}
	for _, length := range []int{0, 1, 9, 10, 11, 99, 100, 101, 127, 128, 999, 1000, 1001} {
		values = append(values, map[string]any{"image": bytes.Repeat([]byte{255}, length)})
	}
	for _, value := range values {
		projected, err := projectSDKMetadataStruct(value)
		if err != nil {
			t.Fatal(err)
		}
		safe, err := SDKMetadataForStruct(value)
		if err != nil {
			t.Fatal(err)
		}
		var actual proto.Message
		if object, ok := safe.(map[string]any); ok {
			actual, err = structpb.NewStruct(object)
		} else {
			actual, err = structpb.NewValue(safe)
		}
		if err != nil {
			t.Fatal(err)
		}
		if projected != int64(proto.Size(actual)) {
			t.Fatalf("Struct projection differs from protobuf encoding: projected=%d actual=%d", projected, proto.Size(actual))
		}
	}
}

/**
 * @brief Reject excessive byte-object expansion before allocating numeric-key maps while retaining full-size tool images.
 * @param t One raw buffer reused for logarithmic projection boundaries; no oversized numeric map is built.
 */
func TestSDKMetadataStructProjectionRejectsBeforeExpansion(t *testing.T) {
	image := make([]byte, 8<<20)
	if size, err := projectSDKMetadataStruct(map[string]any{"image": image[:1<<20]}); err != nil || size > 32<<20 {
		t.Fatalf("legitimate 1MiB tool image no longer fits the object bound: size=%d err=%v", size, err)
	}
	large := map[string]any{"image": image}
	var rejected bool
	allocations := testing.AllocsPerRun(1, func() { _, err := SDKMetadataForStruct(large); rejected = err != nil })
	if !rejected {
		t.Fatal("8MiB image expansion bypassed the 32MiB Struct output bound")
	}
	if allocations > 128 {
		t.Fatalf("oversized byte expansion allocated before rejection: allocations=%f", allocations)
	}
	low, high := 0, len(image)
	for low < high {
		middle := low + (high-low+1)/2
		if _, err := projectSDKMetadataStruct(map[string]any{"image": image[:middle]}); err == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	if _, err := projectSDKMetadataStruct(map[string]any{"image": image[:low]}); err != nil {
		t.Fatal("projection rejected its final fitting byte boundary")
	}
	if _, err := projectSDKMetadataStruct(map[string]any{"image": image[:low+1]}); err == nil {
		t.Fatal("projection accepted the first oversized byte boundary")
	}
}
