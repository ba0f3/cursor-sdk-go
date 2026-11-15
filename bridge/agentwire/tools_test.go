package agentwire

import (
	"bytes"
	"encoding/base64"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

/**
 * @brief Verify native tool arguments preserve jsontypes.
 * @param t Test harness.
 */
func TestNativeToolArgumentsPreserveJSONTypes(t *testing.T) {
	values := map[string]any{"text": "hello", "flag": true, "count": float64(7), "nested": map[string]any{"items": []any{"a", nil}}}
	args := [][]byte{encodeString(1, "fallback"), encodeString(5, "marker"), encodeString(3, "call-id")}
	for name, value := range values {
		v, err := structpb.NewValue(value)
		if err != nil {
			t.Fatal(err)
		}
		data, err := proto.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		args = append(args, encodeMessage(2, encodeString(1, name), encodeBytes(2, data)))
	}
	call, err := DecodeToolCall(encodeMessage(2, encodeUInt32Field(1, 4), encodeString(15, "exec-id"), encodeMessage(11, args...)))
	if err != nil || call == nil || call.Name != "marker" || call.CallID != "call-id" || call.ID != 4 || call.ExecID != "exec-id" {
		t.Fatalf("decoded call: %+v error=%v", call, err)
	}
	want, _ := structpb.NewStruct(values)
	if !proto.Equal(call.Args, want) {
		t.Fatal("tool arguments changed JSON types")
	}
}

/**
 * @brief Verify native tool image result uses image wire content.
 * @param t Test harness.
 */
func TestNativeToolImageResultUsesImageWireContent(t *testing.T) {
	raw := []byte{1, 2, 3, 4}
	reply, err := EncodeToolResult(&ToolCall{ID: 4, ExecID: "exec-id"}, map[string]any{"content": []any{map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": base64.StdEncoding.EncodeToString(raw)}}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	nested := func(data []byte, number int) []byte {
		t.Helper()
		f, err := decodeFields(data)
		if err != nil {
			t.Fatal(err)
		}
		value, ok := findField(f, number)
		if !ok {
			t.Fatalf("missing field %d", number)
		}
		return value.Bytes
	}
	exec := nested(reply[5:], 2)
	result := nested(exec, 11)
	success := nested(result, 1)
	item := nested(success, 1)
	image := nested(item, 2)
	if !bytes.Equal(nested(image, 1), raw) || string(nested(image, 2)) != "image/png" {
		t.Fatal("image did not remain native image bytes")
	}
	_, err = EncodeToolResult(&ToolCall{}, map[string]any{"content": []any{map[string]any{"type": "image", "data": "invalid", "mimeType": "image/png"}}}, false)
	if err == nil {
		t.Fatal("malformed image was accepted")
	}
}

/**
 * @brief Preserve ordered Responses input_text blocks in a native tool result.
 * @param t Test harness.
 */
func TestNativeToolResponsesTextBlocks(t *testing.T) {
	value := map[string]any{"content": []any{
		map[string]any{"type": "input_text", "text": "first"},
		map[string]any{"type": "input_text", "text": "second"},
	}}
	reply, err := EncodeToolResult(&ToolCall{ID: 4, ExecID: "exec-id"}, value, false)
	if err != nil {
		t.Fatal(err)
	}
	fields := decodeAll(reply[5:])
	exec, _ := findField(fields, 2)
	result, _ := findField(decodeAll(exec.Bytes), 11)
	success, _ := findField(decodeAll(result.Bytes), 1)
	blocks := decodeAll(success.Bytes)
	if len(blocks) != 2 {
		t.Fatalf("got %d content blocks; want 2", len(blocks))
	}
	for i, want := range []string{"first", "second"} {
		text, _ := findField(decodeAll(blocks[i].Bytes), 1)
		if got := decodeStringField(text.Bytes, 1); got != want {
			t.Fatalf("block %d = %q; want %q", i, got, want)
		}
	}
}

/**
 * @brief Preserve MCP structured content and metadata beside text blocks.
 * @param t Test harness.
 */
func TestNativeToolStructuredContent(t *testing.T) {
	value := map[string]any{
		"content":           []any{map[string]any{"type": "text", "text": "result"}},
		"structuredContent": map[string]any{"answer": float64(42)},
		"_meta":             map[string]any{"source": "fixture"},
	}
	reply, err := EncodeToolResult(&ToolCall{}, value, false)
	if err != nil {
		t.Fatal(err)
	}
	exec, _ := findField(decodeAll(reply[5:]), 2)
	result, _ := findField(decodeAll(exec.Bytes), 11)
	success, _ := findField(decodeAll(result.Bytes), 1)
	for _, tc := range []struct {
		number int
		key    string
		want   any
	}{{3, "answer", float64(42)}, {5, "source", "fixture"}} {
		field, ok := findField(decodeAll(success.Bytes), tc.number)
		if !ok {
			t.Fatalf("missing MCP field %d", tc.number)
		}
		var decoded structpb.Struct
		if err := proto.Unmarshal(field.Bytes, &decoded); err != nil || decoded.AsMap()[tc.key] != tc.want {
			t.Fatalf("invalid MCP field %d: %v", tc.number, err)
		}
	}
}
