package agentwire

import (
	"bytes"
	"testing"
)

/**
 * @brief Identify control operations without exposing message payloads or correlation IDs.
 * @param t Test harness.
 */
func TestControlOperation(t *testing.T) {
	for _, tc := range []struct {
		frame     []byte
		top, kind int
	}{
		{encodeMessage(4, encodeUInt32Field(1, 9), encodeMessage(3, encodeString(1, "private"))), 4, 3},
		{encodeMessage(2, encodeUInt32Field(1, 9), encodeString(15, "private"), encodeMessage(36, nil)), 2, 36},
		{encodeMessage(1, encodeMessage(1, encodeString(1, "private"))), 0, 0},
	} {
		top, kind, err := ControlOperation(tc.frame)
		if err != nil || top != tc.top || kind != tc.kind {
			t.Fatalf("operation = (%d, %d); want (%d, %d)", top, kind, tc.top, tc.kind)
		}
	}
}

/**
 * @brief Reject host shell operations using the correct Cursor result variants.
 * @param t Test harness.
 */
func TestControlShellRejectionsUseMatchingWireTypes(t *testing.T) {
	for _, tc := range []struct {
		requestField, responseField, rejectedField int
	}{{2, 2, 4}, {14, 14, 5}, {52, 55, 4}} {
		request := encodeMessage(2, encodeUInt32Field(1, 7), encodeMessage(tc.requestField, encodeString(1, "command"), encodeString(2, "dir")))
		var state ControlState
		response := state.ReplyControl(request, nil)
		if len(response) < 5 {
			t.Fatalf("request %d produced no response", tc.requestField)
		}
		exec, ok := findField(decodeAll(response[5:]), 2)
		if !ok {
			t.Fatal("missing exec response")
		}
		result, ok := findField(decodeAll(exec.Bytes), tc.responseField)
		if !ok {
			t.Fatalf("request %d: missing result field %d", tc.requestField, tc.responseField)
		}
		rejected, ok := findField(decodeAll(result.Bytes), tc.rejectedField)
		if !ok {
			t.Fatalf("request %d: missing rejected variant %d", tc.requestField, tc.rejectedField)
		}
		fields := decodeAll(rejected.Bytes)
		reason, ok := findField(fields, 3)
		if !ok || reason.WireType != wireLength || len(reason.Bytes) == 0 {
			t.Fatalf("request %d: missing rejected reason", tc.requestField)
		}
	}
}

/**
 * @brief Return valid error variants for unsupported native filesystem operations.
 * @param t Test harness.
 */
func TestControlBuiltinErrorsUseMatchingWireTypes(t *testing.T) {
	for _, tc := range []struct {
		operation, errorVariant, reasonField int
	}{{3, 5, 2}, {4, 7, 2}, {5, 2, 1}, {7, 2, 2}, {8, 2, 2}, {9, 2, 2}, {16, 2, 3}, {20, 2, 2}, {23, 2, 1}} {
		request := encodeMessage(2, encodeUInt32Field(1, 7), encodeMessage(tc.operation, encodeString(1, "path"), encodeString(2, "dir")))
		var state ControlState
		response := state.ReplyControl(request, nil)
		if len(response) < 5 {
			t.Fatalf("operation %d produced no response", tc.operation)
		}
		exec, _ := findField(decodeAll(response[5:]), 2)
		result, ok := findField(decodeAll(exec.Bytes), tc.operation)
		if !ok {
			t.Fatalf("operation %d: missing result", tc.operation)
		}
		failure, ok := findField(decodeAll(result.Bytes), tc.errorVariant)
		if !ok {
			t.Fatalf("operation %d: missing error variant %d", tc.operation, tc.errorVariant)
		}
		reason, ok := findField(decodeAll(failure.Bytes), tc.reasonField)
		if !ok || reason.WireType != wireLength || len(reason.Bytes) == 0 {
			t.Fatalf("operation %d: missing error reason", tc.operation)
		}
	}
}

/**
 * @brief Match official KV semantics: retain writes, distinguish missing from empty, and isolate runs.
 * @param t Test harness.
 */
func TestControlBlobRoundTrip(t *testing.T) {
	var state, other ControlState
	request := func(set bool, key string, data []byte) []byte {
		field := 2
		args := encodeBytes(1, []byte(key))
		if set {
			field = 3
			args = append(args, encodeBytes(2, data)...)
		}
		return encodeMessage(4, encodeUInt32Field(1, 17), encodeMessage(field, args))
	}
	result := func(response []byte, field int) []Field {
		t.Helper()
		outer := decodeAll(response[5:])
		kv, ok := findField(outer, 3)
		if !ok {
			t.Fatal("missing native KV reply")
		}
		id, _ := findField(decodeAll(kv.Bytes), 1)
		if id.Varint != 17 {
			t.Fatal("KV reply lost request correlation")
		}
		value, ok := findField(decodeAll(kv.Bytes), field)
		if !ok {
			t.Fatal("missing expected KV result")
		}
		return decodeAll(value.Bytes)
	}
	if fields := result(state.ReplyControl(request(false, "missing", nil), nil), 2); len(fields) != 0 {
		t.Fatal("missing blob was reported as present empty bytes")
	}
	data := []byte("retained context and image metadata")
	if fields := result(state.ReplyControl(request(true, "key", data), nil), 3); len(fields) != 0 {
		t.Fatal("blob write failed")
	}
	fields := result(state.ReplyControl(request(false, "key", nil), nil), 2)
	value, ok := findField(fields, 1)
	if !ok || !bytes.Equal(value.Bytes, data) {
		t.Fatal("blob contents were not retained")
	}
	if fields := result(other.ReplyControl(request(false, "key", nil), nil), 2); len(fields) != 0 {
		t.Fatal("blob leaked between runs")
	}
	state.ReplyControl(request(true, "empty", nil), nil)
	if _, ok := findField(result(state.ReplyControl(request(false, "empty", nil), nil), 2), 1); !ok {
		t.Fatal("stored empty blob was treated as absent")
	}
	state.bytes = 32 << 20
	fields = result(state.ReplyControl(request(true, "overflow", []byte{1}), nil), 3)
	if _, ok := findField(fields, 1); !ok {
		t.Fatal("blob capacity failure was not returned to Cursor")
	}
}

/**
 * @brief Native image reads expose only the turn's virtual attachments, never host files.
 * @param t Test harness.
 */
func TestControlReadsOnlyAttachedImages(t *testing.T) {
	image := InputImage{Path: "/ccload/attachments/test.png", Data: []byte("image bytes"), MimeType: "image/png"}
	state := ControlState{Images: map[string]InputImage{image.Path: image}}
	for _, path := range []string{image.Path, "/etc/passwd"} {
		request := encodeMessage(2, encodeUInt32Field(1, 4), encodeString(15, "exec"), encodeMessage(7, encodeString(1, path)))
		response := state.ReplyControl(request, nil)
		exec, _ := findField(decodeAll(response[5:]), 2)
		read, _ := findField(decodeAll(exec.Bytes), 7)
		success, ok := findField(decodeAll(read.Bytes), 1)
		if path == image.Path {
			data, _ := findField(decodeAll(success.Bytes), 5)
			if !ok || !bytes.Equal(data.Bytes, image.Data) {
				t.Fatal("native image read lost attachment bytes")
			}
		} else if ok {
			t.Fatal("native read allowed a host filesystem path")
		}
	}
}
