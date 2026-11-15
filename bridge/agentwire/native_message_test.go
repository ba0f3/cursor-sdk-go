package agentwire

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

/**
 * @brief Never execute inactive MCP oneof members, including members superseded at the outer envelope.
 * @param t Synthetic native messages; no callback or host action is performed.
 */
func TestNativeCallbackUsesActiveOneofs(t *testing.T) {
	mcp := encodeMessage(11, encodeString(5, "client-tool"))
	shell := encodeMessage(2, encodeString(1, "unexecuted shell"))
	for _, tc := range []struct {
		name string
		wire []byte
		call bool
	}{
		{"inner_replaced", encodeMessage(2, mcp, shell), false},
		{"inner_active", encodeMessage(2, shell, mcp), true},
		{"outer_replaced", append(encodeMessage(2, mcp), encodeMessage(1, encodeMessage(1, encodeString(1, "text")))...), false},
		{"outer_active", append(encodeMessage(1, encodeMessage(1, encodeString(1, "text"))), encodeMessage(2, mcp)...), true},
		{"unknown_ignored", encodeMessage(2, mcp, encodeMessage(999)), true},
		{"outer_metadata_ignored", append(encodeMessage(2, mcp), encodeMessage(8)...), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call, err := DecodeToolCall(tc.wire)
			if err != nil || (call != nil) != tc.call {
				t.Fatalf("inactive native member became executable: call=%v error=%v", call != nil, err)
			}
		})
	}
}

/**
 * @brief All native boundary decoders agree on the active outer AgentServerMessage member.
 * @param t Overlapping known members plus independent metadata.
 */
func TestNativeOuterSelectionAcrossBoundaries(t *testing.T) {
	text := encodeMessage(1, encodeMessage(1, encodeString(1, "inactive")))
	exec := encodeMessage(2, encodeUInt32Field(1, 4), encodeMessage(10))
	checkpoint := encodeMessage(3, encodeUInt32Field(1, 8))
	kv := encodeMessage(4, encodeUInt32Field(1, 9), encodeMessage(3, encodeBytes(1, []byte("key")), encodeBytes(2, []byte("value"))))
	abort := encodeMessage(5, encodeMessage(1, encodeUInt32Field(1, 7)))
	query := encodeMessage(7, encodeUInt32Field(1, 5), encodeMessage(2))
	for _, last := range []int{1, 2, 3, 4, 5, 7} {
		parts := [][]byte{text, exec, checkpoint, kv, abort, query}
		index := map[int]int{1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 7: 5}[last]
		wire := []byte{}
		for _, part := range parts {
			wire = append(wire, part...)
		}
		wire = append(wire, parts[index]...)
		wire = append(wire, encodeMessage(8)...)
		if _, ok := ExecControlID(wire); ok != (last == 2) {
			t.Fatalf("exec ID ignored outer oneof %d", last)
		}
		if _, ok := DecodeExecAbort(wire); ok != (last == 5) {
			t.Fatalf("abort ignored outer oneof %d", last)
		}
		if _, ok, err := DecodeCheckpoint(wire); err != nil || ok != (last == 3) {
			t.Fatalf("checkpoint ignored outer oneof %d: %v", last, err)
		}
		if _, ok := InteractionQuery(wire); ok != (last == 7) {
			t.Fatalf("query ignored outer oneof %d", last)
		}
		if kind := InteractionUpdateKind(wire); (kind != 0) != (last == 1) {
			t.Fatalf("interaction kind ignored outer oneof %d", last)
		}
		if deltas := DecodeStreamDeltas(wire); (len(deltas) > 0) != (last == 1) {
			t.Fatalf("text decoder ignored outer oneof %d", last)
		}
		if update, err := DecodeSDKInteractionUpdate(wire, "session"); err != nil || (update != nil) != (last == 1) {
			t.Fatalf("SDK update ignored outer oneof %d: %v", last, err)
		}
		if reply, err := ReplyInteraction(wire); err != nil || (reply != nil) != (last == 7) {
			t.Fatalf("interaction reply ignored outer oneof %d: %v", last, err)
		}
		var state ControlState
		if reply := state.ReplyControl(wire, nil); (reply != nil) != (last == 2 || last == 4) {
			t.Fatalf("control reply ignored outer oneof %d", last)
		}
		if top, _, err := ControlOperation(wire); err != nil || ((top != 0) != (last == 2 || last == 4)) {
			t.Fatalf("control diagnostic ignored outer oneof %d: %v", last, err)
		}
	}
}

/**
 * @brief Merge repeated same message members while retaining last scalar correlations and map replacements.
 * @param t Split MCP, interaction, checkpoint and KV messages with arbitrary binary identifiers.
 */
func TestNativeSameMemberMergesAndScalarOverwrites(t *testing.T) {
	first := encodeMessage(2, encodeUInt32Field(1, 1), encodeString(15, "old-exec"), encodeMessage(11, encodeString(5, "old-tool"), encodeString(3, "old-call")))
	second := encodeMessage(2, encodeUInt32Field(1, 2), encodeString(15, "new-exec"), encodeMessage(11, encodeString(5, "new-tool"), encodeString(3, "new-call")))
	call, err := DecodeToolCall(append(first, second...))
	if err != nil || call == nil || call.ID != 2 || call.ExecID != "new-exec" || call.Name != "new-tool" || call.CallID != "new-call" {
		t.Fatalf("repeated exec message did not merge: %+v %v", call, err)
	}
	wire := append(encodeMessage(1, encodeMessage(1, encodeString(1, "old"))), encodeMessage(1, encodeMessage(1, encodeString(1, "new")))...)
	if deltas := DecodeStreamDeltas(wire); len(deltas) != 1 || deltas[0].Text != "new" {
		t.Fatal("same interaction member did not merge")
	}
	checkpoint := append(encodeMessage(3, encodeUInt32Field(1, 1)), encodeMessage(3, encodeUInt32Field(2, 2))...)
	if data, ok, err := DecodeCheckpoint(checkpoint); err != nil || !ok || !bytes.Equal(data, append(encodeUInt32Field(1, 1), encodeUInt32Field(2, 2)...)) {
		t.Fatal("checkpoint message fragments did not merge")
	}
	key := []byte{0xff, 0, 0x80}
	set := encodeMessage(4, encodeUInt32Field(1, 1), encodeMessage(3, encodeBytes(1, []byte("overwritten")), encodeBytes(1, key)), encodeMessage(3, encodeBytes(2, []byte("stored"))))
	var state ControlState
	state.ReplyControl(set, nil)
	if !bytes.Equal(state.blobs[string(key)], []byte("stored")) {
		t.Fatal("split native set did not merge or last binary key was ignored")
	}
	getAfterSet := encodeMessage(4, encodeMessage(3, encodeBytes(1, []byte("inactive")), encodeBytes(2, []byte("discard"))), encodeMessage(2, encodeBytes(1, key)))
	state.ReplyControl(getAfterSet, nil)
	if _, exists := state.blobs["inactive"]; exists {
		t.Fatal("inactive KV set mutated native state")
	}
}

/**
 * @brief Reject malformed executable argument fields before callback dispatch and preserve legal empty JSON keys.
 * @param t Synthetic argument maps and invalid protobuf JSON values.
 */
func TestNativeCallbackArgumentBoundary(t *testing.T) {
	for _, args := range [][]byte{encodeUInt32Field(5, 1), encodeUInt32Field(2, 1), encodeMessage(2, encodeUInt32Field(1, 1)), encodeMessage(2, encodeString(1, "unset"), encodeMessage(2))} {
		if _, err := DecodeToolCall(encodeMessage(2, encodeMessage(11, args))); err == nil {
			t.Fatal("wrong known-field wire type or unset Value reached callback")
		}
	}
	for _, value := range []*structpb.Value{structpb.NewNumberValue(math.NaN()), structpb.NewNumberValue(math.Inf(1))} {
		data, _ := proto.Marshal(value)
		args := encodeMessage(2, encodeString(1, "number"), encodeBytes(2, data))
		if _, err := DecodeToolCall(encodeMessage(2, encodeMessage(11, args))); err == nil {
			t.Fatal("non-finite executable argument reached callback")
		}
	}
	value, _ := proto.Marshal(structpb.NewStringValue("kept"))
	args := encodeMessage(2, encodeString(1, ""), encodeBytes(2, value))
	call, err := DecodeToolCall(encodeMessage(2, encodeMessage(11, args)))
	if err != nil || call == nil || call.Args.Fields[""].GetStringValue() != "kept" {
		t.Fatal("empty JSON argument key was silently discarded")
	}
}

/**
 * @brief Merge same-member SDK tool envelopes before decoding their ordinary singular ToolCall payload.
 * @param t Split shell metadata with separate argument fields and last call identifiers.
 */
func TestNativeSDKToolEnvelopeMessageMerge(t *testing.T) {
	first := encodeMessage(1, encodeMessage(2, encodeString(1, "old-call"), encodeMessage(2, encodeMessage(1, encodeMessage(1, encodeString(1, "command"))))))
	second := encodeMessage(1, encodeMessage(2, encodeString(1, "new-call"), encodeMessage(2, encodeMessage(1, encodeMessage(1, encodeString(2, "directory"))))))
	got, err := DecodeSDKInteractionUpdate(append(first, second...), "session")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	want := `{"callId":"new-call","modelCallId":"","toolCall":{"args":{"command":"command","workingDirectory":"directory"},"type":"shell"},"type":"tool-call-started"}`
	if string(data) != want {
		t.Fatalf("split native SDK tool payload lost context: %s", data)
	}
}

/**
 * @brief Preserve last scalar IDs/paths and reject bounded native framing before dispatch.
 * @param t Synthetic control fragments, inactive oneofs and oversized/overfull envelopes.
 */
func TestNativeCorrelationsAndBounds(t *testing.T) {
	exec := encodeMessage(2, encodeUInt32Field(1, 1), encodeUInt32Field(1, 2), encodeString(15, "old"), encodeString(15, "new"), encodeMessage(2, encodeString(1, "old-command"), encodeString(1, "new-command")))
	if id, ok := ExecControlID(exec); !ok || id != 2 {
		t.Fatal("exec close would use an overwritten scalar ID")
	}
	var state ControlState
	reply := state.ReplyControl(exec, nil)
	if len(reply) < 5 {
		t.Fatal("active shell operation received no rejection")
	}
	outer, _ := findField(decodeAll(reply[5:]), 2)
	fields := decodeAll(outer.Bytes)
	id, _ := findField(fields, 1)
	execID, _ := findField(fields, 15)
	result, _ := findField(fields, 2)
	rejected, _ := findField(decodeAll(result.Bytes), 4)
	command, _ := findField(decodeAll(rejected.Bytes), 1)
	if id.Varint != 2 || string(execID.Bytes) != "new" || string(command.Bytes) != "new-command" {
		t.Fatal("control rejection used overwritten scalar correlation or arguments")
	}
	abort := encodeMessage(5, encodeMessage(1, encodeUInt32Field(1, 1)), encodeMessage(1, encodeUInt32Field(1, 2)))
	if id, ok := DecodeExecAbort(abort); !ok || id != 2 {
		t.Fatal("abort fragments did not merge with last scalar ID")
	}
	reset := append(encodeMessage(2, encodeMessage(11, encodeString(5, "discarded"))), encodeMessage(1)...)
	reset = append(reset, encodeMessage(2, encodeMessage(2))...)
	if call, err := DecodeToolCall(reset); err != nil || call != nil {
		t.Fatal("reselecting an outer member revived its superseded callback")
	}
	for _, wire := range [][]byte{make([]byte, (8<<20)+1), bytes.Repeat(encodeMessage(999), 100001), encodeMessage(2, encodeString(1, "wrong numeric ID"), encodeMessage(11)), encodeUInt32Field(2, 1)} {
		if _, err := DecodeToolCall(wire); err == nil {
			t.Fatal("malformed or unbounded callback envelope was accepted")
		}
	}
}
