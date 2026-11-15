package agentwire

import (
	"bytes"
	"encoding/json"
	"testing"
)

/**
 * @brief Forward an opaque native checkpoint and retain detached content-addressed blobs.
 * @param t Test harness with synthetic conversation state.
 */
func TestCheckpointRoundTrip(t *testing.T) {
	state := encodeString(4, "pending-call")
	checkpoint, ok, err := DecodeCheckpoint(encodeMessage(3, state))
	if err != nil || !ok || !bytes.Equal(checkpoint, state) {
		t.Fatalf("checkpoint decode: %v", err)
	}
	encoded := EncodeRunRequest(RunInput{UserText: "next", Checkpoint: checkpoint})
	outer, _ := decodeFields(encoded)
	run, _ := findField(outer, 1)
	fields, _ := decodeFields(run.Bytes)
	restored, _ := findField(fields, 1)
	if !bytes.Equal(restored.Bytes, state) {
		t.Fatal("run discarded the native checkpoint")
	}
	s := ControlState{blobs: map[string][]byte{"blob": []byte("value")}, bytes: 5}
	clone := s.Clone()
	clone.blobs["blob"][0] = 'X'
	if string(s.blobs["blob"]) != "value" || clone.bytes != 5 {
		t.Fatal("checkpoint blob clone aliases the prior run")
	}
	for _, bad := range [][]byte{encodeUInt32Field(3, 1), encodeMessage(3, []byte{0x0a, 0xff})} {
		if _, _, err := DecodeCheckpoint(bad); err == nil {
			t.Fatal("invalid checkpoint was accepted")
		}
	}
}

/**
 * @brief Preserve binary native blob identifiers through durable JSON without UTF-8 replacement.
 * @param t Synthetic non-UTF-8 identifiers; no user context or credentials.
 */
func TestControlSnapshotBinaryBlobIdentity(t *testing.T) {
	id := string([]byte{0xff, 0x00, 0x81, 0x22})
	original := ControlState{blobs: map[string][]byte{id: []byte("synthetic blob")}, bytes: 14}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored ControlState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original.blobs[id], restored.blobs[id]) {
		t.Fatal("durable JSON corrupted a binary blob identifier")
	}
}
