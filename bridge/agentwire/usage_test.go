package agentwire

import "testing"

/**
 * @brief Classify native update envelopes without exposing their content.
 * @param t Test harness with synthetic known, unknown and malformed frames.
 */
func TestInteractionUpdateKind(t *testing.T) {
	for _, kind := range []int{1, 4, 14, 17, 99} {
		if got := InteractionUpdateKind(encodeMessage(1, encodeMessage(kind))); got != kind {
			t.Fatalf("kind=%d got=%d", kind, got)
		}
	}
	for _, frame := range [][]byte{nil, encodeMessage(4), encodeMessage(1, []byte{0x0a, 0xff})} {
		if InteractionUpdateKind(frame) != 0 {
			t.Fatal("other or malformed frame was classified as an update")
		}
	}
}

/**
 * @brief Decode the optional TurnEndedUpdate counters from the pinned SDK descriptor.
 * @param t Test harness.
 */
func TestTurnEndedUsage(t *testing.T) {
	frame := encodeMessage(1, encodeMessage(14, encodeUInt32Field(1, 100), encodeUInt32Field(2, 20), encodeUInt32Field(3, 70), encodeUInt32Field(4, 10), encodeUInt32Field(5, 5)))
	d := DecodeStreamDeltas(frame)
	if len(d) != 1 || !d[0].Done || d[0].Usage == nil {
		t.Fatal("turn usage was dropped")
	}
	u := d[0].Usage
	if u.InputTokens != 100 || u.OutputTokens != 20 || u.CacheReadTokens != 70 || u.CacheWriteTokens != 10 || u.ReasoningTokens == nil || *u.ReasoningTokens != 5 {
		t.Fatal("turn usage was corrupted")
	}
	for _, data := range [][]byte{nil, {0x0a, 0xff}, encodeMessage(1, nil)} {
		d = DecodeStreamDeltas(encodeMessage(1, encodeMessage(14, data)))
		if len(d) != 1 || !d[0].Done || d[0].Usage != nil {
			t.Fatal("missing or malformed usage must preserve completion without inventing counters")
		}
	}
}
