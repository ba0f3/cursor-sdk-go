package agentwire

import (
	"bytes"
	"testing"
)

/**
 * @brief Preserve search approval correlation without approving other interaction types.
 * @param t Test harness using the pinned SDK's protobuf field numbers.
 */
func TestReplyInteraction(t *testing.T) {
	for _, tc := range []struct {
		name    string ///< Fixture label.
		payload []byte ///< AgentServerMessage payload.
		want    []byte ///< Expected complete Connect reply, or nil.
		wantErr bool   ///< Whether boundary validation must fail.
	}{
		{"implicit zero id", encodeMessage(7, encodeMessage(2)), EncodeConnectDataFrame(encodeMessage(6, encodeUInt32Field(1, 0), encodeMessage(2, encodeMessage(1)))), false},
		{"maximum id", encodeMessage(7, encodeUInt32Field(1, ^uint32(0)), encodeMessage(2)), EncodeConnectDataFrame(encodeMessage(6, encodeUInt32Field(1, ^uint32(0)), encodeMessage(2, encodeMessage(1)))), false},
		{"unknown query", encodeMessage(7, encodeMessage(99)), nil, false},
		{"other server message", encodeMessage(1), nil, false},
		{"wrong envelope type", encodeUInt32Field(7, 1), nil, true},
		{"wrong id type", encodeMessage(7, encodeString(1, "bad"), encodeMessage(2)), nil, true},
		{"overflowing id", encodeMessage(7, append(encodeTag(1, wireVarint), encodeVarint(1<<32)...), encodeMessage(2)), nil, true},
		{"wrong query type", encodeMessage(7, encodeUInt32Field(2, 1)), nil, true},
		{"truncated query", encodeMessage(7, []byte{0x12, 0xff}), nil, true},
		{"malformed query payload", encodeMessage(7, encodeMessage(9, []byte{0x0a, 0xff})), nil, true},
		{"wrong image args type", encodeMessage(7, encodeMessage(12, encodeUInt32Field(1, 1))), nil, true},
		{"malformed image args", encodeMessage(7, encodeMessage(12, encodeMessage(1, []byte{0x0a, 0xff}))), nil, true},
		{"wrong image description type", encodeMessage(7, encodeMessage(12, encodeMessage(1, encodeUInt32Field(1, 1)))), nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply, err := ReplyInteraction(tc.payload)
			if (err != nil) != tc.wantErr || !bytes.Equal(reply, tc.want) {
				t.Fatalf("reply=%x error=%v; want reply=%x error=%t", reply, err, tc.want, tc.wantErr)
			}
		})
	}
}

/**
 * @brief Match all local interaction policies and protobuf nesting from official SDK v1.2.0.
 * @param t Test harness with synthetic descriptions and no executable host tools.
 */
func TestReplyInteractionLocalPolicies(t *testing.T) {
	for _, tc := range []struct {
		query  int    ///< InteractionQuery oneof field.
		args   []byte ///< Query payload.
		result []byte ///< Expected response payload under the matching oneof.
	}{
		{2, nil, encodeMessage(1)},
		{3, nil, encodeMessage(1, encodeMessage(3, encodeString(1, "Interactive questions are not supported in local SDK runs")))},
		{4, nil, encodeMessage(1)},
		{7, nil, encodeMessage(1, encodeMessage(1))},
		{8, nil, encodeMessage(1)},
		{9, nil, encodeMessage(1)},
		{10, nil, encodeMessage(2, encodeString(1, "PR management is only available in cloud agents"))},
		{11, nil, encodeMessage(2, encodeString(1, "MCP authentication is not supported in local SDK runs"))},
		{12, encodeMessage(1, encodeString(1, "Synthetic image description")), encodeMessage(1, encodeString(1, "Synthetic image description"))},
		{13, nil, encodeMessage(2, encodeString(1, "Environment replacement is not supported in local SDK runs"))},
		{14, nil, encodeMessage(2, encodeString(1, "Connecting GitHub is not supported in local SDK runs"))},
	} {
		for _, id := range []uint32{0, 42} {
			reply, err := ReplyInteraction(encodeMessage(7, encodeUInt32Field(1, id), encodeMessage(tc.query, tc.args)))
			want := EncodeConnectDataFrame(encodeMessage(6, encodeUInt32Field(1, id), encodeMessage(tc.query, tc.result)))
			if err != nil || !bytes.Equal(reply, want) {
				t.Errorf("query=%d id=%d reply=%x error=%v; want %x", tc.query, id, reply, err, want)
			}
		}
	}
}
