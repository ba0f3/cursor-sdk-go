package localrun

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

/**
 * @brief Preserve typed upstream error details instead of dropping them at the Run boundary.
 * @param t Test harness using a synthetic detail with no credentials.
 */
func TestRunEndPreservesErrorDetails(t *testing.T) {
	value := wrapperspb.String("synthetic diagnostic")
	encoded, err := proto.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding} {
		payload, err := json.Marshal(map[string]any{"error": map[string]any{"code": "resource_exhausted", "message": "Error", "details": []any{map[string]any{"type": "google.protobuf.StringValue", "value": encoding.EncodeToString(encoded)}}}})
		if err != nil {
			t.Fatal(err)
		}
		frame := make([]byte, 5)
		frame[0] = 2
		binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)))
		_, err = readConnectDataFrame(bytes.NewReader(append(frame, payload...)))
		var upstream *connect.Error
		if !errors.As(err, &upstream) || upstream.Code() != connect.CodeResourceExhausted {
			t.Fatalf("wrong upstream classification: %v", err)
		}
		if len(upstream.Details()) != 1 {
			t.Fatal("upstream error details were discarded")
		}
		decoded, err := upstream.Details()[0].Value()
		if err != nil || !proto.Equal(decoded, value) {
			t.Fatal("upstream error detail was corrupted")
		}
	}
}
