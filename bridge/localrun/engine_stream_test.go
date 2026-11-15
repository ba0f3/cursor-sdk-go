package localrun

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"connectrpc.com/connect"
)

func TestConnectEndFrame(t *testing.T) {
	for _, tc := range []struct {
		body string
		want string
	}{{`{}`, ""}, {`{"error":{"code":"invalid_argument","message":"bad request"}}`, "bad request"}} {
		header := []byte{2, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(header[1:], uint32(len(tc.body)))
		_, err := readConnectDataFrame(bytes.NewReader(append(header, []byte(tc.body)...)))
		if tc.want == "" && !errors.Is(err, io.EOF) {
			t.Fatalf("successful end: %v", err)
		}
		if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Fatalf("error end: %v", err)
		}
	}
	if _, err := readConnectDataFrame(bytes.NewReader(nil)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated stream: %v", err)
	}
}

/**
 * @brief Preserve upstream error codes so auth, quota and transient failures remain distinguishable.
 * @param t Test harness.
 */
func TestUpstreamErrorRetainsConnectCode(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeResourceExhausted, connect.CodeUnauthenticated, connect.CodeInvalidArgument, connect.CodeUnavailable, connect.CodeDeadlineExceeded} {
		body := fmt.Sprintf(`{"error":{"code":%q,"message":"Error"}}`, code.String())
		header := []byte{2, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
		_, err := readConnectDataFrame(bytes.NewReader(append(header, []byte(body)...)))
		if connect.CodeOf(err) != code {
			t.Fatalf("code = %v, want %v; error = %v", connect.CodeOf(err), code, err)
		}
	}
}
