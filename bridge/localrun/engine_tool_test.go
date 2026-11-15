package localrun

import (
	"context"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Callback fixture that returns the supplied client result without executing a tool.
 */
type callbackResultFixture struct {
	sdkv1connect.UnimplementedSdkCustomToolCallbackServiceHandler
	result *structpb.Struct ///< Test-owned result returned to the run engine.
}

/**
 * @brief Return a prepared tool result to the native run.
 * @param ctx Callback lifetime.
 * @param request Native callback request.
 * @return Prepared SDK response.
 * @return Always nil.
 */
func (f *callbackResultFixture) CallCustomTool(ctx context.Context, request *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
	return connect.NewResponse(&sdkv1.CallCustomToolResponse{Result: f.result}), nil
}

/**
 * @brief Only an explicit isError flag marks a returned SDK result as failed.
 * @param t Test harness.
 */
func TestInvokeToolDoesNotInferFailureFromData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  map[string]any
		failed bool
	}{
		{"successful_data_with_error_key", map[string]any{"error": nil, "value": "ok"}, false},
		{"successful_data_with_isError_key", map[string]any{"isError": true, "value": "ok"}, false},
		{"explicit_failure", map[string]any{"isError": true, "content": []any{map[string]any{"type": "text", "text": "failed"}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := structpb.NewStruct(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			_, handler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(&callbackResultFixture{result: value})
			server := httptest.NewServer(handler)
			defer server.Close()
			engine := NewEngine(nil)
			defer engine.Close()
			engine.SetToolCallback(server.URL, "fake")
			rec := &agentRecord{customTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}
			_, failed, err := engine.invokeTool(context.Background(), "agent", rec, &agentwire.ToolCall{Name: "marker"})
			if err != nil || failed != tc.failed {
				t.Fatalf("failed = %t, error = %v; want %t", failed, err, tc.failed)
			}
		})
	}
}
