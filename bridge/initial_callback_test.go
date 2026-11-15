package cursorbridgeruntime

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Authenticated startup callback with fixed typed arguments and a synthetic result.
 */
type initialCallbackFixture struct {
	sdkv1connect.UnimplementedSdkCustomToolCallbackServiceHandler
	agent  atomic.Value     ///< Expected SDK agent identity, published before Send.
	calls  atomic.Int32     ///< Number of successfully authenticated native callbacks.
	args   *structpb.Struct ///< Immutable expected arguments.
	result *structpb.Struct ///< Immutable callback result.
}

/**
 * @brief Verify initial callback authentication and correlation without executing a host tool.
 * @param _ Unused callback context.
 * @param request Native custom-tool request from the initialized bridge.
 * @return Prepared client-owned tool result.
 * @return Authentication or argument/correlation error.
 */
func (f *initialCallbackFixture) CallCustomTool(_ context.Context, request *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
	if request.Header().Get("Authorization") != "Bearer fake-initial-callback-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("initial callback authorization missing"))
	}
	if request.Msg.GetAgentId() != f.agent.Load() || request.Msg.GetToolName() != "marker" || request.Msg.GetToolCallId() != "startup-call" || !proto.Equal(request.Msg.GetArgs(), f.args) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("initial callback lost native identity or typed arguments"))
	}
	f.calls.Add(1)
	return connect.NewResponse(&sdkv1.CallCustomToolResponse{Result: f.result}), nil
}

/**
 * @brief Read one bounded native Connect data payload from a private HTTP/2 request.
 * @param reader Upstream fixture request body.
 * @return Native protobuf payload.
 * @return Framing or I/O error.
 */
func readInitialCallbackFrame(reader io.Reader) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if header[0] != 0 || size > 8<<20 {
		return nil, errors.New("invalid private native Connect frame")
	}
	data := make([]byte, int(size))
	_, err := io.ReadFull(reader, data)
	return data, err
}

/**
 * @brief Options configure the authenticated callback before publication, with no later SetToolCallback RPC.
 * @param t Private callback, native HTTP/2 upstream and authenticated SDK Connect client.
 */
func TestNativeBridgeInitialToolCallbackExecutesWithoutRegistrationRPC(t *testing.T) {
	field := func(number protowire.Number, value []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
	}
	idField := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 7)
	args, err := structpb.NewStruct(map[string]any{"payload": "synthetic-argument", "count": 2})
	if err != nil {
		t.Fatal(err)
	}
	result, err := structpb.NewStruct(map[string]any{"content": []any{map[string]any{"type": "text", "text": "callback-result"}}, "isError": false})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &initialCallbackFixture{args: args, result: result}
	_, callbackHandler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(fixture)
	callback := httptest.NewServer(callbackHandler)
	defer callback.Close()
	var acknowledged atomic.Bool
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			_, _ = w.Write(field(27, append(field(1, []byte(upstream.URL)), field(2, []byte(upstream.URL))...)))
		case "/agent.v1.AgentService/Run":
			if _, err := readInitialCallbackFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			mcp := append(field(1, []byte("marker")), field(3, []byte("startup-call"))...)
			for _, name := range []string{"payload", "count"} {
				value, err := proto.Marshal(args.Fields[name])
				if err != nil {
					t.Error(err)
					return
				}
				mcp = append(mcp, field(2, append(field(1, []byte(name)), field(2, value)...))...)
			}
			exec := append(append(append([]byte(nil), idField...), field(15, []byte("startup-exec"))...), field(11, mcp)...)
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(2, exec)))
			w.(http.Flusher).Flush()
			var reply []byte
			for {
				var err error
				reply, err = readInitialCallbackFrame(r.Body)
				if err != nil {
					if r.Context().Err() == nil {
						t.Errorf("native callback acknowledgement missing: %v", err)
					}
					return
				}
				number, _, _ := protowire.ConsumeTag(reply)
				if number == 2 {
					break
				}
				if number != 5 && number != 7 {
					t.Errorf("unexpected native control message %d", number)
					return
				}
			}
			content := field(1, field(1, field(1, field(1, []byte("callback-result")))))
			want := field(2, append(append(append([]byte(nil), idField...), field(15, []byte("startup-exec"))...), field(11, content)...))
			if !bytes.Equal(reply, want) {
				t.Error("native callback result lost content or exec correlation")
				return
			}
			closed, err := readInitialCallbackFrame(r.Body)
			if err != nil || !bytes.Equal(closed, field(5, field(1, idField))) {
				t.Errorf("native callback exec close missing: %v", err)
				return
			}
			acknowledged.Store(true)
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, field(1, field(1, []byte("done"))))))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, field(14, nil))))
			_, _ = w.Write([]byte{2, 0, 0, 0, 2, '{', '}'})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()
	server, err := Start(context.Background(), Options{Workspace: t.TempDir(), Stderr: io.Discard, ToolCallbackURL: callback.URL, ToolCallbackToken: "fake-initial-callback-token"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	server.engine.API = api
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake-initial-runtime-key", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	fixture.agent.Store(created.Msg.GetAgentId())
	stream, err := client.Send(ctx, connect.NewRequest(&sdkv1.SendRequest{AgentId: created.Msg.GetAgentId(), Message: &sdkv1.UserMessage{Text: "exercise initial callback"}}))
	if err != nil {
		t.Fatal(err)
	}
	var final string
	for stream.Receive() {
		message := stream.Msg()
		if event := message.GetSdkMessage(); event != nil && event.GetType() == "tool_call" && event.GetMessage().AsMap()["status"] == "error" {
			cancel()
			_ = stream.Close()
			t.Fatal("startup-configured callback failed before native acknowledgement")
		}
		if result := message.GetResult(); result != nil {
			final = result.GetResult().GetResult()
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("initial callback send: %v", err)
	}
	if fixture.calls.Load() != 1 || !acknowledged.Load() || final != "done" {
		t.Fatalf("initial callback incomplete: calls=%d native_ack=%v final=%q", fixture.calls.Load(), acknowledged.Load(), final)
	}
}
