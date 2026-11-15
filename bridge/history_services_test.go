package cursorbridgeruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Verify wait, observe, conversation and message RPCs against an actual synthetic native turn.
 * @param t Private bridge, durable directory and HTTP/2 upstream.
 */
func TestNativeBridgeHistoryRPCs(t *testing.T) {
	field := func(number protowire.Number, value []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
	}
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			_, _ = w.Write(field(27, append(field(1, []byte(upstream.URL)), field(2, []byte(upstream.URL))...)))
		case "/agent.v1.AgentService/Run":
			var header [5]byte
			if _, err := io.ReadFull(r.Body, header[:]); err != nil {
				t.Error(err)
				return
			}
			if header[0] != 0 || binary.BigEndian.Uint32(header[1:]) > 8<<20 {
				t.Error("invalid synthetic request frame")
				return
			}
			if _, err := io.CopyN(io.Discard, r.Body, int64(binary.BigEndian.Uint32(header[1:]))); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, field(1, field(1, []byte("done"))))))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, field(14, []byte{8, 100, 16, 20, 24, 70}))))
			_, _ = w.Write([]byte{2, 0, 0, 0, 2, '{', '}'})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	server.engine.API = api
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake"}}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	sent, err := client.Send(ctx, connect.NewRequest(&sdkv1.SendRequest{AgentId: id, Message: &sdkv1.UserMessage{Text: "synthetic question"}}))
	if err != nil {
		t.Fatal(err)
	}
	var events []*sdkv1.RunStreamMessage
	var runID string
	for sent.Receive() {
		event := proto.Clone(sent.Msg()).(*sdkv1.RunStreamMessage)
		events = append(events, event)
		if event.GetResult() != nil {
			runID = event.GetResult().GetRunId()
		}
	}
	if err := sent.Err(); err != nil {
		t.Fatal(err)
	}
	waited, err := client.WaitLiveRun(ctx, connect.NewRequest(&sdkv1.WaitLiveRunRequest{RunId: runID}))
	if err != nil || waited.Msg.GetResult().GetResult() != "done" || waited.Msg.GetResult().GetUsage().GetCacheReadTokens() != 70 {
		t.Fatalf("wait RPC: %v", err)
	}
	offset := "1"
	observed, err := client.ObserveRun(ctx, connect.NewRequest(&sdkv1.ObserveRunRequest{RunId: runID, AfterOffset: &offset}))
	if err != nil {
		t.Fatal(err)
	}
	index := 1
	for observed.Receive() {
		if index >= len(events) || !proto.Equal(observed.Msg(), events[index]) {
			t.Fatal("observe RPC changed replay events or offsets")
		}
		index++
	}
	if err := observed.Err(); err != nil || index != len(events) {
		t.Fatalf("observe RPC: %v", err)
	}
	conversation, err := client.GetRunConversation(ctx, connect.NewRequest(&sdkv1.GetRunConversationRequest{RunId: runID}))
	if err != nil || !strings.Contains(conversation.Msg.GetConversationJson(), "synthetic question") || !strings.Contains(conversation.Msg.GetConversationJson(), "done") {
		t.Fatalf("conversation RPC: %v", err)
	}
	messages, err := client.ListAgentMessages(ctx, connect.NewRequest(&sdkv1.ListAgentMessagesRequest{AgentId: id}))
	if err != nil || len(messages.Msg.GetMessages()) != 1 || messages.Msg.GetMessages()[0].GetUuid() != id+":0" {
		t.Fatalf("messages RPC: %v", err)
	}
}
