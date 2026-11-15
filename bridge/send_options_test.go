package cursorbridgeruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Extract one synthetic native fixture field without using the production decoder.
 * @param data Fixture protobuf message.
 * @param wanted Requested field number.
 * @return Bytes and varint values, empty when absent or malformed.
 */
func fixtureNativeValue(data []byte, wanted protowire.Number) ([]byte, uint64) {
	for len(data) > 0 {
		number, kind, used := protowire.ConsumeTag(data)
		if used < 0 {
			return nil, 0
		}
		data = data[used:]
		switch kind {
		case protowire.BytesType:
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return nil, 0
			}
			if number == wanted {
				return value, 0
			}
			data = data[size:]
		case protowire.VarintType:
			value, size := protowire.ConsumeVarint(data)
			if size < 0 {
				return nil, 0
			}
			if number == wanted {
				return nil, value
			}
			data = data[size:]
		default:
			used = protowire.ConsumeFieldValue(number, kind, data)
			if used < 0 {
				return nil, 0
			}
			data = data[used:]
		}
	}
	return nil, 0
}

/**
 * @brief Honor per-send model/mode and opt-in raw delta/step callbacks while retaining SDK lifecycle updates.
 * @param t Private HTTP/2 provider and authenticated loopback SDK bridge.
 */
func TestNativeBridgeSendStreamOptions(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "callbacks"}[enabled], func(t *testing.T) {
			field := func(number protowire.Number, value []byte) []byte {
				return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
			}
			varint := func(number protowire.Number, value uint64) []byte {
				return protowire.AppendVarint(protowire.AppendTag(nil, number, protowire.VarintType), value)
			}
			var provider *httptest.Server
			provider = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/auth/exchange_user_api_key":
					_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": provider.URL})
				case "/aiserver.v1.ServerConfigService/GetServerConfig":
					_, _ = w.Write(field(27, append(field(1, []byte(provider.URL)), field(2, []byte(provider.URL))...)))
				case "/agent.v1.AgentService/Run":
					var header [5]byte
					if _, err := io.ReadFull(r.Body, header[:]); err != nil || binary.BigEndian.Uint32(header[1:]) > 8<<20 {
						t.Error("invalid native request frame")
						return
					}
					payload := make([]byte, binary.BigEndian.Uint32(header[1:]))
					if _, err := io.ReadFull(r.Body, payload); err != nil {
						t.Error(err)
						return
					}
					run, _ := fixtureNativeValue(payload, 1)
					model, _ := fixtureNativeValue(run, 9)
					modelID, _ := fixtureNativeValue(model, 1)
					parameter, _ := fixtureNativeValue(model, 3)
					parameterID, _ := fixtureNativeValue(parameter, 1)
					parameterValue, _ := fixtureNativeValue(parameter, 2)
					action, _ := fixtureNativeValue(run, 2)
					action, _ = fixtureNativeValue(action, 1)
					user, _ := fixtureNativeValue(action, 1)
					_, mode := fixtureNativeValue(user, 4)
					if string(modelID) != "override-model" || mode != 3 {
						t.Errorf("send overrides not forwarded: model=%q mode=%d", modelID, mode)
					}
					if string(parameterID) != "opaque-catalog-option" || string(parameterValue) != "opaque-value" {
						t.Error("opaque model parameters were dropped or rewritten")
					}
					w.Header().Set("Content-Type", "application/connect+proto")
					for _, update := range [][]byte{
						field(4, field(1, []byte("thinking"))),
						field(5, varint(1, 25)),
						field(10, nil), field(11, nil),
						field(1, field(1, []byte("progress"))),
						field(3, append(field(1, []byte("metadata-only")), field(2, field(15, field(1, field(5, []byte("marker")))))...)),
						field(1, field(1, []byte("done"))),
						field(14, append(varint(1, 100), varint(2, 20)...)),
					} {
						_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, update)))
					}
					_, _ = w.Write([]byte{2, 0, 0, 0, 2, '{', '}'})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			provider.EnableHTTP2 = true
			provider.StartTLS()
			defer provider.Close()
			server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = server.Close(context.Background()) }()
			api := cursorapi.NewClient(provider.Client())
			api.APIBaseURL = provider.URL
			server.engine.API = api
			client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake", Model: &sdkv1.ModelSelection{Id: "original-model"}}}))
			if err != nil {
				t.Fatal(err)
			}
			sent, err := client.Send(ctx, connect.NewRequest(&sdkv1.SendRequest{AgentId: created.Msg.GetAgentId(), Message: &sdkv1.UserMessage{Text: "synthetic question"}, Options: &sdkv1.SendOptions{Model: &sdkv1.ModelSelection{Id: "override-model", Params: []*sdkv1.ModelParameterValue{{Id: "opaque-catalog-option", Value: "opaque-value"}}}, Mode: sdkv1.AgentModeOption_AGENT_MODE_OPTION_PLAN, EnableDeltas: enabled, EnableSteps: enabled}}))
			if err != nil {
				t.Fatal(err)
			}
			raw, steps, summaries, thinkingComplete := 0, 0, 0, false
			finalText := ""
			for sent.Receive() {
				event := sent.Msg()
				if result := event.GetResult(); result != nil {
					finalText = result.GetResult().GetResult()
				}
				if update := event.GetInteractionUpdate(); update != nil {
					raw++
				}
				if step := event.GetStep(); step != nil {
					steps++
				}
				if sdk := event.GetSdkMessage(); sdk != nil {
					payload := sdk.GetMessage().AsMap()
					if sdk.GetType() == "summary" {
						summaries++
					}
					if sdk.GetType() == "thinking" && payload["thinking_duration_ms"] == float64(25) && payload["text"] == "" {
						thinkingComplete = true
					}
				}
			}
			if err := sent.Err(); err != nil {
				t.Fatal(err)
			}
			if enabled && (raw == 0 || steps != 4) || !enabled && (raw != 0 || steps != 0) {
				t.Errorf("callback options ignored: raw=%d steps=%d enabled=%t", raw, steps, enabled)
			}
			if summaries != 2 || !thinkingComplete {
				t.Errorf("missing SDK lifecycle updates: summaries=%d thinking_completed=%t", summaries, thinkingComplete)
			}
			if finalText != "done" {
				t.Errorf("SDK final result included earlier assistant steps: %q", finalText)
			}
		})
	}
}
