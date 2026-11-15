package localrun

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Retain a partially generated SDK step when the native stream errors or is canceled.
 * @param t Private HTTP/2 provider without executable tools or real credentials.
 */
func TestSDKPendingStepFlushesOnFailure(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			var provider *httptest.Server
			provider = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/auth/exchange_user_api_key":
					_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": provider.URL})
				case "/aiserver.v1.ServerConfigService/GetServerConfig":
					_, _ = w.Write(fixtureField(27, append(fixtureField(1, []byte(provider.URL)), fixtureField(2, []byte(provider.URL))...)))
				case "/agent.v1.AgentService/Run":
					if _, err := readConnectDataFrame(r.Body); err != nil {
						t.Error(err)
						return
					}
					w.Header().Set("Content-Type", "application/connect+proto")
					_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(4, fixtureField(1, []byte("partial thinking"))))))
					w.(http.Flusher).Flush()
					if canceled {
						<-r.Context().Done()
						return
					}
					body := []byte(`{"error":{"code":"unavailable","message":"synthetic failure"}}`)
					frame := agentwire.EncodeConnectDataFrame(body)
					frame[0] = 2
					_, _ = w.Write(frame)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			provider.EnableHTTP2 = true
			provider.StartTLS()
			defer provider.Close()
			api := cursorapi.NewClient(provider.Client())
			api.APIBaseURL = provider.URL
			e := NewEngine(api)
			defer e.Close()
			id := forceTestAgent(t, e)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			steps := 0
			err := e.Send(ctx, id, "synthetic", nil, &sdkv1.SendOptions{EnableSteps: true}, func(event *sdkv1.RunStreamMessage) error {
				if sdk := event.GetSdkMessage(); sdk != nil && sdk.GetType() == "thinking" && canceled {
					cancel()
				}
				if event.GetStep() != nil {
					steps++
				}
				return nil
			})
			if err == nil || steps != 1 {
				t.Fatalf("pending step lost on failed turn: steps=%d error=%v", steps, err)
			}
			for runID := range e.snapshots {
				replayed := 0
				if err := e.ObserveRun(context.Background(), runID, "", func(event *sdkv1.RunStreamMessage) error {
					if event.GetStep() != nil {
						replayed++
					}
					return nil
				}); err != nil || replayed != 1 {
					t.Fatalf("failed turn lost durable step: replayed=%d error=%v", replayed, err)
				}
			}
		})
	}
}
