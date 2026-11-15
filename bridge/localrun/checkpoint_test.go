package localrun

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Preserve actual checkpoint bytes and KV values over two native HTTP/2 turns.
 * @param t Private upstream and registry; no user context or credentials are sent.
 */
func TestNativeCheckpointContinuesAcrossTurns(t *testing.T) {
	testNativeCheckpointContinuation(t, false)
}

/**
 * @brief Recover native context and blobs after closing and reconstructing the whole engine.
 * @param t Private upstream and durable state directory.
 */
func TestNativeCheckpointSurvivesRestart(t *testing.T) { testNativeCheckpointContinuation(t, true) }

/**
 * @brief Exercise two actual native requests with optional process-state reconstruction in between.
 * @param t Private upstream harness.
 * @param restart Whether to rebuild the engine and resume the stable agent between turns.
 */
func testNativeCheckpointContinuation(t *testing.T, restart bool) {
	checkpoint := fixtureField(4, []byte("synthetic-pending-call"))
	blobID := []byte{0xff, 0x00, 0x81, 0x22}
	var upstream *httptest.Server
	turn := 0
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			_, _ = w.Write(fixtureField(27, append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)))
		case "/agent.v1.AgentService/Run":
			turn++
			frame, err := readConnectDataFrame(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			_, _, n := protowire.ConsumeTag(frame)
			run, _ := protowire.ConsumeBytes(frame[n:])
			_, _, n = protowire.ConsumeTag(run)
			state, _ := protowire.ConsumeBytes(run[n:])
			if turn == 2 && !bytes.Equal(state, checkpoint) {
				t.Error("second turn discarded native checkpoint")
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			if turn == 1 {
				blob := fixtureField(3, append(fixtureField(1, blobID), fixtureField(2, []byte("synthetic-value"))...))
				_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(4, blob)))
			} else {
				_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(4, fixtureField(2, fixtureField(1, blobID)))))
			}
			w.(http.Flusher).Flush()
			reply, err := readConnectDataFrame(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			if turn == 2 && !bytes.Contains(reply, []byte("synthetic-value")) {
				t.Error("second turn lost checkpoint KV blob")
				return
			}
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(3, checkpoint)))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(1, fixtureField(1, []byte("done"))))))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(14, nil))))
			_, _ = w.Write([]byte{2, 0, 0, 0, 2, '{', '}'})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	e := NewEngine(api)
	root := t.TempDir()
	if restart {
		var err error
		e, err = NewPersistentEngine(api, root)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() { e.Close() }()
	agent, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var lastRun string
	var lastEvents []*sdkv1.RunStreamMessage
	for i := range 2 {
		if restart && i == 1 {
			e.Close()
			e, err = NewPersistentEngine(api, root)
			if err != nil {
				t.Fatal(err)
			}
			waited, err := e.WaitRun(ctx, lastRun)
			if err != nil || waited.GetResult() != "done" {
				t.Fatalf("restart lost final run result: %v", err)
			}
			var replay []*sdkv1.RunStreamMessage
			if err := e.ObserveRun(ctx, lastRun, "", func(event *sdkv1.RunStreamMessage) error { replay = append(replay, event); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(replay) != len(lastEvents) {
				t.Fatal("restart lost replay events")
			}
			for i, event := range replay {
				if !proto.Equal(event, lastEvents[i]) {
					t.Fatal("restart changed a replay event or offset")
				}
			}
			if _, err := e.ResumeAgent(agent, &sdkv1.AgentOptions{ApiKey: "fake"}); err != nil {
				t.Fatal(err)
			}
		}
		lastEvents = nil
		if err := e.Send(ctx, agent, "synthetic turn", nil, nil, func(event *sdkv1.RunStreamMessage) error {
			lastEvents = append(lastEvents, event)
			if event.GetResult() != nil {
				lastRun = event.GetResult().GetRunId()
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		conversation, err := e.RunConversation(ctx, lastRun)
		if err != nil || !bytes.Contains([]byte(conversation), []byte("synthetic turn")) || !bytes.Contains([]byte(conversation), []byte("done")) {
			t.Fatalf("run conversation lost history: %v", err)
		}
	}
}
