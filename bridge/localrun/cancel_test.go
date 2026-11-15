package localrun

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Verify cancel run cancels registered run.
 * @param t Test harness.
 */
func TestCancelRunCancelsRegisteredRun(t *testing.T) {
	engine := NewEngine(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.activeRuns["run-1"] = cancel
	if !engine.CancelRun("run-1") {
		t.Fatal("active run not found")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("context error = %v", ctx.Err())
	}
	if engine.CancelRun("missing") {
		t.Fatal("missing run was reported active")
	}
}

/**
 * @brief Retained run snapshots cannot be mutated by RPC consumers and disappear with their agent.
 * @param t Test harness.
 */
func TestRunSnapshotIsolationAndDeletion(t *testing.T) {
	engine := NewEngine(nil)
	engine.snapshots["finished"] = &sdkv1.RunSnapshot{AgentId: "agent", RunId: "finished", Result: "original", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED}
	snapshot := engine.GetRun("finished")
	snapshot.Result = "changed"
	if engine.GetRun("finished").GetResult() != "original" {
		t.Fatal("caller modified retained run state")
	}
	if !engine.CancelRun("finished") {
		t.Fatal("completed run cancellation was not idempotent")
	}
	if err := engine.DeleteAgent("agent"); err != nil {
		t.Fatal(err)
	}
	if engine.GetRun("finished") != nil || engine.CancelRun("finished") {
		t.Fatal("agent deletion retained run state")
	}
	engine.Close()
	if _, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"}); err == nil {
		t.Fatal("closed engine accepted a new agent")
	}
}

/**
 * @brief Encode length-delimited fixture fields using protobuf's standard codec.
 * @param number Field number from the pinned Cursor descriptors.
 * @param value Nested fixture payload.
 * @return Encoded field.
 */
func fixtureField(number protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
}

/**
 * @brief Wait for callback cancellation rather than executing a host tool.
 */
type blockedToolHandler struct {
	started chan struct{} ///< Callback arrival signal.
	stopped chan struct{} ///< Callback context cancellation signal.
}

/**
 * @brief Hold one tool result until the owning run is canceled.
 * @param ctx Callback request context.
 * @param request Unused fixture call.
 * @return No result after cancellation.
 * @return The callback context error.
 */
func (h *blockedToolHandler) CallCustomTool(ctx context.Context, request *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
	close(h.started)
	<-ctx.Done()
	close(h.stopped)
	return nil, ctx.Err()
}

/**
 * @brief Prove abort, agent deletion and shutdown close a real bidirectional Run and its pending callback.
 * @param t Test harness with private HTTP/2 upstream and callback servers.
 */
func TestCancellationClosesRunAndPendingTool(t *testing.T) {
	for _, action := range []string{"cancel", "delete", "close", "heartbeat"} {
		t.Run(action, func(t *testing.T) {
			callback := &blockedToolHandler{started: make(chan struct{}), stopped: make(chan struct{})}
			path, handler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(callback)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			callbackServer := httptest.NewServer(mux)
			defer callbackServer.Close()
			disconnected := make(chan struct{})
			controlReplied := make(chan struct{})
			heartbeatSeen := make(chan struct{})
			var upstream *httptest.Server
			upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/auth/exchange_user_api_key":
					_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
				case "/aiserver.v1.ServerConfigService/GetServerConfig":
					urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
					_, _ = w.Write(fixtureField(27, urls))
				case "/agent.v1.AgentService/Run":
					defer close(disconnected)
					if r.ProtoMajor != 2 {
						t.Error("Run did not negotiate HTTP/2")
					}
					if _, err := readConnectDataFrame(r.Body); err != nil {
						t.Error(err)
						return
					}
					call := append(fixtureField(15, []byte("exec-test")), fixtureField(11, append(fixtureField(1, []byte("marker")), fixtureField(3, []byte("call-test"))...))...)
					w.Header().Set("Content-Type", "application/connect+proto")
					_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, call)))
					w.(http.Flusher).Flush()
					<-callback.started
					blob := fixtureField(3, append(fixtureField(1, []byte("test-blob")), fixtureField(2, []byte("context"))...))
					_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(4, blob)))
					w.(http.Flusher).Flush()
					if _, err := readConnectDataFrame(r.Body); err == nil {
						close(controlReplied)
					}
					if action == "heartbeat" {
						clientBeat, execBeat := false, false
						for !clientBeat || !execBeat {
							frame, err := readConnectDataFrame(r.Body)
							if err != nil {
								t.Errorf("heartbeat while tool is pending: %v", err)
								return
							}
							clientBeat = clientBeat || bytes.Equal(frame, fixtureField(7, nil))
							execBeat = execBeat || bytes.Equal(frame, fixtureField(5, fixtureField(3, protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 0))))
						}
						close(heartbeatSeen)
					}
					<-r.Context().Done()
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			upstream.EnableHTTP2 = true
			upstream.StartTLS()
			defer upstream.Close()
			api := cursorapi.NewClient(upstream.Client())
			api.APIBaseURL = upstream.URL
			engine := NewEngine(api)
			defer engine.Close()
			engine.SetToolCallback(callbackServer.URL, "fake-callback-token")
			agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- engine.Send(ctx, agent, "call marker", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil })
			}()
			select {
			case <-callback.started:
			case <-ctx.Done():
				t.Fatal("native tool callback did not start")
			}
			select {
			case <-controlReplied:
			case <-time.After(time.Second):
				t.Fatal("pending tool blocked upstream KV control replies")
			}
			engine.mu.Lock()
			var runID string
			for id := range engine.activeRuns {
				runID = id
			}
			engine.mu.Unlock()
			switch action {
			case "heartbeat":
				select {
				case <-heartbeatSeen:
				case <-ctx.Done():
					t.Fatal("heartbeat stopped while the tool was pending")
				}
				if !engine.CancelRun(runID) {
					t.Fatal("active Run was not found")
				}
			case "cancel":
				if !engine.CancelRun(runID) {
					t.Fatal("active Run was not found")
				}
			case "delete":
				if err := engine.DeleteAgent(agent); err != nil {
					t.Fatal(err)
				}
			case "close":
				engine.Close()
			}
			for _, signal := range []<-chan struct{}{callback.stopped, disconnected} {
				select {
				case <-signal:
				case <-time.After(time.Second):
					t.Fatal("Run or tool callback remained connected after cancellation")
				}
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Run stayed active after cancellation")
			}
			if action != "delete" && engine.GetRun(runID).GetStatus() != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_CANCELLED {
				t.Fatal("snapshot did not record cancellation")
			}
		})
	}
}

/**
 * @brief Cancel a pending client tool when Cursor closes its KV stream.
 * @param t Test harness with private HTTP/2 upstream and callback servers.
 */
func TestUpstreamKVCloseCancelsPendingTool(t *testing.T) {
	callback := &blockedToolHandler{started: make(chan struct{}), stopped: make(chan struct{})}
	_, handler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(callback)
	callbackServer := httptest.NewServer(handler)
	defer callbackServer.Close()
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			call := fixtureField(11, append(fixtureField(1, []byte("marker")), fixtureField(3, []byte("call-test"))...))
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, call)))
			w.(http.Flusher).Flush()
			select {
			case <-callback.started:
			case <-r.Context().Done():
				return
			}
			payload := []byte(`{"error":{"code":"canceled","message":"KV stream closed"}}`)
			header := []byte{2, 0, 0, 0, 0}
			binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
			_, _ = w.Write(append(header, payload...))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	engine := NewEngine(api)
	defer engine.Close()
	engine.SetToolCallback(callbackServer.URL, "fake-callback-token")
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = engine.Send(ctx, agent, "call marker", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil })
	if connect.CodeOf(err) != connect.CodeCanceled || !strings.Contains(err.Error(), "KV stream closed") {
		t.Fatalf("upstream failure was not preserved: %v", err)
	}
	select {
	case <-callback.stopped:
	case <-time.After(time.Second):
		t.Fatal("client tool callback remained active after the upstream stream closed")
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.activeRuns) != 0 || len(engine.runAgents) != 0 {
		t.Fatal("closed upstream run remained registered")
	}
}

/**
 * @brief Keep answering KV requests after turnEnded until the upstream stream terminates.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestTurnEndedDrainsPendingKV(t *testing.T) {
	replied := make(chan bool, 1)
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(1, fixtureField(1, []byte("done"))))))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(14, []byte{8, 100, 16, 20, 24, 70, 32, 10, 40, 5}))))
			blob := fixtureField(3, append(fixtureField(1, []byte("blob")), fixtureField(2, []byte("value"))...))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(4, blob)))
			w.(http.Flusher).Flush()
			_, err := readConnectDataFrame(r.Body)
			replied <- err == nil
			if err != nil {
				return
			}
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
	engine := NewEngine(api)
	defer engine.Close()
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var usageEvents int
	var runID string
	if err := engine.Send(ctx, agent, "hello", nil, nil, func(msg *sdkv1.RunStreamMessage) error {
		if m := msg.GetSdkMessage(); m != nil && m.GetType() == "usage" {
			u := m.GetMessage().AsMap()["usage"].(map[string]any)
			if u["cacheReadTokens"] != float64(70) || u["cacheWriteTokens"] != float64(10) {
				t.Error("usage event lost cache counters")
			}
			usageEvents++
		}
		if result := msg.GetResult(); result != nil {
			runID = result.GetRunId()
			u := result.GetResult().GetUsage()
			if u.GetInputTokens() != 100 || u.GetOutputTokens() != 20 || u.GetCacheReadTokens() != 70 || u.GetCacheWriteTokens() != 10 || u.GetTotalTokens() != 120 || u.GetReasoningTokens() != 5 {
				t.Error("terminal result lost real usage")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if usageEvents != 1 || engine.GetRun(runID).GetUsage().GetCacheReadTokens() != 70 {
		t.Fatal("usage event or snapshot was not recorded")
	}
	select {
	case ok := <-replied:
		if !ok {
			t.Fatal("KV reply was not sent after turnEnded")
		}
	case <-ctx.Done():
		t.Fatal("upstream did not receive a KV reply")
	}
}

/**
 * @brief Reject an apparently clean transport end that lacks Cursor's terminal turn event.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunRejectsEndWithoutTurnEnded(t *testing.T) {
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(1, fixtureField(1, []byte("partial"))))))
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
	engine := NewEngine(api)
	defer engine.Close()
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); err == nil {
		t.Fatal("incomplete upstream turn was reported successful")
	}
}

/**
 * @brief Reject malformed data even after a terminal turn event.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunRejectsMalformedFrameAfterTurnEnded(t *testing.T) {
	for _, tc := range []struct {
		name string
		tail []byte
	}{
		{name: "malformed protobuf", tail: append(agentwire.EncodeConnectDataFrame([]byte{0x0d, 1, 2, 3}), []byte{2, 0, 0, 0, 2, '{', '}'}...)},
		{name: "malformed KV control", tail: append(agentwire.EncodeConnectDataFrame(fixtureField(4, []byte{0x0d, 1, 2, 3})), []byte{2, 0, 0, 0, 2, '{', '}'}...)},
		{name: "KV control has wrong wire type", tail: append(agentwire.EncodeConnectDataFrame([]byte{0x20, 1}), []byte{2, 0, 0, 0, 2, '{', '}'}...)},
		{name: "missing frame payload", tail: []byte{0, 0, 0, 0, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var upstream *httptest.Server
			upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/auth/exchange_user_api_key":
					_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
				case "/aiserver.v1.ServerConfigService/GetServerConfig":
					urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
					_, _ = w.Write(fixtureField(27, urls))
				case "/agent.v1.AgentService/Run":
					if _, err := readConnectDataFrame(r.Body); err != nil {
						t.Error(err)
						return
					}
					w.Header().Set("Content-Type", "application/connect+proto")
					_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(1, fixtureField(1, []byte("done"))))))
					_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(14, nil))))
					_, _ = w.Write(tc.tail)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			upstream.EnableHTTP2 = true
			upstream.StartTLS()
			defer upstream.Close()
			api := cursorapi.NewClient(upstream.Client())
			api.APIBaseURL = upstream.URL
			engine := NewEngine(api)
			defer engine.Close()
			agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); err == nil {
				t.Fatal("malformed upstream frame was reported successful")
			}
		})
	}
}

/**
 * @brief Keep the upstream request stream alive with the official client heartbeat.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunSendsClientHeartbeat(t *testing.T) {
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			frames := make(chan []byte, 1)
			go func() { frame, _ := readConnectDataFrame(r.Body); frames <- frame }()
			select {
			case frame := <-frames:
				if !bytes.Equal(frame, fixtureField(7, nil)) {
					t.Error("client heartbeat had an unexpected wire format")
					return
				}
			case <-time.After(6500 * time.Millisecond):
				payload := []byte(`{"error":{"code":"canceled","message":"KV stream closed"}}`)
				header := []byte{2, 0, 0, 0, 0}
				binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
				_, _ = w.Write(append(header, payload...))
				return
			}
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
	engine := NewEngine(api)
	defer engine.Close()
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	if err := engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); err != nil {
		t.Fatalf("heartbeat-required upstream failed: %v", err)
	}
}

/**
 * @brief Close each native exec stream after returning its control response.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunClosesCompletedExecStream(t *testing.T) {
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			exec := append(protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 7), fixtureField(10, nil)...)
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, exec)))
			w.(http.Flusher).Flush()
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			closeFrame, err := readConnectDataFrame(r.Body)
			want := fixtureField(5, fixtureField(1, protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 7)))
			if err != nil || !bytes.Equal(closeFrame, want) {
				t.Errorf("missing exec stream close: %v", err)
				return
			}
			unknown := append(protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 8), fixtureField(44, nil)...)
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, unknown)))
			w.(http.Flusher).Flush()
			failure, err := readConnectDataFrame(r.Body)
			wantFailure := fixtureField(5, fixtureField(2, append(protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 8), fixtureField(2, []byte("Unsupported Cursor exec operation"))...)))
			if err != nil || !bytes.Equal(failure, wantFailure) {
				t.Errorf("missing unsupported exec failure: %v", err)
				return
			}
			closeFrame, err = readConnectDataFrame(r.Body)
			want = fixtureField(5, fixtureField(1, protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 8)))
			if err != nil || !bytes.Equal(closeFrame, want) {
				t.Errorf("missing unsupported exec close: %v", err)
				return
			}
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
	engine := NewEngine(api)
	defer engine.Close()
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	if err := engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); err != nil {
		t.Fatalf("exec completion failed: %v", err)
	}
}

/**
 * @brief Send each native tool result before closing its exec stream, including reused completed IDs.
 * @param t Test harness with private callback and HTTP/2 servers.
 */
func TestRunClosesCompletedToolExec(t *testing.T) {
	value, err := structpb.NewStruct(map[string]any{"value": "result"})
	if err != nil {
		t.Fatal(err)
	}
	_, callback := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(&callbackResultFixture{result: value})
	callbackServer := httptest.NewServer(callback)
	defer callbackServer.Close()
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			id := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 7)
			call := append(id, fixtureField(11, append(fixtureField(1, []byte("marker")), fixtureField(3, []byte("call"))...))...)
			for range 128 {
				_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, call)))
				w.(http.Flusher).Flush()
				result, err := readConnectDataFrame(r.Body)
				number, _, _ := protowire.ConsumeTag(result)
				if err != nil || number != 2 {
					t.Errorf("missing tool result: %v", err)
					return
				}
				closed, err := readConnectDataFrame(r.Body)
				if err != nil || !bytes.Equal(closed, fixtureField(5, fixtureField(1, id))) {
					t.Errorf("missing tool exec close: %v", err)
					return
				}
			}
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
	engine := NewEngine(api)
	defer engine.Close()
	engine.SetToolCallback(callbackServer.URL, "fake")
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	if err := engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); err != nil {
		t.Fatalf("tool exec completion failed: %v", err)
	}
}

/**
 * @brief An upstream exec abort cancels its callback without cancelling the agent run.
 * @param t Test harness with private callback and HTTP/2 servers.
 */
func TestRunHandlesUpstreamToolAbort(t *testing.T) {
	callback := &blockedToolHandler{started: make(chan struct{}), stopped: make(chan struct{})}
	_, handler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(callback)
	callbackServer := httptest.NewServer(handler)
	defer callbackServer.Close()
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			id := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 7)
			call := append(id, fixtureField(11, append(fixtureField(1, []byte("marker")), fixtureField(3, []byte("call"))...))...)
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, call)))
			w.(http.Flusher).Flush()
			select {
			case <-callback.started:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(5, fixtureField(1, id))))
			w.(http.Flusher).Flush()
			failure, err := readConnectDataFrame(r.Body)
			want := fixtureField(5, fixtureField(2, append(id, fixtureField(2, []byte("cursor canceled exec operation"))...)))
			if err != nil || !bytes.Equal(failure, want) {
				t.Errorf("missing exec abort response: %v", err)
				return
			}
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
	engine := NewEngine(api)
	defer engine.Close()
	engine.SetToolCallback(callbackServer.URL, "fake")
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	if err := engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); err != nil {
		t.Fatalf("upstream abort terminated the run: %v", err)
	}
	select {
	case <-callback.stopped:
	case <-ctx.Done():
		t.Fatal("aborted callback remained active")
	}
}

/**
 * @brief Report unsupported interactive queries instead of waiting indefinitely.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunRejectsUnsupportedInteractionQuery(t *testing.T) {
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
			_, _ = w.Write(fixtureField(27, urls))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(7, fixtureField(99, nil))))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	engine := NewEngine(api)
	defer engine.Close()
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil })
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("interaction query returned %v; want unimplemented", err)
	}
}

/**
 * @brief Answer each official local interaction and continue the same run to completion.
 * @param t Test harness with a private HTTP/2 upstream.
 */
func TestRunAnswersLocalInteractions(t *testing.T) {
	responses := map[protowire.Number][]byte{
		2:  fixtureField(1, nil),
		3:  fixtureField(1, fixtureField(3, fixtureField(1, []byte("Interactive questions are not supported in local SDK runs")))),
		4:  fixtureField(1, nil),
		7:  fixtureField(1, fixtureField(1, nil)),
		8:  fixtureField(1, nil),
		9:  fixtureField(1, nil),
		10: fixtureField(2, fixtureField(1, []byte("PR management is only available in cloud agents"))),
		11: fixtureField(2, fixtureField(1, []byte("MCP authentication is not supported in local SDK runs"))),
		12: fixtureField(1, fixtureField(1, nil)),
		13: fixtureField(2, fixtureField(1, []byte("Environment replacement is not supported in local SDK runs"))),
		14: fixtureField(2, fixtureField(1, []byte("Connecting GitHub is not supported in local SDK runs"))),
	}
	for kind, expected := range responses {
		for _, id := range []uint64{0, 42} {
			t.Run(fmt.Sprintf("query_%d/id_%d", kind, id), func(t *testing.T) {
				var upstream *httptest.Server
				upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/auth/exchange_user_api_key":
						_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
					case "/aiserver.v1.ServerConfigService/GetServerConfig":
						urls := append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)
						_, _ = w.Write(fixtureField(27, urls))
					case "/agent.v1.AgentService/Run":
						if _, err := readConnectDataFrame(r.Body); err != nil {
							t.Error(err)
							return
						}
						queryID := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), id)
						query := append(queryID, fixtureField(kind, nil)...)
						w.Header().Set("Content-Type", "application/connect+proto")
						_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(7, query)))
						w.(http.Flusher).Flush()
						reply, err := readConnectDataFrame(r.Body)
						want := fixtureField(6, append(queryID, fixtureField(kind, expected)...))
						if err != nil || !bytes.Equal(reply, want) {
							t.Errorf("web search approval = %x, %v; want %x", reply, err, want)
							return
						}
						_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(1, fixtureField(1, []byte("search completed"))))))
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
				engine := NewEngine(api)
				defer engine.Close()
				agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var result *sdkv1.RunResult
				err = engine.Send(ctx, agent, "hello", nil, nil, func(msg *sdkv1.RunStreamMessage) error {
					if msg.GetResult() != nil {
						result = msg.GetResult().GetResult()
					}
					return nil
				})
				if err != nil || result.GetResult() != "search completed" {
					t.Fatalf("web search did not complete: result=%v error=%v", result, err)
				}
			})
		}
	}
}

/**
 * @brief Verify cancel run closes upstream request.
 * @param t Test harness.
 */
func TestCancelRunClosesUpstreamRequest(t *testing.T) {
	started := make(chan struct{})
	disconnected := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(disconnected)
	}))
	defer upstream.Close()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	engine := NewEngine(api)
	agent, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- engine.Send(ctx, agent, "hello", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil })
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("upstream request did not start")
	}
	engine.mu.Lock()
	var runID string
	for id := range engine.activeRuns {
		runID = id
	}
	engine.mu.Unlock()
	if runID == "" || !engine.CancelRun(runID) {
		t.Fatal("active run could not be canceled")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("upstream request remained connected after abort")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("abort error = %v", err)
		}
		snapshot := engine.GetRun(runID)
		if snapshot == nil || snapshot.Status != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_CANCELLED {
			t.Fatal("run snapshot did not record cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("Send remained blocked after abort")
	}
}
