package localrun

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Register a private agent for deterministic forced-send lifecycle fixtures.
 * @param t Test harness.
 * @param e Memory-only engine.
 * @return Registered agent identity.
 */
func forceTestAgent(t *testing.T, e *Engine) string {
	t.Helper()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake-force-key"})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

/**
 * @brief Force cancels outside the mutex and waits only for the initially selected agent's cleanup.
 * @param t Synthetic independent owners and private cleanup channels.
 */
func TestPrepareForcedSendWaitsForOwnedCleanup(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	id, otherID := forceTestAgent(t, e), forceTestAgent(t, e)
	oldDone, otherDone, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishingDone := make(chan struct{})
	var cancels atomic.Int32
	e.activeRuns["old"] = func() {
		// Locking here proves prepareForcedSend never invokes cancellation under e.mu.
		e.mu.Lock()
		defer e.mu.Unlock()
		if cancels.Add(1) == 1 {
			close(canceled)
		}
	}
	otherCtx, otherCancel := context.WithCancel(context.Background())
	defer otherCancel()
	e.activeRuns["other"], e.runAgents["other"], e.runDone["other"] = otherCancel, otherID, otherDone
	e.runAgents["old"], e.runDone["old"] = id, oldDone
	e.runDone["finishing"] = finishingDone
	e.snapshots["finishing"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "finishing", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED}
	if err := e.prepareForcedSend(context.Background(), id, false); err != nil || cancels.Load() != 0 {
		t.Fatalf("nonforce touched owner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- e.prepareForcedSend(ctx, id, true) }()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("force did not cancel the selected run")
	}
	select {
	case err := <-result:
		t.Fatalf("force returned before cleanup: %v", err)
	default:
	}
	if otherCtx.Err() != nil {
		t.Fatal("force canceled another agent")
	}
	var newCancels atomic.Int32
	e.mu.Lock()
	delete(e.activeRuns, "old")
	delete(e.runAgents, "old")
	delete(e.runDone, "old")
	// A competing send may register before this waiter resumes. It must not be
	// canceled by a loop that keeps looking for more work after the original wait.
	e.activeRuns["new"] = func() { newCancels.Add(1) }
	e.runAgents["new"], e.runDone["new"] = id, make(chan struct{})
	close(oldDone)
	e.mu.Unlock()
	select {
	case err := <-result:
		t.Fatalf("force skipped older physical cleanup: %v", err)
	default:
	}
	close(finishingDone)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("force waited for an unrelated or replacement run")
	}
	if newCancels.Load() != 0 || cancels.Load() != 1 || otherCtx.Err() != nil {
		t.Fatal("force canceled outside its original target set")
	}
}

/**
 * @brief A logically finished run still owns physical cleanup through its runDone channel and snapshot.
 * @param t Private lifecycle completion channels.
 */
func TestPrepareForcedSendWaitsAfterLogicalFinish(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	id := forceTestAgent(t, e)
	done := make(chan struct{})
	e.runDone["finishing"] = done
	e.snapshots["finishing"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "finishing", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.prepareForcedSend(ctx, id, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := e.prepareForcedSend(ctx, id, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("force did not await physical cleanup: %v", err)
	}
	close(done)
	if err := e.prepareForcedSend(context.Background(), id, true); err != nil {
		t.Fatal(err)
	}
}

/**
 * @brief Cancellation of the force waiter stops waiting without canceling unrelated agents.
 * @param t Synthetic active owner with delayed physical completion.
 */
func TestPrepareForcedSendWaiterCancellation(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	id := forceTestAgent(t, e)
	canceled := make(chan struct{})
	var once sync.Once
	e.activeRuns["old"] = func() { once.Do(func() { close(canceled) }) }
	e.runAgents["old"], e.runDone["old"] = id, make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- e.prepareForcedSend(ctx, id, true) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("force did not cancel old owner")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("force ignored caller cancellation")
	}
}

/**
 * @brief Invalid ownership or agent lifecycle state fails before cancellation.
 * @param t Private lifecycle fixtures without network work.
 */
func TestPrepareForcedSendRejectsUnsafeOwner(t *testing.T) {
	for _, state := range []string{"missing_completion", "missing_cancel", "detached", "missing_agent", "closed"} {
		t.Run(state, func(t *testing.T) {
			e := NewEngine(nil)
			defer e.Close()
			id := forceTestAgent(t, e)
			var calls atomic.Int32
			e.activeRuns["old"] = func() { calls.Add(1) }
			e.runAgents["old"], e.runDone["old"] = id, make(chan struct{})
			want := connect.CodeFailedPrecondition
			switch state {
			case "missing_completion":
				delete(e.runDone, "old")
			case "missing_cancel":
				delete(e.activeRuns, "old")
			case "detached":
				e.agents[id].detached = true
			case "missing_agent":
				delete(e.agents, id)
				want = connect.CodeNotFound
			case "closed":
				e.closed = true
			}
			if err := e.prepareForcedSend(context.Background(), id, true); connect.CodeOf(err) != want {
				t.Fatalf("state=%s error=%v", state, err)
			}
			if calls.Load() != 0 {
				t.Fatal("invalid owner was canceled")
			}
		})
	}
}

/**
 * @brief Actual forced Send cancels native I/O and waits for callback cleanup before starting its replacement.
 * @param t Local HTTP/2 native upstream and authenticated callback fixture.
 */
func TestForcedSendWaitsForNativeCallbackCleanup(t *testing.T) {
	callback := &blockedToolHandler{started: make(chan struct{}), stopped: make(chan struct{})}
	_, callbackHandler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(&receiptCallbackFixture{fn: callback.CallCustomTool})
	callbackServer := httptest.NewServer(callbackHandler)
	defer callbackServer.Close()
	disconnected, replacementStarted, cleanupBlocked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseCleanup := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseCleanup) })
	var turns atomic.Int32
	var oldPhysicalDone atomic.Value
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			_, _ = w.Write(fixtureField(27, append(fixtureField(1, []byte(upstream.URL)), fixtureField(2, []byte(upstream.URL))...)))
		case "/agent.v1.AgentService/Run":
			if _, err := readConnectDataFrame(r.Body); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			if turns.Add(1) == 1 {
				defer close(disconnected)
				call := fixtureField(11, append(fixtureField(1, []byte("marker")), fixtureField(3, []byte("old-call"))...))
				_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(2, call)))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				return
			}
			select {
			case <-oldPhysicalDone.Load().(chan struct{}):
			default:
				t.Error("replacement started before old physical cleanup")
			}
			close(replacementStarted)
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(fixtureField(1, fixtureField(1, fixtureField(1, []byte("replacement"))))))
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
	defer func() {
		releaseOnce.Do(func() { close(releaseCleanup) })
		e.Close()
	}()
	e.SetToolCallback(callbackServer.URL, "fake-receipt-callback-secret")
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake-force-key", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"marker": {}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldResult, replacementResult := make(chan error, 1), make(chan error, 1)
	go func() {
		oldResult <- e.Send(ctx, id, "old", nil, nil, func(message *sdkv1.RunStreamMessage) error {
			if event := message.GetSdkMessage(); event != nil && event.GetType() == "tool_call" && event.GetMessage().AsMap()["status"] == "error" {
				close(cleanupBlocked)
				<-releaseCleanup
			}
			return nil
		})
	}()
	select {
	case <-callback.started:
	case <-ctx.Done():
		t.Fatal("native callback did not start")
	}
	e.mu.Lock()
	var physicalDone chan struct{}
	for run, owner := range e.runAgents {
		if owner == id {
			physicalDone = e.runDone[run]
		}
	}
	e.mu.Unlock()
	if physicalDone == nil {
		t.Fatal("old run has no physical completion owner")
	}
	oldPhysicalDone.Store(physicalDone)
	if err := e.Send(ctx, id, "ordinary", nil, nil, func(*sdkv1.RunStreamMessage) error { return nil }); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("ordinary send error=%v", err)
	}
	force := true
	options := &sdkv1.SendOptions{Local: &sdkv1.LocalSendOptions{Force: &force}}
	if err := e.Send(ctx, id, "", nil, options, func(*sdkv1.RunStreamMessage) error { return nil }); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid forced send error=%v", err)
	}
	select {
	case <-callback.stopped:
		t.Fatal("nonforce or invalid send canceled active callback")
	default:
	}
	go func() {
		replacementResult <- e.Send(ctx, id, "replacement", nil, options, func(*sdkv1.RunStreamMessage) error { return nil })
	}()
	for _, signal := range []<-chan struct{}{cleanupBlocked, callback.stopped, disconnected} {
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("force did not cancel native stream and callback")
		}
	}
	select {
	case <-replacementStarted:
		t.Fatal("replacement ignored pending callback cleanup")
	default:
	}
	select {
	case err := <-replacementResult:
		t.Fatalf("replacement returned before cleanup: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(releaseCleanup) })
	select {
	case err := <-oldResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("old run error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("old run did not finish cleanup")
	}
	select {
	case err := <-replacementResult:
		if err != nil {
			t.Fatalf("replacement run error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("replacement did not complete")
	}
	if turns.Load() != 2 {
		t.Fatalf("native turns=%d, want 2", turns.Load())
	}
}
