package localrun

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Authenticated callback fixture that can return a result or an ambiguous failure.
 */
type receiptCallbackFixture struct {
	sdkv1connect.UnimplementedSdkCustomToolCallbackServiceHandler
	calls atomic.Int32                                                                                                                  ///< Number of actual callback attempts.
	fn    func(context.Context, *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) ///< Test-controlled callback behavior.
}

/**
 * @brief Verify callback authentication before running a test-owned callback.
 * @param ctx Callback lifetime.
 * @param request SDK callback request.
 * @return Prepared response.
 * @return Authentication or fixture failure.
 */
func (f *receiptCallbackFixture) CallCustomTool(ctx context.Context, request *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
	if request.Header().Get("Authorization") != "Bearer fake-receipt-callback-secret" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing fixture authorization"))
	}
	f.calls.Add(1)
	return f.fn(ctx, request)
}

/**
 * @brief Create a private persistent or memory-only agent with one callback tool.
 * @param t Test harness.
 * @param root Private durable directory, or empty for memory-only state.
 * @param fixture Authenticated callback implementation.
 * @return Isolated engine and registered agent.
 */
func receiptTestEngine(t *testing.T, root string, fixture *receiptCallbackFixture) (*Engine, *agentRecord) {
	t.Helper()
	var e *Engine
	if root == "" {
		e = NewEngine(nil)
	} else {
		var err error
		e, err = NewPersistentEngine(nil, root)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(e.Close)
	_, handler := sdkv1connect.NewSdkCustomToolCallbackServiceHandler(fixture)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	e.SetToolCallback(server.URL, "fake-receipt-callback-secret")
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake-receipt-runtime-key", Local: &sdkv1.LocalAgentOptions{CustomTools: map[string]*sdkv1.CustomToolDefinition{"write": {}}}})
	if err != nil {
		t.Fatal(err)
	}
	return e, e.agents[id]
}

/**
 * @brief Construct a valid native custom-tool call with typed JSON arguments.
 * @param t Test harness.
 * @param id Native tool call identity.
 * @param values Tool argument values.
 * @return Owned native invocation.
 */
func receiptTestCall(t *testing.T, id string, values map[string]any) *agentwire.ToolCall {
	t.Helper()
	args, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatal(err)
	}
	return &agentwire.ToolCall{ID: 1, ExecID: "exec", Name: "write", CallID: id, Args: args}
}

/**
 * @brief Replay completed values across restart without repeating actions or sharing mutable results.
 * @param t Test harness.
 */
func TestDurableToolCompletedReplay(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, explicitError := range []bool{false, true} {
			name := "memory"
			if persistent {
				name = "persistent"
			}
			if explicitError {
				name += "_explicit_error"
			}
			t.Run(name, func(t *testing.T) {
				root := ""
				if persistent {
					root = t.TempDir()
				}
				fixture := &receiptCallbackFixture{fn: func(_ context.Context, request *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
					if request.Msg.GetToolCallId() != "call-1" || request.Msg.GetArgs().AsMap()["path"] != "a" {
						t.Error("callback lost native identity or arguments")
					}
					value, _ := structpb.NewStruct(map[string]any{"content": []any{map[string]any{"type": "text", "text": "recorded"}}, "isError": explicitError, "error": "ordinary data"})
					return connect.NewResponse(&sdkv1.CallCustomToolResponse{Result: value}), nil
				}}
				e, rec := receiptTestEngine(t, root, fixture)
				call := receiptTestCall(t, "call-1", map[string]any{"path": "a", "count": 2})
				value, failed, err := e.invokeDurableTool(context.Background(), rec.id, rec, call)
				if err != nil || failed != explicitError {
					t.Fatalf("initial failed=%v err=%v", failed, err)
				}
				value["content"].([]any)[0].(map[string]any)["text"] = "caller mutation"
				call.Args.Fields["path"] = structpb.NewStringValue("changed after dispatch")
				if persistent {
					e.Close()
					e, err = NewPersistentEngine(nil, root)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(e.Close)
					rec = e.agents[rec.id]
					if rec == nil {
						t.Fatal("agent missing after restart")
					}
				}
				// A different exec identity and map insertion order must preserve the native call identity.
				retry := receiptTestCall(t, "call-1", map[string]any{"count": 2, "path": "a"})
				retry.ID, retry.ExecID = 9, "new-exec"
				value, failed, err = e.invokeDurableTool(context.Background(), rec.id, rec, retry)
				if err != nil || failed != explicitError || value["content"].([]any)[0].(map[string]any)["text"] != "recorded" {
					t.Fatalf("replay=%v failed=%v err=%v", value, failed, err)
				}
				if fixture.calls.Load() != 1 {
					t.Fatal("completed callback executed again")
				}
				if !persistent && len(rec.toolReceiptObjects) != 1 {
					t.Fatal("superseded pending receipt retained in memory")
				}
				if persistent {
					if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
						if walkErr != nil {
							return walkErr
						}
						if entry.IsDir() {
							return nil
						}
						data, readErr := os.ReadFile(path)
						if readErr != nil {
							return readErr
						}
						for _, secret := range []string{"fake-receipt-callback-secret", "fake-receipt-runtime-key"} {
							if strings.Contains(string(data), secret) {
								t.Error("runtime credential persisted")
							}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

/**
 * @brief Identity reuse with different arguments or a different tool fails before execution.
 * @param t Test harness.
 */
func TestDurableToolRejectsSignatureMismatch(t *testing.T) {
	fixture := &receiptCallbackFixture{fn: func(context.Context, *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
		value, _ := structpb.NewStruct(map[string]any{"ok": true})
		return connect.NewResponse(&sdkv1.CallCustomToolResponse{Result: value}), nil
	}}
	e, rec := receiptTestEngine(t, t.TempDir(), fixture)
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, receiptTestCall(t, "same", map[string]any{"value": "first"})); err != nil {
		t.Fatal(err)
	}
	for _, call := range []*agentwire.ToolCall{receiptTestCall(t, "same", map[string]any{"value": "changed"}), {Name: "other", CallID: "same"}} {
		if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil {
			t.Fatal("changed signature was accepted")
		}
	}
	if fixture.calls.Load() != 1 {
		t.Fatal("signature mismatch repeated callback")
	}
}

/**
 * @brief Transport ambiguity leaves a durable unknown claim and never repeats a possible action.
 * @param t Test harness.
 */
func TestDurableToolUnknownOutcomeSurvivesRestart(t *testing.T) {
	fixture := &receiptCallbackFixture{fn: func(context.Context, *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("response lost after action"))
	}}
	root := t.TempDir()
	e, rec := receiptTestEngine(t, root, fixture)
	call := receiptTestCall(t, "unknown", nil)
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil {
		t.Fatal("ambiguous callback was marked completed")
	}
	e.Close()
	e, err := NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	rec = e.agents[rec.id]
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("pending replay error=%v", err)
	}
	if fixture.calls.Load() != 1 {
		t.Fatal("unknown action executed again")
	}
}

/**
 * @brief A concurrent duplicate cannot execute while the durable claim is in flight.
 * @param t Test harness.
 */
func TestDurableToolConcurrentDuplicateAndInterruptedClaim(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	fixture := &receiptCallbackFixture{fn: func(ctx context.Context, _ *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, connect.NewError(connect.CodeCanceled, errors.New("interrupted"))
	}}
	root := t.TempDir()
	e, rec := receiptTestEngine(t, root, fixture)
	call := receiptTestCall(t, "interrupted", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := e.invokeDurableTool(ctx, rec.id, rec, call); done <- err }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("callback never started: %v", err)
	case <-ctx.Done():
		t.Fatal("callback did not start before timeout")
	}
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil {
		t.Error("in-flight duplicate accepted")
	}
	// Read the published agent document while its callback is still in progress.
	data, err := os.ReadFile(filepath.Join(root, "agent-"+rec.id+".json"))
	if err != nil || !strings.Contains(string(data), "tool_receipts") {
		t.Error("callback started before its claim was durable")
	}
	cancel()
	close(release)
	if err := <-done; err == nil {
		t.Error("interrupted callback acknowledged completion")
	}
	e.Close()
	e, err = NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	rec = e.agents[rec.id]
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil {
		t.Fatal("interrupted claim replayed")
	}
	if fixture.calls.Load() != 1 {
		t.Fatal("interrupted callback executed twice")
	}
}

/**
 * @brief Missing identities and explicit argument/count/storage limits never silently permit execution.
 * @param t Test harness.
 */
func TestDurableToolLimits(t *testing.T) {
	fixture := &receiptCallbackFixture{fn: func(context.Context, *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
		value, _ := structpb.NewStruct(map[string]any{"ok": true})
		return connect.NewResponse(&sdkv1.CallCustomToolResponse{Result: value}), nil
	}}
	e, rec := receiptTestEngine(t, "", fixture)
	for _, call := range []*agentwire.ToolCall{
		receiptTestCall(t, "", nil),
		receiptTestCall(t, strings.Repeat("i", maxToolCallIDBytes+1), nil),
		receiptTestCall(t, "huge-args", map[string]any{"value": strings.Repeat("x", maxToolArgumentBytes)}),
	} {
		if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil {
			t.Fatal("invalid invocation was accepted")
		}
	}
	if fixture.calls.Load() != 0 || len(rec.toolReceipts) != 0 {
		t.Fatal("invalid invocation reached the callback or created a claim")
	}
	call := receiptTestCall(t, "existing", nil)
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err != nil {
		t.Fatal(err)
	}
	ref := rec.toolReceipts[call.CallID]
	for i := len(rec.toolReceipts); i < maxToolReceipts; i++ {
		rec.toolReceipts[fmt.Sprintf("reserved-%d", i)] = ref
	}
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, receiptTestCall(t, "new", nil)); err == nil || !strings.Contains(err.Error(), "count limit") {
		t.Fatalf("count limit error=%v", err)
	}
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err != nil || fixture.calls.Load() != 1 {
		t.Fatalf("retained receipt cannot replay at capacity: %v", err)
	}
	if err := e.checkToolReceiptBudgetLocked(rec, "existing", maxToolReceiptBytes+1); err == nil {
		t.Fatal("oversized receipt accepted")
	}
	// Shared test bytes avoid allocating the full aggregate budget; each reference
	// still contributes its retained object size to the enforced per-agent limit.
	data := make([]byte, maxToolReceiptBytes)
	rec.toolReceipts, rec.toolReceiptObjects = make(map[string]string), map[string][]byte{"fixture": data}
	for i := range maxToolReceiptTotalBytes / maxToolReceiptBytes {
		rec.toolReceipts[fmt.Sprintf("budget-%d", i)] = "fixture"
	}
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, receiptTestCall(t, "over-budget", nil)); err == nil || !strings.Contains(err.Error(), "storage limit") {
		t.Fatalf("aggregate storage limit error=%v", err)
	}
	if fixture.calls.Load() != 1 {
		t.Fatal("limit violation reached callback")
	}
}

/**
 * @brief Claim publication failure prevents execution; result publication failure prevents acknowledgement and replay.
 * @param t Test harness.
 */
func TestDurableToolPersistenceFailure(t *testing.T) {
	for _, stage := range []string{"claim", "result"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			var document, backup string
			blockDocument := func() error {
				if err := os.Rename(document, backup); err != nil {
					return err
				}
				return os.Mkdir(document, 0700)
			}
			fixture := &receiptCallbackFixture{fn: func(context.Context, *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
				if stage == "result" {
					if err := blockDocument(); err != nil {
						return nil, err
					}
				}
				value, _ := structpb.NewStruct(map[string]any{"ok": true})
				return connect.NewResponse(&sdkv1.CallCustomToolResponse{Result: value}), nil
			}}
			e, rec := receiptTestEngine(t, root, fixture)
			document, backup = filepath.Join(root, "agent-"+rec.id+".json"), filepath.Join(root, ".pending-document")
			if stage == "claim" {
				if err := blockDocument(); err != nil {
					t.Fatal(err)
				}
			}
			call := receiptTestCall(t, "write-failed", nil)
			if result, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil || result != nil {
				t.Fatal("publication failure acknowledged tool completion")
			}
			wantCalls := int32(0)
			if stage == "result" {
				wantCalls = 1
			}
			if fixture.calls.Load() != wantCalls {
				t.Fatalf("callbacks=%d, want %d", fixture.calls.Load(), wantCalls)
			}
			if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil || fixture.calls.Load() != wantCalls {
				t.Fatal("failed publication permitted callback replay")
			}
			if stage == "result" {
				// Recover the last successfully published pending document and prove
				// that a restarted process cannot execute its possibly completed action.
				if err := os.Remove(document); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(backup, document); err != nil {
					t.Fatal(err)
				}
				e.Close()
				e, err := NewPersistentEngine(nil, root)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(e.Close)
				rec = e.agents[rec.id]
				if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, call); err == nil || !strings.Contains(err.Error(), "unknown") {
					t.Fatalf("restart acknowledged unpublished result: %v", err)
				}
			}
		})
	}
}
