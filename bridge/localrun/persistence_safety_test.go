package localrun

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief A closed engine must never mutate a replacement engine's durable state after releasing its lease.
 * @param t Separate private roots for stale delete, archive and handle-close operations.
 */
func TestPersistentClosedEngineCannotMutateReplacement(t *testing.T) {
	for _, operation := range []string{"delete", "archive", "close", "save"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			old, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
			if err != nil {
				t.Fatal(err)
			}
			id, _, err := old.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
			if err != nil {
				old.Close()
				t.Fatal(err)
			}
			old.Close()
			replacement, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
			if err != nil {
				t.Fatal(err)
			}
			defer replacement.Close()
			if _, err := replacement.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake", Name: "replacement"}); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "agent-"+id+".json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "delete":
				err = old.DeleteAgent(id)
			case "archive":
				err = old.ArchiveAgent(id, true)
			case "close":
				old.CloseAgent(id)
			case "save":
				old.mu.Lock()
				err = old.saveAgentLocked(old.agents[id])
				old.mu.Unlock()
			}
			if operation != "close" && connect.CodeOf(err) != connect.CodeFailedPrecondition {
				t.Errorf("stale engine did not reject mutation: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("stale engine changed replacement metadata: %v", err)
			}
		})
	}
}

/**
 * @brief Repair equal-length corrupt objects while the writer still owns the correct bytes.
 * @param t Private object directory and synthetic immutable content.
 */
func TestPersistentPutRepairsCorruptedObject(t *testing.T) {
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("correct bytes")
	ref, err := e.putAgentObject(id, want)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.agentObjectDirectory(id), ref)
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), len(want)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.putAgentObject(id, want); err != nil {
		t.Fatal(err)
	}
	got, err := e.getAgentObject(id, ref)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("put acknowledged a corrupt object instead of preserving the correct bytes: %v", err)
	}
}

/**
 * @brief Retain a delivered SDK update across interruption without an explicit checkpoint or terminal save.
 * @param t Private durable root and a synthetic uncompleted run.
 */
func TestPersistentDeliveredEventSurvivesInterruption(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		e.Close()
		t.Fatal(err)
	}
	e.snapshots["interrupted"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "interrupted", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["interrupted"] = newRunHistory()
	message, _ := sdkMessage("assistant", map[string]any{"message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "synthetic delivered suffix"}}}})
	if _, err := e.recordRunEvent("interrupted", message); err != nil {
		e.Close()
		t.Fatal(err)
	}
	e.Close() // No explicit save: simulate interruption after delivering this update.
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	conversation, err := e.RunConversation(context.Background(), "interrupted")
	if err != nil || !strings.Contains(conversation, "synthetic delivered suffix") {
		t.Fatalf("delivered update disappeared after interruption: %v", err)
	}
	var offsets []string
	if err := e.ObserveRun(context.Background(), "interrupted", "1", func(message *sdkv1.RunStreamMessage) error {
		offsets = append(offsets, message.GetOffset())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != "2" || offsets[1] != "3" {
		t.Fatal("recovery reused an already delivered offset")
	}
}
