package localrun

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Keep large context out of metadata updates and release closed-agent context without losing resume data.
 * @param t Private durable root with synthetic binary blobs and no provider traffic.
 */
func TestPersistentContextObjectsAndEviction(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte{0x80, 0xff, 0x00, 0x31}, 1<<18)
	encode := func(blobs map[string][]byte) *agentwire.ControlState {
		data, err := json.Marshal(map[string]any{"blobs": blobs, "images": map[string]agentwire.InputImage{"/ccload/attachments/synthetic.png": {Path: "/ccload/attachments/synthetic.png", MimeType: "image/png", Width: 2, Height: 3, Data: large}}})
		if err != nil {
			t.Fatal(err)
		}
		state := &agentwire.ControlState{}
		if err := json.Unmarshal(data, state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	key := base64.StdEncoding.EncodeToString([]byte{0xff, 0, 0xc3})
	record := e.agents[id]
	record.controls = encode(map[string][]byte{key: large})
	record.checkpoint = []byte{8, 1}
	e.mu.Lock()
	err = e.saveAgentLocked(record)
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := os.ReadFile(filepath.Join(root, "agent-"+id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata) > 8192 || bytes.Contains(metadata, []byte(base64.StdEncoding.EncodeToString(large))) {
		t.Fatal("a large immutable blob was rewritten into the agent metadata")
	}
	t.Logf("native_value_bytes=%d metadata_bytes=%d", len(large), len(metadata))
	before := make(map[string]os.FileInfo)
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() && info.Size() == int64(len(large)) {
			before[path] = info
		}
		return err
	})
	if err != nil || len(before) != 1 {
		t.Fatalf("expected one immutable large object: files=%d error=%v", len(before), err)
	}
	record.controls = encode(map[string][]byte{key: large, base64.StdEncoding.EncodeToString([]byte("small")): []byte("new")})
	e.mu.Lock()
	err = e.saveAgentLocked(record)
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for path, previous := range before {
		current, err := os.Stat(path)
		if err != nil || !os.SameFile(previous, current) || current.ModTime() != previous.ModTime() || current.Mode().Perm() != 0600 {
			t.Fatal("unchanged context object was rewritten or is not private")
		}
	}
	want, _ := json.Marshal(record.controls)
	e.CloseAgent(id)
	if record.controls != nil || record.checkpoint != nil {
		t.Fatal("closed agent retained its large native context in memory")
	}
	if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake"}); err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(e.agents[id].controls)
	if !bytes.Equal(got, want) || !bytes.Equal(e.agents[id].checkpoint, []byte{8, 1}) {
		t.Fatal("resume did not recover exact binary context after eviction")
	}
	e.Close()
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	if e.agents[id].controls != nil || e.agents[id].checkpoint != nil {
		t.Fatal("startup eagerly loaded detached agent context")
	}
	if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake"}); err != nil {
		t.Fatal(err)
	}
	got, _ = json.Marshal(e.agents[id].controls)
	if !bytes.Equal(got, want) {
		t.Fatal("restart lost context objects")
	}
	if err := e.DeleteAgent(id); err != nil {
		t.Fatal(err)
	}
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() && info.Size() == int64(len(large)) {
			t.Error("deleted agent left its large context object on disk")
		}
		return err
	})
}

/**
 * @brief Finish deletion after a crash between removing metadata and its object directory.
 * @param t Private state root with one simulated interrupted deletion.
 */
func TestPersistentOrphanAgentObjects(t *testing.T) {
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
	orphan := e.agentObjectDirectory(id)
	e.Close()
	if err := os.Remove(filepath.Join(root, "agent-"+id+".json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "unrelated.objects"), 0700); err != nil {
		t.Fatal(err)
	}
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("interrupted agent deletion leaked its object directory")
	}
	if _, err := os.Stat(filepath.Join(root, "unrelated.objects")); err != nil {
		t.Fatal("orphan collection removed an unrelated directory")
	}
}

/**
 * @brief Fail resume atomically when a referenced context object is missing or corrupted.
 * @param t Private state roots, synthetic values and fake replacement credentials.
 */
func TestPersistentContextObjectIntegrity(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "corrupted", true: "missing"}[missing], func(t *testing.T) {
			root := t.TempDir()
			e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { e.Close() }()
			id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			var state agentwire.ControlState
			if err := json.Unmarshal([]byte(`{"blobs":{"aWQ=":"dmFsdWU="}}`), &state); err != nil {
				t.Fatal(err)
			}
			if err := e.persistNativeState(e.agents[id], []byte{8, 1}, &state); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(e.agentObjectDirectory(id), e.agents[id].native.Controls.Blobs["aWQ="])
			e.Close()
			if missing {
				err = os.Remove(path)
			} else {
				err = os.WriteFile(path, []byte("other"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake"}); err == nil {
				t.Fatal("corrupt or missing context was silently discarded")
			}
			if record := e.agents[id]; record.apiKey != "" || record.controls != nil || record.checkpoint != nil {
				t.Fatal("failed hydration published partial state or installed a credential")
			}
		})
	}
}

/**
 * @brief Migrate the existing inline format once and collect unreferenced objects left by interrupted writes.
 * @param t Private state roots without provider traffic.
 */
func TestPersistentLegacyMigrationAndObjectCollection(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Name: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "agent-"+id+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved savedAgent
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	saved.Version, saved.Native, saved.Checkpoint = 1, nil, []byte{8, 1}
	var state agentwire.ControlState
	if err := json.Unmarshal([]byte(`{"blobs":{"aWQ=":"dmFsdWU="}}`), &state); err != nil {
		t.Fatal(err)
	}
	saved.Controls = &state
	data, err = json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake"}); err != nil || !bytes.Equal(e.agents[id].checkpoint, []byte{8, 1}) {
		t.Fatalf("legacy native context was not preserved: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	saved = savedAgent{}
	if err := json.Unmarshal(data, &saved); err != nil || saved.Version != 2 || saved.Controls != nil || saved.Native == nil {
		t.Fatal("legacy context was not migrated to referenced objects")
	}
	orphan, err := e.putAgentObject(id, []byte("interrupted uncommitted object"))
	if err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(e.agentObjectDirectory(id), orphan)
	e.Close()
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatal("an interrupted write left an unreferenced object indefinitely")
	}
	if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake"}); err != nil {
		t.Fatalf("object collection removed referenced context: %v", err)
	}
}

/**
 * @brief Evict completed event payloads and replay them exactly after close and restart.
 * @param t Private durable engine with one synthetic completed conversation turn.
 */
func TestPersistentHistoryLazyReplay(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	e.snapshots["synthetic-run"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "synthetic-run", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED}
	e.histories["synthetic-run"] = newRunHistory()
	user, _ := sdkMessage("user", map[string]any{"message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "synthetic retained question"}}}})
	if _, err := e.recordRunEvent("synthetic-run", user); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	_, err = e.sealRunHistoryLocked("synthetic-run")
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	want := append([]*sdkv1.RunStreamMessage(nil), e.histories["synthetic-run"].events...)
	e.CloseAgent(id)
	if len(e.histories["synthetic-run"].events) != 0 {
		t.Fatal("closed agent retained completed replay payloads")
	}
	check := func() {
		t.Helper()
		var got []*sdkv1.RunStreamMessage
		if err := e.ObserveRun(context.Background(), "synthetic-run", "", func(message *sdkv1.RunStreamMessage) error { got = append(got, message); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatal("evicted replay lost events")
		}
		for i := range got {
			if !proto.Equal(got[i], want[i]) {
				t.Fatal("evicted replay changed event contents or offsets")
			}
		}
		messages, err := e.AgentMessages(id, nil)
		if err != nil || len(messages) != 1 {
			t.Fatalf("lazy conversation messages: %v", err)
		}
		if len(e.histories["synthetic-run"].events) != 0 {
			t.Fatal("observing detached history left payloads pinned in memory")
		}
	}
	check()
	e.Close()
	e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.histories["synthetic-run"].events) != 0 {
		t.Fatal("startup eagerly loaded all completed replay events")
	}
	check()
}
