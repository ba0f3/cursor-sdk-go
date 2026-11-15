package localrun

import (
	"bytes"
	"encoding/base64"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Recover stable agent identity without persisting credentials and require a fresh resume credential.
 * @param t Harness with private state root and synthetic credentials/checkpoint.
 */
func TestPersistentAgentRestart(t *testing.T) {
	root := t.TempDir()
	api := cursorapi.NewClient(nil)
	e, err := NewPersistentEngine(api, root)
	if err != nil {
		t.Fatal(err)
	}
	e.SetToolCallback("http://127.0.0.1/unused", "fake-private-callback")
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake-key-never-persist", Name: "saved", Model: &sdkv1.ModelSelection{Id: "composer-2.5"}, Tools: &sdkv1.ToolList{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ArchiveAgent(id, true); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPersistentEngine(api, root); err == nil {
		t.Fatal("two writers acquired the same state root")
	}
	e.Close()
	files, _ := filepath.Glob(filepath.Join(root, "*.json"))
	if len(files) != 1 {
		t.Fatal("agent was not persisted")
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("fake-key-never-persist")) || bytes.Contains(data, []byte(base64.StdEncoding.EncodeToString([]byte("fake-key-never-persist")))) {
		t.Fatal("credential was persisted in plaintext")
	}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, credential := range []string{"fake-key-never-persist", "fake-private-callback"} {
			if bytes.Contains(contents, []byte(credential)) || bytes.Contains(contents, []byte(base64.StdEncoding.EncodeToString([]byte(credential)))) {
				t.Error("runtime authentication material was serialized into durable state")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(files[0]); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("durable agent file is not private")
	}
	if err := os.WriteFile(filepath.Join(root, ".agent-write-interrupted"), []byte("incomplete new state"), 0600); err != nil {
		t.Fatal(err)
	}
	e, err = NewPersistentEngine(api, root)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	info, err := e.AgentInfo(id)
	if err != nil || info.GetName() != "saved" || !info.GetArchived() {
		t.Fatalf("restart lost agent metadata: %v", err)
	}
	if e.agents[id].apiKey != "" {
		t.Fatal("loaded agent retained a credential")
	}
	if e.agents[id].toolPolicy.Allowed == nil {
		t.Fatal("restart lost explicit no-tools policy")
	}
	if _, err := e.ResumeAgent(id, nil); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("resume without credential: %v", err)
	}
	model, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "replacement-fake"})
	if err != nil || model.GetId() != "composer-2.5" {
		t.Fatalf("resume failed: %v", err)
	}
	if e.agents[id].apiKey != "replacement-fake" {
		t.Fatal("resume did not install the supplied credential")
	}
	if err := e.DeleteAgent(id); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e, err = NewPersistentEngine(api, root)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.AgentInfo(id); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatal("deleted agent reappeared after restart")
	}
}

/**
 * @brief Refuse corrupted durable state instead of silently discarding conversation history.
 * @param t Private state root.
 */
func TestPersistentAgentCorruptState(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "agent-bad.json"), []byte(`{broken`), 0600); err != nil {
		t.Fatal(err)
	}
	if e, err := NewPersistentEngine(cursorapi.NewClient(nil), root); err == nil {
		e.Close()
		t.Fatal("corrupted state was silently ignored")
	}
}
