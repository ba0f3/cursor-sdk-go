package localrun

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Verify native checkpoint context across real Composer turns without resending the first prompt.
 * @param t Opt-in harness; sends only synthetic text and never executes host/client tools.
 */
func TestNativeCheckpointLive(t *testing.T) {
	if os.Getenv("CCLOAD_CURSOR_STRESS_LIVE_TEST") != "1" {
		t.Skip("live checkpoint verification is disabled")
	}
	key := strings.TrimSpace(os.Getenv("CURSOR_API_KEY"))
	if key == "" {
		t.Fatal("CURSOR_API_KEY is required")
	}
	root := t.TempDir()
	e, err := NewPersistentEngine(cursorapi.NewClient(nil), root)
	if err != nil {
		t.Fatal("durable engine startup failed")
	}
	defer func() { e.Close() }()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: key, Model: &sdkv1.ModelSelection{Id: "composer-2.5"}, Tools: &sdkv1.ToolList{}})
	if err != nil {
		t.Fatal("agent registration failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for i, prompt := range []string{"Remember this synthetic code: FALCON7291. Reply only OK. Do not use tools.", "What synthetic code did I ask you to remember? Reply only with the code. Do not use tools."} {
		if i == 1 && os.Getenv("CCLOAD_CURSOR_CHECKPOINT_MEMORY") == "resume" {
			if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: key}); err != nil {
				t.Fatal("in-memory resume failed")
			}
		}
		if i == 1 && os.Getenv("CCLOAD_CURSOR_CHECKPOINT_MEMORY") != "1" && os.Getenv("CCLOAD_CURSOR_CHECKPOINT_MEMORY") != "resume" {
			beforeCheckpoint := append([]byte(nil), e.agents[id].checkpoint...)
			beforeControls, _ := json.Marshal(e.agents[id].controls)
			beforeConversation, beforeModel := e.agents[id].conversationID, e.agents[id].model.GetId()
			e.Close()
			e, err = NewPersistentEngine(cursorapi.NewClient(nil), root)
			if err != nil {
				t.Fatal("durable engine restart failed")
			}
			if _, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: key}); err != nil {
				t.Fatal("durable agent resume failed")
			}
			afterControls, _ := json.Marshal(e.agents[id].controls)
			if !bytes.Equal(beforeCheckpoint, e.agents[id].checkpoint) || !bytes.Equal(beforeControls, afterControls) || beforeConversation != e.agents[id].conversationID || beforeModel != e.agents[id].model.GetId() {
				t.Fatal("native state changed across durable round trip")
			}
		}
		var answer string
		if err := e.Send(ctx, id, prompt, nil, nil, func(message *sdkv1.RunStreamMessage) error {
			if result := message.GetResult(); result != nil {
				answer = result.GetResult().GetResult()
			}
			return nil
		}); err != nil {
			t.Fatalf("synthetic checkpoint turn %d failed: %v", i+1, err)
		}
		if i == 1 && !strings.Contains(answer, "FALCON7291") {
			t.Fatal("second turn could not recall the first turn's synthetic code")
		}
	}
	e.mu.Lock()
	checkpointPresent := len(e.agents[id].checkpoint) > 0
	e.mu.Unlock()
	if !checkpointPresent {
		t.Fatal("no native checkpoint was received")
	}
	mode := os.Getenv("CCLOAD_CURSOR_CHECKPOINT_MEMORY")
	if mode == "" {
		mode = "restart"
	}
	t.Logf("mode=%s: two native turns completed; second turn recalled synthetic context and retained checkpoint", mode)
}
