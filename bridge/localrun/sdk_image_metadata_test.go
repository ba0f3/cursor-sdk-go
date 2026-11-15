package localrun

import (
	"testing"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Keep a supported one-MiB tool image journalable after the pinned SDK's numeric-object serialization.
 * @param t Synthetic opaque image data without provider calls or image execution.
 */
func TestSDKMaximumToolImageMetadataFitsHistory(t *testing.T) {
	image := append(fixtureField(1, make([]byte, 1<<20)), fixtureField(2, []byte("image/png"))...)
	result := fixtureField(1, fixtureField(1, fixtureField(2, image)))
	tool := fixtureField(15, append(fixtureField(1, fixtureField(5, []byte("marker"))), fixtureField(2, result)...))
	metadata, err := agentwire.DecodeSDKToolCall(tool)
	if err != nil {
		t.Fatal(err)
	}
	e := NewEngine(nil)
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	e.snapshots["image"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "image", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["image"] = newRunHistory()
	callbacks := runUpdateCallbacks{emit: func(message *sdkv1.RunStreamMessage) error {
		_, err := e.recordRunEvent("image", message)
		return err
	}}
	if err := callbacks.lifecycle(map[string]any{"type": "tool-call-completed", "callId": "image-call", "toolCall": metadata}, id, "image"); err != nil {
		t.Fatalf("supported tool image failed solely because its SDK metadata expanded: %v", err)
	}
	t.Logf("image_bytes=%d sdk_metadata_bytes=%d", 1<<20, e.histories["image"].bytes)
}
