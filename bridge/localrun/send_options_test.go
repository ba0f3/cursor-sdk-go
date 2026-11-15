package localrun

import (
	"testing"

	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Preserve opaque model parameters and an unused initial mode across a durable restart and resume.
 * @param t Private state store and fake credentials without provider requests.
 */
func TestModelAndPendingModeSurviveRestart(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	model := &sdkv1.ModelSelection{Id: "synthetic-model", Params: []*sdkv1.ModelParameterValue{{Id: "opaque", Value: "first"}, {Id: "fast", Value: "true"}}}
	id, returned, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Model: model, Mode: sdkv1.AgentModeOption_AGENT_MODE_OPTION_PLAN})
	if err != nil {
		t.Fatal(err)
	}
	expected := proto.Clone(model).(*sdkv1.ModelSelection)
	model.Params[0].Value, returned.Params[0].Value = "input-mutated", "output-mutated"
	e.Close()
	e, err = NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := e.ResumeAgent(id, &sdkv1.AgentOptions{ApiKey: "fake-new"})
	if err != nil || !proto.Equal(resumed, expected) {
		t.Fatalf("model parameters lost or aliased across restart: %v", err)
	}
	if record := e.agents[id]; record.mode != sdkv1.AgentModeOption_AGENT_MODE_OPTION_PLAN || !record.modePending {
		t.Fatal("restart consumed a mode before any native turn")
	}
	resumed.Params[0].Value = "resumed-output-mutated"
	if !proto.Equal(e.agents[id].model, expected) {
		t.Fatal("resume result aliases the stored model")
	}
}
