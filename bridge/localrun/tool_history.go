package localrun

import (
	"context"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Retain actual native callback metadata without synthesizing executable client actions.
 * @param runID Owning run identity.
 * @param agentID Owning SDK agent.
 * @param call Decoded native custom-tool callback, preserving arguments and both exec identities.
 * @param status Callback lifecycle stage.
 * @param result Structured callback result, absent before execution.
 * @param failed Tool-declared failure flag.
 * @param callErr Callback transport or cancellation failure, absent on success.
 * @param emit Serialized SDK event delivery which also appends the replay journal.
 * @return Journal, durable state or message encoding error; context.Canceled if the agent was deleted.
 * @note This only records history; execution remains exclusively in invokeTool and its authenticated callback.
 */
func (e *Engine) recordToolHistory(runID, agentID string, call *agentwire.ToolCall, status string, result map[string]any, failed bool, callErr error, emit func(*sdkv1.RunStreamMessage) error) error {
	payload := map[string]any{"agent_id": agentID, "run_id": runID, "name": call.Name, "call_id": call.CallID, "exec_id": call.ExecID, "exec_number": call.ID, "args": call.Args.AsMap(), "status": status}
	if result != nil {
		payload["result"] = result
	}
	if failed || callErr != nil {
		payload["status"] = "error"
		payload["is_error"] = true
	}
	if callErr != nil {
		payload["error"] = callErr.Error()
	}
	message, err := sdkMessage("tool_call", payload)
	if err != nil {
		return err
	}
	if err = emit(message); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	record := e.agents[agentID]
	if record == nil {
		return context.Canceled
	}
	return e.saveAgentLocked(record)
}
