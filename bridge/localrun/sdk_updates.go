package localrun

import (
	"slices"

	"google.golang.org/protobuf/types/known/structpb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Per-run normalized SDK callbacks, matching the pinned accumulator's completed-step boundaries.
 */
type runUpdateCallbacks struct {
	deltas    bool                                ///< Whether raw normalized interaction callbacks are requested.
	steps     bool                                ///< Whether completed conversation-step callbacks are requested.
	current   []conversationStep                  ///< Adjacent immutable text/thinking/tool steps for this turn.
	lastFired int                                 ///< Last completed step delivered to the client.
	emit      func(*sdkv1.RunStreamMessage) error ///< Serialized durable journal and SDK event writer.
}

/**
 * @brief Emit the current completed step once, before its following update callback.
 * @return Structured payload or client delivery error.
 */
func (c *runUpdateCallbacks) flushStep() error {
	index := len(c.current) - 1
	if !c.steps || index < 0 || index <= c.lastFired {
		return nil
	}
	step := c.current[index]
	value, err := structpb.NewStruct(map[string]any{"type": step.Type, "message": step.Message})
	if err != nil {
		return err
	}
	if err := c.emit(&sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_Step{Step: &sdkv1.ConversationStep{Type: step.Type, Step: value}}}); err != nil {
		return err
	}
	c.lastFired = index
	return nil
}

/**
 * @brief Merge native updates and expose requested callbacks without creating executable tool actions.
 * @param update Normalized metadata from the native protocol decoder.
 * @return Structured payload, durable save or client delivery error.
 */
func (c *runUpdateCallbacks) apply(update map[string]any) error {
	kind, _ := update["type"].(string)
	summary := kind == "summary" || kind == "summary-started" || kind == "summary-completed"
	if len(c.current) > 0 && kind != "token-delta" && !summary {
		last := c.current[len(c.current)-1].Type
		if last == "assistantMessage" && kind != "text-delta" || last == "thinkingMessage" && kind != "thinking-delta" && kind != "thinking-completed" {
			if err := c.flushStep(); err != nil {
				return err
			}
		}
	}
	switch kind {
	case "user-message-appended":
		c.current, c.lastFired = nil, -1
	case "text-delta", "thinking-delta":
		stepType := "assistantMessage"
		if kind == "thinking-delta" {
			stepType = "thinkingMessage"
		}
		text, _ := update["text"].(string)
		if n := len(c.current); n > 0 && c.current[n-1].Type == stepType {
			previous, _ := c.current[n-1].Message["text"].(string)
			c.current[n-1].Message["text"] = previous + text
		} else {
			c.current = append(c.current, conversationStep{Type: stepType, Message: map[string]any{"text": text}})
		}
	case "thinking-completed":
		if n := len(c.current); n > 0 && c.current[n-1].Type == "thinkingMessage" {
			c.current[n-1].Message["thinkingDurationMs"] = update["thinkingDurationMs"]
		}
		if err := c.flushStep(); err != nil {
			return err
		}
	case "tool-call-completed":
		tool, _ := update["toolCall"].(map[string]any)
		c.current = append(c.current, conversationStep{Type: "toolCall", Message: tool})
		if err := c.flushStep(); err != nil {
			return err
		}
	}
	if c.deltas && !summary {
		value, err := structpb.NewStruct(update)
		if err != nil {
			return err
		}
		return c.emit(&sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_InteractionUpdate{InteractionUpdate: &sdkv1.InteractionUpdate{Type: kind, Update: value}}})
	}
	return nil
}

/**
 * @brief Return the final assistant step as the SDK result while preserving earlier streamed progress in history.
 * @return Last assistant message text in the current turn, or empty when no assistant step completed.
 */
func (c *runUpdateCallbacks) finalAssistantText() string {
	for _, step := range slices.Backward(c.current) {
		if step.Type == "assistantMessage" {
			text, _ := step.Message["text"].(string)
			return text
		}
	}
	return ""
}

/**
 * @brief Preserve SDK lifecycle messages omitted by the legacy text/usage decoder.
 * @param update Normalized thinking-completion, summary or tool metadata.
 * @param agentID Public SDK agent identity.
 * @param runID Public SDK run identity.
 * @return Structured payload, durable save or client delivery error.
 * @note Tool metadata remains non-executable; only the authenticated native custom-tool callback runs tools.
 */
func (c *runUpdateCallbacks) lifecycle(update map[string]any, agentID, runID string) error {
	kind, _ := update["type"].(string)
	payload := map[string]any{"agent_id": agentID, "run_id": runID}
	sdkType := ""
	switch kind {
	case "thinking-completed":
		sdkType, payload["text"], payload["thinking_duration_ms"] = "thinking", "", update["thinkingDurationMs"]
	case "summary-started":
		sdkType, payload["status"] = "summary", "running"
	case "summary-completed":
		sdkType, payload["status"] = "summary", "completed"
		if update["failed"] == true {
			payload["status"] = "error"
		}
		if update["fallback"] == true {
			payload["fallback"] = true
		}
	case "tool-call-started", "partial-tool-call", "tool-call-completed":
		tool, _ := update["toolCall"].(map[string]any)
		sdkType, payload["call_id"], payload["name"], payload["status"] = "tool_call", update["callId"], tool["type"], "running"
		if name, ok := tool["name"].(string); ok {
			payload["name"] = name
		}
		if kind == "tool-call-completed" {
			payload["status"] = "completed"
		}
		for _, field := range []string{"args", "result", "truncated"} {
			if value, ok := tool[field]; ok {
				payload[field] = value
			}
		}
	}
	if sdkType == "" {
		return nil
	}
	message, err := sdkMessage(sdkType, payload)
	if err != nil {
		return err
	}
	return c.emit(message)
}
