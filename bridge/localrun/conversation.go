package localrun

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief One SDK-normalized conversation turn in chronological order.
 */
type conversationTurn struct {
	Type string            `json:"type"` ///< SDK discriminator, currently agentConversationTurn.
	Turn agentConversation `json:"turn"` ///< User message and merged steps.
}

/**
 * @brief Aggregate SDK deltas without flattening roles or tool correlation identifiers.
 */
type agentConversation struct {
	UserMessage map[string]any     `json:"userMessage,omitempty"` ///< Original user text, when present.
	Steps       []conversationStep `json:"steps"`                 ///< Ordered assistant, thinking and tool steps.
}

/**
 * @brief One SDK conversation step, retaining typed payload objects.
 */
type conversationStep struct {
	Type    string         `json:"type"`    ///< assistantMessage, thinkingMessage or toolCall.
	Message map[string]any `json:"message"` ///< Structured step payload.
}

/**
 * @brief Convert retained SDK events to the official normalized conversation turn shape.
 * @param events Detached or registry-owned immutable events.
 * @return Ordered turns with adjacent text deltas merged; usage and terminal events do not become messages.
 */
func conversationFromEvents(events []*sdkv1.RunStreamMessage) []conversationTurn {
	turns := make([]conversationTurn, 0)
	pendingTools := make(map[string]map[string]any)
	completedTools := make(map[string]int)
	for _, event := range events {
		sdk := event.GetSdkMessage()
		if sdk == nil {
			continue
		}
		payload := sdk.GetMessage().AsMap()
		kind := sdk.GetType()
		if kind != "user" && kind != "assistant" && kind != "thinking" && kind != "tool_call" {
			continue
		}
		if kind == "tool_call" {
			running := payload["status"] == "running"
			if callID, _ := payload["call_id"].(string); callID != "" {
				merged := maps.Clone(pendingTools[callID])
				if merged == nil {
					merged = make(map[string]any)
				}
				maps.Copy(merged, payload)
				pendingTools[callID], payload = merged, merged
			}
			// Running updates retain arguments for a later completion without creating a conversation step.
			if running {
				continue
			}
		}
		text := ""
		switch kind {
		case "user", "assistant":
			message, _ := payload["message"].(map[string]any)
			content, _ := message["content"].([]any)
			for _, item := range content {
				block, _ := item.(map[string]any)
				if block["type"] == "text" {
					value, _ := block["text"].(string)
					text += value
				}
			}
		case "thinking":
			text, _ = payload["text"].(string)
		}
		if kind == "user" {
			turns = append(turns, conversationTurn{Type: "agentConversationTurn", Turn: agentConversation{UserMessage: map[string]any{"text": text}, Steps: make([]conversationStep, 0)}})
			clear(pendingTools)
			clear(completedTools)
			continue
		}
		if len(turns) == 0 {
			turns = append(turns, conversationTurn{Type: "agentConversationTurn", Turn: agentConversation{Steps: make([]conversationStep, 0)}})
		}
		turn := &turns[len(turns)-1].Turn
		if kind == "tool_call" {
			if name, ok := payload["name"].(string); ok {
				payload["type"] = name
			}
			callID, _ := payload["call_id"].(string)
			index, completed := completedTools[callID]
			if !completed || callID == "" {
				if callID != "" {
					completedTools[callID] = len(turn.Steps)
				}
				turn.Steps = append(turn.Steps, conversationStep{Type: "toolCall", Message: maps.Clone(payload)})
			} else {
				maps.Copy(turn.Steps[index].Message, payload)
			}
			continue
		}
		if kind == "thinking" {
			if duration, ok := payload["thinking_duration_ms"]; ok {
				if n := len(turn.Steps); n > 0 && turn.Steps[n-1].Type == "thinkingMessage" {
					turn.Steps[n-1].Message["thinkingDurationMs"] = duration
				}
			}
		}
		if text == "" {
			continue
		}
		stepType := "assistantMessage"
		if kind == "thinking" {
			stepType = "thinkingMessage"
		}
		if n := len(turn.Steps); n > 0 && turn.Steps[n-1].Type == stepType {
			previous, _ := turn.Steps[n-1].Message["text"].(string)
			turn.Steps[n-1].Message["text"] = previous + text
		} else {
			turn.Steps = append(turn.Steps, conversationStep{Type: stepType, Message: map[string]any{"text": text}})
		}
	}
	return turns
}

/**
 * @brief Return the completed run's conversation using the official normalized SDK shape.
 * @param ctx Observer lifetime while waiting for completion.
 * @param id Registered or retained run identity.
 * @return JSON turn array, including partial conversation for failed/canceled generation.
 * @return Observer, missing-run or JSON encoding error.
 */
func (e *Engine) RunConversation(ctx context.Context, id string) (string, error) {
	if _, err := e.WaitRun(ctx, id); err != nil {
		return "", err
	}
	e.mu.Lock()
	history := e.histories[id]
	if history == nil {
		e.mu.Unlock()
		return "", connect.NewError(connect.CodeNotFound, errors.New("run history was not found"))
	}
	if err := e.hydrateHistoryLocked(id); err != nil {
		e.mu.Unlock()
		return "", err
	}
	events := make([]*sdkv1.RunStreamMessage, len(history.events))
	for i, event := range history.events {
		events[i] = proto.Clone(event).(*sdkv1.RunStreamMessage)
	}
	e.releaseDetachedHistoryLocked(id)
	e.mu.Unlock()
	data, err := json.Marshal(conversationFromEvents(events))
	return string(data), err
}

/**
 * @brief List local conversation turns using the SDK's full-conversation message wrapper and pagination.
 * @param id Agent identity.
 * @param options Turn offset and optional limit; zero limit returns the remaining turns.
 * @return Detached SDK messages with stable agent/index UUIDs, or nil on invalid input.
 * @return NotFound, unsupported runtime/workspace error, or structured-message encoding failure.
 */
func (e *Engine) AgentMessages(id string, options *sdkv1.GetAgentMessagesOptions) ([]*sdkv1.AgentMessage, error) {
	e.mu.Lock()
	record := e.agents[id]
	if record == nil {
		e.mu.Unlock()
		return nil, connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	if options.GetRuntime() == sdkv1.Runtime_RUNTIME_CLOUD || (options.GetCwd() != "" && strings.TrimSpace(options.GetCwd()) != record.workdir) {
		e.mu.Unlock()
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("message filters do not match the local agent"))
	}
	var snapshots []*sdkv1.RunSnapshot
	for _, snapshot := range e.snapshots {
		if snapshot.AgentId == id {
			snapshots = append(snapshots, snapshot)
		}
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if d := snapshots[i].CreatedAt.AsTime().Compare(snapshots[j].CreatedAt.AsTime()); d != 0 {
			return d < 0
		}
		return snapshots[i].RunId < snapshots[j].RunId
	})
	var events []*sdkv1.RunStreamMessage
	for _, snapshot := range snapshots {
		if history := e.histories[snapshot.RunId]; history != nil {
			if err := e.hydrateHistoryLocked(snapshot.RunId); err != nil {
				e.mu.Unlock()
				return nil, err
			}
			for _, event := range history.events {
				events = append(events, proto.Clone(event).(*sdkv1.RunStreamMessage))
			}
			e.releaseDetachedHistoryLocked(snapshot.RunId)
		}
	}
	e.mu.Unlock()
	turns := conversationFromEvents(events)
	start := min(uint64(options.GetOffset()), uint64(len(turns)))
	end := uint64(len(turns))
	if options.GetLimit() > 0 {
		end = min(start+uint64(options.GetLimit()), end)
	}
	messages := make([]*sdkv1.AgentMessage, 0, end-start)
	for i := start; i < end; i++ {
		turn := turns[i].Turn
		steps := make([]any, 0, len(turn.Steps))
		for _, step := range turn.Steps {
			steps = append(steps, map[string]any{step.Type: step.Message})
		}
		body := map[string]any{"steps": steps}
		kind := "assistant"
		if turn.UserMessage != nil {
			body["userMessage"] = turn.UserMessage
			kind = "user"
		}
		payload, err := structpb.NewStruct(map[string]any{"agentConversationTurn": body})
		if err != nil {
			return nil, err
		}
		messages = append(messages, &sdkv1.AgentMessage{Type: kind, Uuid: id + ":" + strconv.FormatUint(i, 10), AgentId: id, Message: payload})
	}
	return messages, nil
}
