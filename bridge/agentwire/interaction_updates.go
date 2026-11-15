package agentwire

import (
	"errors"
	"slices"
)

/**
 * @brief Select the last matching protobuf field without exposing or changing its payload.
 * @param fields Decoded fields in wire order.
 * @param number Desired field identity.
 * @return Last matching field and presence flag, preserving protobuf overwrite semantics.
 */
func lastSDKField(fields []Field, number int) (Field, bool) {
	for _, field := range slices.Backward(fields) {
		if field.Number == number {
			return field, true
		}
	}
	return Field{}, false
}

/**
 * @brief Select the active native interaction oneof while excluding its independent timestamp metadata.
 * @param fields Native InteractionUpdate fields in wire order.
 * @return Active known oneof member with repeated same-member message fragments merged.
 * @return Wrong known-member wire type or decoding-limit error.
 */
func sdkInteractionEvent(fields []Field) (Field, error) {
	event, _, err := nativeMessageOneof(fields, nativeInteractionMember)
	return event, err
}

/**
 * @brief Validate known scalar and message field wire types before interpreting an SDK callback payload.
 * @param fields Decoded callback fields; unknown extensions remain ignored.
 * @param bytesTags Known string or nested-message fields.
 * @param numberTags Known integer, enum or boolean fields.
 * @return Wrong known-field wire type error.
 */
func validateSDKUpdateFields(fields []Field, bytesTags, numberTags []int) error {
	for _, field := range fields {
		for _, tag := range bytesTags {
			if field.Number == tag && field.WireType != wireLength {
				return errors.New("invalid cursor interaction string or message wire type")
			}
		}
		for _, tag := range numberTags {
			if field.Number == tag && field.WireType != wireVarint {
				return errors.New("invalid cursor interaction numeric wire type")
			}
		}
	}
	return nil
}

/**
 * @brief Decode a native interaction event into the pinned SDK's normalized, non-executable callback payload.
 * @param payload One AgentServerMessage, not a framed stream.
 * @param sessionID Stable native conversation identity used in user-message metadata.
 * @return Normalized update, or nil for control frames and variants the official SDK does not expose.
 * @return Malformed envelope, nested-depth or tool metadata decoding failure.
 */
func DecodeSDKInteractionUpdate(payload []byte, sessionID string) (map[string]any, error) {
	update, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || update.Number != 1 {
		return nil, err
	}
	return decodeSDKInteractionBody(update.Bytes, sessionID, 0)
}

/**
 * @brief Normalize one native update with bounded task-delta recursion and the official SDK exposure policy.
 * @param data InteractionUpdate protobuf bytes.
 * @param sessionID Native conversation identity.
 * @param depth Current nested task depth.
 * @return Normalized update or nil for unexposed variants.
 * @return Malformed native event or recursion-limit error.
 */
func decodeSDKInteractionBody(data []byte, sessionID string, depth int) (map[string]any, error) {
	if depth > 16 {
		return nil, errors.New("cursor nested task update exceeds depth limit")
	}
	fields, err := nativeBoundaryFields(data)
	if err != nil {
		return nil, err
	}
	event, err := sdkInteractionEvent(fields)
	if err != nil {
		return nil, err
	}
	if event.Number == 0 {
		return nil, nil
	}
	if event.WireType != wireLength {
		return nil, errors.New("invalid cursor interaction update wire type")
	}
	body, err := decodeFields(event.Bytes)
	if err != nil {
		return nil, err
	}
	var bytesTags, numberTags []int
	switch event.Number {
	case 1, 4, 9:
		bytesTags = []int{1}
	case 2, 3, 15:
		bytesTags = []int{1, 2, 3}
	case 7, 12:
		bytesTags = []int{1, 2, 3, 4}
	case 5, 8, 16, 27:
		numberTags = []int{1}
	case 17:
		numberTags = []int{1, 2}
	case 6:
		bytesTags = []int{1}
	case 11:
		bytesTags, numberTags = []int{1}, []int{2, 3}
	case 14:
		bytesTags, numberTags = []int{7}, []int{1, 2, 3, 4, 5, 6}
	}
	if err := validateSDKUpdateFields(body, bytesTags, numberTags); err != nil {
		return nil, err
	}
	text := func(number int) string { value, _ := lastSDKField(body, number); return string(value.Bytes) }
	number := func(tag int) uint64 { value, _ := lastSDKField(body, tag); return value.Varint }
	var update map[string]any
	switch event.Number {
	case 1:
		update = map[string]any{"type": "text-delta", "text": text(1)}
	case 4:
		update = map[string]any{"type": "thinking-delta", "text": text(1)}
	case 5:
		update = map[string]any{"type": "thinking-completed", "thinkingDurationMs": int32(number(1))}
	case 6:
		user, _, err := nativeSingularMessage(body, 1)
		if err != nil {
			return nil, err
		}
		message, err := decodeFields(user.Bytes)
		if err != nil {
			return nil, err
		}
		if err := validateSDKUpdateFields(message, []int{1}, nil); err != nil {
			return nil, err
		}
		value, _ := lastSDKField(message, 1)
		update = map[string]any{"type": "user-message-appended", "userMessage": map[string]any{"type": "user_message", "session_id": sessionID, "text": string(value.Bytes)}}
	case 8:
		update = map[string]any{"type": "token-delta", "tokens": int32(number(1))}
	case 9:
		update = map[string]any{"type": "summary", "summary": text(1)}
	case 10:
		update = map[string]any{"type": "summary-started"}
	case 11:
		update = map[string]any{"type": "summary-completed"}
		if number(2) != 0 {
			update["failed"] = true
		}
		if number(3) != 0 {
			update["fallback"] = true
		}
	case 2, 3, 7:
		tool, _, err := nativeSingularMessage(body, 2)
		if err != nil {
			return nil, err
		}
		decoded, err := DecodeSDKToolCall(tool.Bytes)
		if err != nil || decoded == nil {
			return nil, err
		}
		kind, modelTag := "tool-call-started", 3
		switch event.Number {
		case 3:
			kind = "tool-call-completed"
		case 7:
			kind, modelTag = "partial-tool-call", 4
		}
		update = map[string]any{"type": kind, "callId": text(1), "modelCallId": text(modelTag), "toolCall": decoded}
	case 12:
		decoded, err := DecodeSDKShellOutput(event.Bytes)
		if err != nil {
			return nil, err
		}
		update = map[string]any{"type": "shell-output-delta", "event": decoded}
	case 14:
		update = map[string]any{"type": "turn-ended"}
		if usage := decodeTurnUsage(event.Bytes); usage != nil {
			counts := map[string]any{"inputTokens": usage.InputTokens, "outputTokens": usage.OutputTokens, "cacheReadTokens": usage.CacheReadTokens, "cacheWriteTokens": usage.CacheWriteTokens}
			if usage.ReasoningTokens != nil {
				counts["reasoningTokens"] = *usage.ReasoningTokens
			}
			update["usage"] = counts
		}
	case 15:
		delta, _, err := nativeSingularMessage(body, 2)
		if err != nil {
			return nil, err
		}
		union, err := nativeBoundaryFields(delta.Bytes)
		if err != nil {
			return nil, err
		}
		task, _, err := nativeMessageOneof(union, func(number int) bool { return number >= 1 && number <= 4 })
		if err != nil {
			return nil, err
		}
		if task.Number != 2 {
			return nil, nil
		}
		if task.WireType != wireLength {
			return nil, errors.New("invalid cursor task delta wire type")
		}
		childFields, err := nativeBoundaryFields(task.Bytes)
		if err != nil {
			return nil, err
		}
		if err := validateSDKUpdateFields(childFields, []int{1}, nil); err != nil {
			return nil, err
		}
		child, _, err := nativeSingularMessage(childFields, 1)
		if err != nil {
			return nil, err
		}
		nested, err := decodeSDKInteractionBody(child.Bytes, sessionID, depth+1)
		if err != nil || nested == nil {
			return nil, err
		}
		switch nested["type"] {
		case "text-delta", "thinking-delta", "thinking-completed", "tool-call-started", "tool-call-completed", "partial-tool-call", "step-started", "step-completed", "tool-requests-listed":
			update = map[string]any{"type": "tool-call-delta", "callId": text(1), "modelCallId": text(3), "taskUpdate": nested}
		}
	case 16:
		update = map[string]any{"type": "step-started", "stepId": number(1)}
	case 17:
		update = map[string]any{"type": "step-completed", "stepId": number(1), "stepDurationMs": int64(number(2))}
	case 27:
		update = map[string]any{"type": "tool-requests-listed", "callCount": uint32(number(1))}
	}
	return update, nil
}
