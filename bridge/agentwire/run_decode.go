package agentwire

import (
	"bytes"
	"fmt"
	"math"
)

/**
 * @brief Optional upstream counters for a completed native turn, with cache included in input.
 */
type TurnUsage struct {
	InputTokens      int64  ///< Total input including cache reads and writes.
	OutputTokens     int64  ///< Generated output tokens.
	CacheReadTokens  int64  ///< Tokens read from upstream cache.
	CacheWriteTokens int64  ///< Tokens written to upstream cache.
	ReasoningTokens  *int64 ///< Optional reasoning token count.
}

/**
 * @brief Decode optional TurnEndedUpdate counters without guessing absent usage.
 * @param data Native turn-ended payload.
 * @return Valid counters or nil when absent or malformed.
 */
func decodeTurnUsage(data []byte) *TurnUsage {
	fields, err := decodeFields(data)
	if err != nil {
		return nil
	}
	u := &TurnUsage{}
	seen := false
	for _, f := range fields {
		if f.Number < 1 || f.Number > 5 {
			continue
		}
		if f.WireType != wireVarint || f.Varint > math.MaxInt64 {
			return nil
		}
		seen = true
		v := int64(f.Varint)
		switch f.Number {
		case 1:
			u.InputTokens = v
		case 2:
			u.OutputTokens = v
		case 3:
			u.CacheReadTokens = v
		case 4:
			u.CacheWriteTokens = v
		case 5:
			u.ReasoningTokens = &v
		}
	}
	if !seen || u.InputTokens > math.MaxInt64-u.OutputTokens {
		return nil
	}
	return u
}

/**
 * @brief One assistant-visible delta decoded from an AgentServerMessage payload.
 */
type StreamDelta struct {
	Text     string
	Thinking string
	Done     bool
	Usage    *TurnUsage ///< Real upstream usage, when supplied on turn completion.
}

const (
	agentServerInteractionUpdate = 1
	interactionTextDelta         = 1
	interactionThinkingDelta     = 4
	interactionTurnEnded         = 14
)

/**
 * @brief Decode assistant text and thinking deltas from one AgentServerMessage frame.
 */
func DecodeStreamDeltas(payload []byte) []StreamDelta {
	var out []StreamDelta
	update, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || update.Number != agentServerInteractionUpdate {
		return nil
	}
	fields, err := nativeBoundaryFields(update.Bytes)
	if err != nil {
		return nil
	}
	inner, err := sdkInteractionEvent(fields)
	if err != nil {
		return nil
	}
	switch inner.Number {
	case interactionTextDelta, interactionThinkingDelta:
		if inner.WireType == wireLength {
			value, _ := lastSDKField(decodeAll(inner.Bytes), 1)
			if text := string(value.Bytes); text != "" {
				if inner.Number == interactionTextDelta {
					out = append(out, StreamDelta{Text: text})
				} else {
					out = append(out, StreamDelta{Thinking: text})
				}
			}
		}
	case interactionTurnEnded:
		out = append(out, StreamDelta{Done: true, Usage: decodeTurnUsage(inner.Bytes)})
	}
	return out
}

/**
 * @brief Identify an interaction update for content-free stream diagnostics.
 * @param payload AgentServerMessage wire bytes.
 * @return Numeric update variant, or zero for another message or malformed envelope.
 */
func InteractionUpdateKind(payload []byte) int {
	update, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || update.Number != 1 {
		return 0
	}
	fields, err := nativeBoundaryFields(update.Bytes)
	if err != nil {
		return 0
	}
	event, err := sdkInteractionEvent(fields)
	if err != nil {
		return 0
	}
	if event.Number == 0 {
		for _, field := range fields {
			if field.Number != 25 && field.WireType == wireLength {
				return field.Number
			}
		}
	}
	return event.Number
}

/**
 * @brief Run-local blob storage shared by context and tool rounds.
 * @note Owned by one run's frame reader; no concurrent access is permitted.
 */
type ControlState struct {
	MissingBlobs int                   ///< Missing KV reads observed during the current turn; not persisted.
	Images       map[string]InputImage ///< Read-only attachments exposed through native image reads.
	blobs        map[string][]byte     ///< Native content-addressed blob values.
	bytes        int                   ///< Total retained bytes; bounded to 32 MiB per run.
}

/**
 * @brief Extract only the numeric control operation for diagnostics.
 * @param payload One upstream AgentServerMessage.
 * @return Top-level stream field, or zero for non-control messages.
 * @return Operation field, or zero when no operation is present.
 * @return Error if the control envelope is malformed.
 * @note Does not return payload data, identifiers or credential material.
 */
func ControlOperation(payload []byte) (int, int, error) {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid cursor stream message: %w", err)
	}
	if !ok || (top.Number != 2 && top.Number != 4) {
		return 0, 0, nil
	}
	var entries []Field
	var operation Field
	if top.Number == 2 {
		entries, operation, err = nativeExecOperation(top.Bytes)
	} else {
		entries, operation, err = nativeKVOperation(top.Bytes)
	}
	if err != nil {
		return 0, 0, err
	}
	if operation.Number != 0 {
		return top.Number, operation.Number, nil
	}
	// Unknown extensions cannot replace a known operation; unknown-only requests retain an explicit rejection path.
	for _, field := range entries {
		if field.Number == 1 || (top.Number == 2 && (field.Number == 15 || field.Number == 19 || field.Number == 55 || field.Number == 57)) || (top.Number == 4 && field.Number == 4) {
			continue
		}
		return top.Number, field.Number, nil
	}
	return top.Number, 0, nil
}

/**
 * @brief Extract the upstream exec identifier from a control request.
 * @param payload One upstream AgentServerMessage.
 * @return Exec identifier; zero is valid when omitted by protobuf.
 * @return True for an exec request, false for other frames.
 */
func ExecControlID(payload []byte) (uint32, bool) {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || top.Number != 2 {
		return 0, false
	}
	fields, _, err := nativeExecOperation(top.Bytes)
	if err != nil {
		return 0, false
	}
	id, _ := lastSDKField(fields, 1)
	return uint32(id.Varint), true
}

/**
 * @brief Extract an upstream request to abort one active exec operation.
 * @param payload One upstream AgentServerMessage.
 * @return Exec identifier; zero is valid when omitted by protobuf.
 * @return True for an exec abort, false for other messages.
 */
func DecodeExecAbort(payload []byte) (uint32, bool) {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || top.Number != 5 {
		return 0, false
	}
	fields, err := nativeBoundaryFields(top.Bytes)
	if err != nil {
		return 0, false
	}
	abort, ok, err := nativeSingularMessage(fields, 1)
	if err != nil || !ok {
		return 0, false
	}
	fields, err = nativeBoundaryFields(abort.Bytes)
	if err != nil {
		return 0, false
	}
	for _, field := range fields {
		if field.Number == 1 && field.WireType != wireVarint {
			return 0, false
		}
	}
	id, _ := lastSDKField(fields, 1)
	return uint32(id.Varint), true
}

/**
 * @brief Identify an upstream interactive query without exposing its contents.
 * @param payload One upstream AgentServerMessage.
 * @return Numeric query variant, or zero if it is absent.
 * @return True for an interaction query, false for other messages.
 */
func InteractionQuery(payload []byte) (int, bool) {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || top.Number != 7 {
		return 0, false
	}
	fields, err := nativeBoundaryFields(top.Bytes)
	if err != nil {
		return 0, false
	}
	query, ok, err := nativeMessageOneof(fields, nativeQueryMember)
	if err != nil {
		return 0, false
	}
	if ok {
		return query.Number, true
	}
	for _, field := range fields {
		if field.Number != 1 {
			return field.Number, true
		}
	}
	return 0, true
}

/**
 * @brief Answer context, MCP-state and blob control frames without executing host tools.
 * @param payload One upstream AgentServerMessage.
 * @param tools Available client tools advertised during MCP discovery.
 * @return Framed control reply, or nil for non-control and unrecognized frames.
 */
func (s *ControlState) ReplyControl(payload []byte, tools []Tool) []byte {
	top, ok, err := nativeAgentMessage(payload)
	if err != nil || !ok || (top.Number != 2 && top.Number != 4) {
		return nil
	}
	if top.Number == 2 {
		entries, operation, err := nativeExecOperation(top.Bytes)
		if err != nil {
			return nil
		}
		canonical := make([]Field, 0, len(entries)+1)
		for _, field := range entries {
			if !nativeExecMember(field.Number) {
				canonical = append(canonical, field)
			}
		}
		if operation.Number != 0 {
			canonical = append(canonical, operation)
		}
		return s.replyExecControl(canonical, tools)
	}
	entries, operation, err := nativeKVOperation(top.Bytes)
	if err != nil || operation.Number == 0 {
		return nil
	}
	id, _ := lastSDKField(entries, 1)
	args, err := nativeBoundaryFields(operation.Bytes)
	key, _ := lastSDKField(args, 1)
	value, _ := lastSDKField(args, 2)
	var result []byte
	var failure string
	if err == nil {
		for _, field := range args {
			if (field.Number == 1 || (operation.Number == 3 && field.Number == 2)) && field.WireType != wireLength {
				err = fmt.Errorf("invalid cursor KV argument wire type")
			}
		}
	}
	if err != nil || len(key.Bytes) == 0 {
		failure = "Invalid blob identifier"
	} else if operation.Number == 3 {
		previous, exists := s.blobs[string(key.Bytes)]
		if s.bytes-len(previous)+len(value.Bytes) > 32<<20 || (!exists && len(s.blobs) >= 1024) {
			failure = "Run blob storage limit exceeded"
		} else {
			if s.blobs == nil {
				s.blobs = make(map[string][]byte)
			}
			s.blobs[string(key.Bytes)] = bytes.Clone(value.Bytes)
			s.bytes += len(value.Bytes) - len(previous)
		}
	} else if data, exists := s.blobs[string(key.Bytes)]; exists {
		result = encodeBytes(1, data)
	} else {
		s.MissingBlobs++
	}
	if operation.Number == 3 {
		if failure != "" {
			result = encodeMessage(1, encodeString(1, failure))
		}
	} else if failure != "" {
		result = encodeMessage(2, encodeString(1, failure))
	}
	return EncodeConnectDataFrame(encodeMessage(3, encodeUInt32Field(1, uint32(id.Varint)), encodeMessage(operation.Number, result)))
}
