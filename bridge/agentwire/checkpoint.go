package agentwire

import (
	"bytes"
)

/**
 * @brief Validate opaque conversation structure bytes when restoring durable state.
 * @param data ConversationStateStructure protobuf bytes.
 * @return Malformed wire error, or nil for a valid or empty structure.
 */
func ValidateCheckpoint(data []byte) error { _, err := decodeFields(data); return err }

/**
 * @brief Decode the opaque ConversationStateStructure supplied by Cursor for subsequent turns.
 * @param payload Native AgentServerMessage.
 * @return Detached checkpoint bytes, or nil for other messages.
 * @return True when a checkpoint update is present, including an empty checkpoint.
 * @return Error when the envelope or checkpoint is malformed.
 */
func DecodeCheckpoint(payload []byte) ([]byte, bool, error) {
	field, ok, err := nativeAgentMessage(payload)
	if err != nil {
		return nil, false, err
	}
	if !ok || field.Number != 3 {
		return nil, false, nil
	}
	if _, err := nativeBoundaryFields(field.Bytes); err != nil {
		return nil, true, err
	}
	return bytes.Clone(field.Bytes), true, nil
}

/**
 * @brief Detach retained checkpoint blobs and attachments before starting another turn.
 * @return Independent state owned by one frame reader; no concurrent access is permitted.
 */
func (s *ControlState) Clone() ControlState {
	clone := ControlState{blobs: make(map[string][]byte), Images: make(map[string]InputImage)}
	if s == nil {
		return clone
	}
	for key, value := range s.blobs {
		clone.blobs[key] = bytes.Clone(value)
	}
	for path, image := range s.Images {
		image.Data = bytes.Clone(image.Data)
		clone.Images[path] = image
	}
	clone.bytes = s.bytes
	return clone
}
