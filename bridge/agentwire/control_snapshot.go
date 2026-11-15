package agentwire

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

/**
 * @brief Durable native context data; excludes callback identities and credentials.
 */
type controlSnapshot struct {
	Blobs  map[string][]byte     `json:"blobs"`  ///< Base64 byte identifiers mapped to opaque values.
	Images map[string]InputImage `json:"images"` ///< Read-only attachments referenced by native history.
}

/**
 * @brief Serialize run-owned control state without losing private KV values.
 * @return JSON checkpoint data.
 * @return JSON encoding error, or nil on success.
 * @note Caller must own the state exclusively.
 */
func (s ControlState) MarshalJSON() ([]byte, error) {
	blobs := make(map[string][]byte, len(s.blobs))
	for id, value := range s.blobs {
		blobs[base64.StdEncoding.EncodeToString([]byte(id))] = value
	}
	return json.Marshal(controlSnapshot{Blobs: blobs, Images: s.Images})
}

/**
 * @brief Restore bounded checkpoint data and rebuild accounting rather than trusting serialized counters.
 * @param data JSON checkpoint data.
 * @return Malformed JSON or storage-bound violation; leaves the receiver unchanged on failure.
 */
func (s *ControlState) UnmarshalJSON(data []byte) error {
	var state controlSnapshot
	if err := json.Unmarshal(data, &state); err != nil {
		return errors.New("invalid cursor control snapshot")
	}
	if len(state.Blobs) > 1024 || len(state.Images) > 128 {
		return errors.New("cursor control snapshot exceeds entry limit")
	}
	var blobBytes, imageBytes int
	blobs := make(map[string][]byte, len(state.Blobs))
	for key, value := range state.Blobs {
		id, err := base64.StdEncoding.DecodeString(key)
		if err != nil || len(id) == 0 {
			return errors.New("invalid cursor blob identifier")
		}
		blobs[string(id)] = value
		blobBytes += len(value)
	}
	for path, image := range state.Images {
		if path != image.Path {
			return errors.New("cursor image snapshot path mismatch")
		}
		imageBytes += len(image.Data)
	}
	if blobBytes > 32<<20 || imageBytes > 32<<20 {
		return errors.New("cursor control snapshot exceeds size limit")
	}
	*s = ControlState{blobs: blobs, Images: state.Images, bytes: blobBytes}
	return nil
}
