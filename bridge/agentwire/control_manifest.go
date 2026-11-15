package agentwire

import (
	"encoding/base64"
	"errors"
)

/**
 * @brief Small native-state manifest whose opaque values live in an external immutable object store.
 */
type ControlManifest struct {
	Blobs  map[string]string      `json:"blobs"`  ///< Base64 native byte identifiers mapped to object references.
	Images map[string]StoredImage `json:"images"` ///< Virtual attachments without inline image bytes.
}

/**
 * @brief Read-only image metadata with an external data reference.
 */
type StoredImage struct {
	Reference string     `json:"reference"` ///< Immutable image-byte object reference.
	Metadata  InputImage `json:"metadata"`  ///< Image dimensions, media type and virtual path; Data must be empty.
}

/**
 * @brief Store native values individually without encoding or copying all context into one document.
 * @param put Immutable object writer; must finish durable writes before returning a reference.
 * @return Manifest, or nil for absent state or failure.
 * @return Object writer failure.
 * @note Caller exclusively owns the state; put must not retain or mutate the supplied bytes.
 */
func (s *ControlState) StoreManifest(put func([]byte) (string, error)) (*ControlManifest, error) {
	if s == nil {
		return nil, nil
	}
	manifest := &ControlManifest{Blobs: make(map[string]string, len(s.blobs))}
	if s.Images != nil {
		manifest.Images = make(map[string]StoredImage, len(s.Images))
	}
	for id, data := range s.blobs {
		ref, err := put(data)
		if err != nil {
			return nil, err
		}
		manifest.Blobs[base64.StdEncoding.EncodeToString([]byte(id))] = ref
	}
	for path, image := range s.Images {
		ref, err := put(image.Data)
		if err != nil {
			return nil, err
		}
		image.Data = nil
		manifest.Images[path] = StoredImage{Reference: ref, Metadata: image}
	}
	return manifest, nil
}

/**
 * @brief List every immutable object needed by a native control manifest without reading its values.
 * @return Referenced object identities, or nil for an absent manifest.
 */
func (m *ControlManifest) References() []string {
	if m == nil {
		return nil
	}
	refs := make([]string, 0, len(m.Blobs)+len(m.Images))
	for _, ref := range m.Blobs {
		refs = append(refs, ref)
	}
	for _, image := range m.Images {
		refs = append(refs, image.Reference)
	}
	return refs
}

/**
 * @brief Validate metadata before allocating native blob or image contents.
 * @return Invalid identifiers, metadata or entry-limit error; nil for a valid or absent manifest.
 */
func (m *ControlManifest) Validate() error {
	if m == nil {
		return nil
	}
	if len(m.Blobs) > 1024 || len(m.Images) > 128 {
		return errors.New("cursor control manifest exceeds entry limit")
	}
	for id := range m.Blobs {
		decoded, err := base64.StdEncoding.DecodeString(id)
		if err != nil || len(decoded) == 0 {
			return errors.New("invalid cursor manifest blob identifier")
		}
	}
	for path, image := range m.Images {
		if path != image.Metadata.Path || len(image.Metadata.Data) != 0 {
			return errors.New("invalid cursor manifest image metadata")
		}
	}
	return nil
}

/**
 * @brief Restore detached context on demand and rebuild storage accounting from actual object bytes.
 * @param get Bounded immutable object reader which verifies each reference and its content.
 * @return Restored state, or nil for an absent manifest or on failure.
 * @return Corrupt metadata, missing object, reader or aggregate-size error.
 */
func (m *ControlManifest) Restore(get func(string) ([]byte, error)) (*ControlState, error) {
	if err := m.Validate(); err != nil || m == nil {
		return nil, err
	}
	state := &ControlState{blobs: make(map[string][]byte, len(m.Blobs))}
	if m.Images != nil {
		state.Images = make(map[string]InputImage, len(m.Images))
	}
	for encodedID, ref := range m.Blobs {
		id, _ := base64.StdEncoding.DecodeString(encodedID) // Validated before loading any value.
		data, err := get(ref)
		if err != nil {
			return nil, err
		}
		state.bytes += len(data)
		if state.bytes > 32<<20 {
			return nil, errors.New("cursor manifest blob storage exceeds size limit")
		}
		state.blobs[string(id)] = data
	}
	var imageBytes int
	for path, saved := range m.Images {
		data, err := get(saved.Reference)
		if err != nil {
			return nil, err
		}
		imageBytes += len(data)
		if imageBytes > 32<<20 {
			return nil, errors.New("cursor manifest image storage exceeds size limit")
		}
		image := saved.Metadata
		image.Data = data
		state.Images[path] = image
	}
	return state, nil
}
