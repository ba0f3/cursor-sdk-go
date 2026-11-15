package localrun

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

/**
 * @brief Small references to one durable native checkpoint and its independently stored immutable values.
 */
type nativeReferences struct {
	Checkpoint string                     `json:"checkpoint"` ///< SHA-256 checkpoint object identity.
	Controls   *agentwire.ControlManifest `json:"controls"`   ///< Blob and image references, without inline contents.
}

/**
 * @brief Atomically publish a private file only after its data is durable.
 * @param directory Existing private destination directory.
 * @param destination Final filename in that directory.
 * @param data Complete immutable file contents.
 * @return Write, flush, rename or directory durability failure.
 */
func writeStateFile(directory, destination string, data []byte) error {
	file, err := os.CreateTemp(directory, ".state-write-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer func() { _ = file.Close(); _ = os.Remove(name) }()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(directory, destination)); err != nil {
		return err
	}
	return syncStateDirectory(directory)
}

/**
 * @brief Locate one agent's private immutable objects without sharing mutable state across agents.
 * @param id Validated internal agent identifier.
 * @return Object directory under the engine's exclusively owned state root.
 */
func (e *Engine) agentObjectDirectory(id string) string {
	return filepath.Join(e.stateRoot, "agent-"+id+".objects")
}

/**
 * @brief Check a persisted object reference before using it as a filename.
 * @param ref Lowercase SHA-256 hexadecimal identity.
 * @return Whether the reference is canonical and cannot escape the private object directory.
 */
func validObjectReference(ref string) bool {
	decoded, err := hex.DecodeString(ref)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == ref
}

/**
 * @brief Reject durable writes after the process has released ownership of its state root.
 * @return FailedPrecondition without a live writer lease.
 * @note Caller holds the registry mutex or is initializing the engine before publication.
 */
func (e *Engine) requireStateWriter() error {
	if e.stateRoot == "" || e.stateLease == nil {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor runtime no longer owns durable state"))
	}
	return nil
}

/**
 * @brief Verify an existing immutable object without allocating another full context-sized buffer.
 * @param path Existing regular object file.
 * @param ref Expected SHA-256 content hash.
 * @param size Expected byte length.
 * @return Whether all expected bytes were read and match the reference.
 */
func objectContentMatches(path, ref string, size int64) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, size+1))
	return err == nil && written == size && hex.EncodeToString(hash.Sum(nil)) == ref
}

/**
 * @brief Store an immutable object once; unchanged values never replace or rewrite their files.
 * @param id Owning agent identifier.
 * @param data Reader-owned value; this function does not retain its memory.
 * @return SHA-256 object identity, or empty on failure.
 * @return Filesystem or existing-object inconsistency error.
 */
func (e *Engine) putAgentObject(id string, data []byte) (string, error) {
	if err := e.requireStateWriter(); err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	ref := hex.EncodeToString(sum[:])
	directory := e.agentObjectDirectory(id)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	path := filepath.Join(directory, ref)
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return "", errors.New("invalid existing cursor context object")
		}
		if info.Size() == int64(len(data)) && objectContentMatches(path, ref, int64(len(data))) {
			// A prior rename may have succeeded before its directory sync failed.
			// Retry that durability barrier before acknowledging an existing name.
			if err := syncStateDirectory(directory); err != nil {
				return "", err
			}
			return ref, nil
		}
		// The caller still owns bytes whose hash proves the correct contents.
		// Repair the private regular file atomically before any eviction or acknowledgement.
		if err := writeStateFile(directory, ref, data); err != nil {
			return "", err
		}
		return ref, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := writeStateFile(directory, ref, data); err != nil {
		return "", err
	}
	return ref, nil
}

/**
 * @brief Load and verify a bounded immutable object before publishing its contents to a run.
 * @param id Owning agent identifier.
 * @param ref Canonical SHA-256 reference from a validated document.
 * @return Owned object bytes, or nil on failure.
 * @return Invalid reference, size, file type, missing file or content-integrity failure.
 */
func (e *Engine) getAgentObject(id, ref string) ([]byte, error) {
	if !validObjectReference(ref) {
		return nil, errors.New("invalid cursor context object reference")
	}
	path := filepath.Join(e.agentObjectDirectory(id), ref)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 32<<20 {
		return nil, errors.New("cursor context object exceeds size limit or is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, 32<<20+1))
	if err != nil || len(data) > 32<<20 {
		return nil, errors.New("cursor context object exceeds size limit or cannot be read")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != ref {
		return nil, errors.New("cursor context object integrity check failed")
	}
	return data, nil
}

/**
 * @brief Write native values before any metadata can reference them.
 * @param id Owning agent identifier.
 * @param checkpoint Native conversation checkpoint.
 * @param controls Exclusively owned native values and attachments.
 * @return Detached manifest, or nil on failure.
 * @return Object-store failure without changing registry state.
 */
func (e *Engine) storeNativeObjects(id string, checkpoint []byte, controls *agentwire.ControlState) (*nativeReferences, error) {
	checkpointRef, err := e.putAgentObject(id, checkpoint)
	if err != nil {
		return nil, err
	}
	manifest, err := controls.StoreManifest(func(data []byte) (string, error) { return e.putAgentObject(id, data) })
	if err != nil {
		return nil, err
	}
	return &nativeReferences{Checkpoint: checkpointRef, Controls: manifest}, nil
}

/**
 * @brief Rehydrate native context only when an inactive agent is resumed or sent a new turn.
 * @param record Inactive agent, protected by the registry mutex.
 * @return Missing, corrupt or malformed durable context error; receiver is unchanged on failure.
 */
func (e *Engine) hydrateNativeLocked(record *agentRecord) error {
	if !record.nativeEvicted {
		return nil
	}
	if record.native == nil {
		return errors.New("cursor agent has no durable native context")
	}
	checkpoint, err := e.getAgentObject(record.id, record.native.Checkpoint)
	if err != nil {
		return err
	}
	if err := agentwire.ValidateCheckpoint(checkpoint); err != nil {
		return errors.New("invalid cursor durable checkpoint")
	}
	controls, err := record.native.Controls.Restore(func(ref string) ([]byte, error) { return e.getAgentObject(record.id, ref) })
	if err != nil {
		return err
	}
	record.checkpoint, record.controls, record.nativeEvicted = checkpoint, controls, false
	return nil
}

/**
 * @brief Drop native values from a detached inactive agent after confirming a durable copy exists.
 * @param record Agent protected by the registry mutex.
 * @return Durable-save failure; failures leave the native bytes in memory.
 */
func (e *Engine) evictNativeLocked(record *agentRecord) error {
	if e.stateRoot == "" || !record.detached {
		return nil
	}
	for _, owner := range e.runAgents {
		if owner == record.id {
			return nil
		}
	}
	if !record.nativeEvicted {
		if err := e.saveAgentLocked(record); err != nil {
			return err
		}
		record.checkpoint, record.controls, record.nativeEvicted = nil, nil, true
	}
	for runID, snapshot := range e.snapshots {
		if snapshot.AgentId == record.id {
			e.releaseDetachedHistoryLocked(runID)
		}
	}
	return nil
}

/**
 * @brief Load a completed replay journal on demand with strict offsets, ownership and aggregate bounds.
 * @param runID Registered run whose history is protected by the registry mutex.
 * @return Missing or corrupt durable history error; the cached history is unchanged on failure.
 */
func (e *Engine) hydrateHistoryLocked(runID string) error {
	history := e.histories[runID]
	if history == nil || !history.evicted {
		return nil
	}
	snapshot := e.snapshots[runID]
	if snapshot == nil {
		return errors.New("cursor durable history has no owning run")
	}
	loaded := newRunHistory()
	for _, ref := range history.objectRefs {
		data, err := e.getAgentObject(snapshot.AgentId, ref)
		if err != nil {
			return err
		}
		event := &sdkv1.RunStreamMessage{}
		if err := proto.Unmarshal(data, event); err != nil {
			return errors.New("invalid cursor durable run event")
		}
		offset := event.GetOffset()
		event.Offset = nil
		copy, err := loaded.append(event, true)
		if err != nil || copy.GetOffset() != offset {
			return errors.New("invalid cursor durable run event offset or size")
		}
	}
	if history.finished {
		if len(loaded.events) < 2 {
			return errors.New("cursor durable run history has no terminal events")
		}
		result, done := loaded.events[len(loaded.events)-2].GetResult(), loaded.events[len(loaded.events)-1].GetDone()
		if result == nil || done == nil || result.GetRunId() != runID || done.GetRunId() != runID || result.GetAgentId() != snapshot.AgentId || done.GetAgentId() != snapshot.AgentId || result.GetStatus() != snapshot.Status {
			return errors.New("invalid cursor durable run terminal identity or status")
		}
	}
	history.events, history.bytes, history.evicted = loaded.events, loaded.bytes, false
	return nil
}

/**
 * @brief Release a detached agent's completed journal after an observer owns independent copies.
 * @param runID Registered history protected by the registry mutex.
 */
func (e *Engine) releaseDetachedHistoryLocked(runID string) {
	if e.stateRoot == "" {
		return
	}
	snapshot, history := e.snapshots[runID], e.histories[runID]
	if snapshot == nil || history == nil || !history.finished || history.evicted || len(history.objectRefs) != len(history.events) {
		return
	}
	if record := e.agents[snapshot.AgentId]; record != nil && record.detached {
		history.events, history.bytes, history.evicted = nil, 0, true
	}
}

/**
 * @brief Remove objects superseded by a committed metadata document, including interrupted writes.
 * @param saved Fully committed document whose references are authoritative.
 * @note Cleanup failures retain extra objects and are logged; they never invalidate the committed state.
 */
func (e *Engine) pruneAgentObjects(saved *savedAgent) {
	if e.requireStateWriter() != nil {
		return
	}
	keep := make(map[string]bool)
	if saved.Native != nil {
		keep[saved.Native.Checkpoint] = true
		for _, ref := range saved.Native.Controls.References() {
			keep[ref] = true
		}
	}
	for _, refs := range saved.EventRefs {
		for _, ref := range refs {
			keep[ref] = true
		}
	}
	for _, ref := range saved.ToolReceipts {
		keep[ref] = true
	}
	directory := e.agentObjectDirectory(saved.ID)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err == nil {
		for _, entry := range entries {
			if keep[entry.Name()] {
				continue
			}
			if err = os.Remove(filepath.Join(directory, entry.Name())); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = syncStateDirectory(directory)
	}
	if err != nil {
		log.Printf("[WARN] failed to prune Cursor context objects for agent %s: %v", saved.ID, err)
	}
}

/**
 * @brief Validate all object identities in untrusted durable metadata before opening referenced files.
 * @param saved Decoded manifest document.
 * @return Invalid native metadata or object reference error.
 */
func validateSavedReferences(saved *savedAgent) error {
	if len(saved.ToolReceipts) > maxToolReceipts {
		return errors.New("cursor durable tool receipt count exceeds limit")
	}
	for id := range saved.ToolReceipts {
		if len(id) == 0 || len(id) > maxToolCallIDBytes {
			return errors.New("invalid cursor durable tool receipt identity")
		}
	}
	if saved.Native == nil || !validObjectReference(saved.Native.Checkpoint) {
		return errors.New("invalid cursor native context manifest")
	}
	if err := saved.Native.Controls.Validate(); err != nil {
		return err
	}
	refs := saved.Native.Controls.References()
	for _, ref := range saved.ToolReceipts {
		refs = append(refs, ref)
	}
	for _, events := range saved.EventRefs {
		if len(events) > 65536 {
			return errors.New("cursor durable run event references exceed count limit")
		}
		refs = append(refs, events...)
	}
	for _, ref := range refs {
		if !validObjectReference(ref) {
			return errors.New("invalid cursor durable object reference")
		}
	}
	return nil
}

/**
 * @brief Finish interrupted agent deletions without touching directories outside the owned filename format.
 * @return Filesystem or directory durability error.
 * @note Called while the state-root writer lease is held, after all agent metadata has been loaded.
 */
func (e *Engine) removeOrphanAgentObjects() error {
	if err := e.requireStateWriter(); err != nil {
		return err
	}
	entries, err := os.ReadDir(e.stateRoot)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(name, "agent-") || !strings.HasSuffix(name, ".objects") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, "agent-"), ".objects")
		decoded, err := hex.DecodeString(id)
		if err != nil || len(decoded) != 16 || e.agents[id] != nil {
			continue
		}
		if err := os.RemoveAll(filepath.Join(e.stateRoot, name)); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncStateDirectory(e.stateRoot)
	}
	return nil
}
