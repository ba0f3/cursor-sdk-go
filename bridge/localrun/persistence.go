package localrun

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Credential-free durable agent document; protobufs use their authoritative binary encoding.
 */
type savedAgent struct {
	Version        int                     `json:"version"`                   ///< Supported storage schema version.
	ID             string                  `json:"id"`                        ///< Stable random agent identity.
	ConversationID string                  `json:"conversation_id"`           ///< Native conversation identity.
	Workdir        string                  `json:"workdir"`                   ///< Client workspace reference, never used to execute host tools.
	ModelID        string                  `json:"model_id,omitempty"`        ///< Legacy selected model ID.
	Fast           bool                    `json:"fast,omitempty"`            ///< Legacy fast model parameter.
	ModelSelection []byte                  `json:"model_selection,omitempty"` ///< Complete model-selection protobuf.
	Mode           sdkv1.AgentModeOption   `json:"mode,omitempty"`            ///< Initial or explicit resume mode.
	ModePending    bool                    `json:"mode_pending,omitempty"`    ///< Whether to seed the configured mode on the next turn.
	ToolPolicy     agentwire.ToolPolicy    `json:"tool_policy"`               ///< Native tool restrictions, preserving explicit empty selection.
	Info           []byte                  `json:"info"`                      ///< Public SDK metadata protobuf.
	Tools          map[string][]byte       `json:"tools"`                     ///< Client tool definition protobufs, without callback credentials.
	Checkpoint     []byte                  `json:"checkpoint,omitempty"`      ///< Legacy inline native conversation structure.
	Controls       *agentwire.ControlState `json:"controls,omitempty"`        ///< Legacy inline native blobs and attachments.
	Runs           map[string][]byte       `json:"runs"`                      ///< Retained SDK run snapshot protobufs.
	Events         map[string][][]byte     `json:"events,omitempty"`          ///< Legacy inline per-run SDK replay protobufs.
	Native         *nativeReferences       `json:"native,omitempty"`          ///< Independently stored native context, without inline contents.
	EventRefs      map[string][]string     `json:"event_refs,omitempty"`      ///< Immutable per-run event object identities in offset order.
	ToolReceipts   map[string]string       `json:"tool_receipts,omitempty"`   ///< Immutable callback receipt objects keyed by native tool call identity.
}

/**
 * @brief Open an exclusively owned durable state root before starting workers or serving requests.
 * @param api Cursor API transport.
 * @param root Stable private directory; credentials are never serialized.
 * @return Restored engine; callers must Close it to release the writer lease.
 * @return Filesystem, conflicting writer, unsupported schema or corrupt state error.
 */
func NewPersistentEngine(api *cursorapi.Client, root string) (*Engine, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lease, err := os.OpenFile(filepath.Join(root, ".writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockStateFile(lease); err != nil {
		_ = lease.Close()
		return nil, errors.New("cursor state root already has an active writer")
	}
	e := NewEngine(api)
	e.stateRoot = root
	e.stateLease = lease
	paths, err := durableMetadataPaths(root, maxDurableMetadataAgents)
	if err != nil {
		e.Close()
		return nil, err
	}
	e.metadataQuota, err = loadDurableMetadataQuota(paths, nil)
	if err != nil {
		e.Close()
		return nil, err
	}
	for _, path := range paths {
		saved, _, err := readDurableMetadataDocument(path)
		if err != nil {
			e.Close()
			return nil, err
		}
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "agent-"), ".json")
		if err := e.restoreAgent(id, saved); err != nil {
			e.Close()
			return nil, err
		}
		if saved.Version == 1 {
			if err := e.evictNativeLocked(e.agents[id]); err != nil {
				e.Close()
				return nil, err
			}
		}
	}
	e.metadataQuota.recovering = false
	if err := e.removeOrphanAgentObjects(); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

/**
 * @brief Restore a validated credential-free agent before exposing it through SDK APIs.
 * @param id Agent identifier extracted from a local filename.
 * @param saved Decoded durable document.
 * @return Schema, identity, protobuf or checkpoint validation error.
 */
func (e *Engine) restoreAgent(id string, saved *savedAgent) error {
	raw, err := hex.DecodeString(id)
	if err != nil || len(raw) != 16 || (saved.Version != 1 && saved.Version != 2) || saved.ID != id || saved.ConversationID == "" {
		return errors.New("invalid cursor durable agent identity or schema")
	}
	if saved.Version == 2 {
		if err := validateSavedReferences(saved); err != nil {
			return err
		}
	}
	info := &sdkv1.SdkAgentInfo{}
	if err := agentwire.ValidateCheckpoint(saved.Checkpoint); err != nil {
		return errors.New("invalid cursor durable checkpoint")
	}
	if err := proto.Unmarshal(saved.Info, info); err != nil || info.GetAgentId() != id {
		return errors.New("invalid cursor durable agent metadata")
	}
	// A process restart cannot keep an upstream run alive.
	if info.Status == sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_RUNNING {
		info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_ERROR
	}
	selection := &sdkv1.ModelSelection{Id: saved.ModelID}
	if saved.Fast {
		selection.Params = []*sdkv1.ModelParameterValue{{Id: "fast", Value: "true"}}
	}
	if len(saved.ModelSelection) > 0 {
		if err := proto.Unmarshal(saved.ModelSelection, selection); err != nil {
			return errors.New("invalid cursor durable model selection")
		}
	}
	if selection.GetId() == "" {
		selection.Id = "default"
	}
	selection, err = resolveModelSelection(selection, nil)
	if err != nil {
		return err
	}
	if _, err := nativeAgentMode(saved.Mode); err != nil {
		return err
	}
	record := &agentRecord{id: id, conversationID: saved.ConversationID, workdir: saved.Workdir, model: selection, mode: saved.Mode, modePending: saved.ModePending, info: info, checkpoint: saved.Checkpoint, controls: saved.Controls, customTools: make(map[string]*sdkv1.CustomToolDefinition), detached: true}
	record.toolReceipts = maps.Clone(saved.ToolReceipts)
	if saved.Version == 2 {
		record.checkpoint, record.controls, record.native, record.nativeEvicted = nil, nil, saved.Native, true
	}
	if saved.ToolPolicy.Allowed != nil {
		if _, err := agentwire.ResolveToolNames(saved.ToolPolicy.Allowed); err != nil {
			return errors.New("invalid cursor durable tool policy")
		}
	}
	if _, err := agentwire.ResolveToolNames(saved.ToolPolicy.Excluded); err != nil {
		return errors.New("invalid cursor durable tool exclusions")
	}
	record.toolPolicy = saved.ToolPolicy
	for name, data := range saved.Tools {
		definition := &sdkv1.CustomToolDefinition{}
		if err := proto.Unmarshal(data, definition); err != nil {
			return errors.New("invalid cursor durable tool definition")
		}
		record.customTools[name] = definition
	}
	for runID, data := range saved.Runs {
		snapshot := &sdkv1.RunSnapshot{}
		if err := proto.Unmarshal(data, snapshot); err != nil || snapshot.GetAgentId() != id || snapshot.GetRunId() != runID {
			return errors.New("invalid cursor durable run snapshot")
		}
		interrupted := snapshot.Status == sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING
		if interrupted {
			snapshot.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_ERROR
			code, message := interruptedRunCode, interruptedRunMessage
			snapshot.ErrorCode = &code
			snapshot.ErrorMessage = &message
		}
		e.snapshots[runID] = snapshot
		history := newRunHistory()
		encoded := saved.Events[runID]
		if saved.Version == 2 {
			history.objectRefs = append([]string(nil), saved.EventRefs[runID]...)
			history.evicted = true
			history.finished = !interrupted
			encoded = nil
		}
		for _, data := range encoded {
			event := &sdkv1.RunStreamMessage{}
			if err := proto.Unmarshal(data, event); err != nil {
				return errors.New("invalid cursor durable run event")
			}
			offset := event.GetOffset()
			event.Offset = nil
			copy, err := history.append(event, true)
			if err != nil || copy.GetOffset() != offset {
				return errors.New("invalid cursor durable run event offset or size")
			}
		}
		if len(history.events) >= 2 && history.events[len(history.events)-1].GetDone() != nil && snapshot.Status != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING {
			history.finished = true
		}
		e.histories[runID] = history
	}
	e.agents[id] = record
	if saved.Version == 2 {
		e.pruneAgentObjects(saved)
	}
	for runID, snapshot := range e.snapshots {
		if snapshot.AgentId == id {
			if history := e.histories[runID]; history != nil && !history.finished && snapshot.Status != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING {
				if _, err := e.sealRunHistoryLocked(runID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

/**
 * @brief Atomically replace one durable document while holding the registry mutex.
 * @param record Agent whose execution options and native state are stable under the mutex.
 * @return Filesystem, size or protobuf encoding error; nil for an in-memory engine.
 */
func (e *Engine) saveAgentLocked(record *agentRecord) error {
	if e.stateRoot == "" {
		return nil
	}
	if err := e.requireStateWriter(); err != nil {
		return err
	}
	if err := e.requireMetadataQuotaLocked(); err != nil {
		return err
	}
	saved := savedAgent{Version: 2, ID: record.id, ConversationID: record.conversationID, Workdir: record.workdir, Mode: record.mode, ModePending: record.modePending, Tools: make(map[string][]byte), Runs: make(map[string][]byte), Native: record.native}
	saved.EventRefs = make(map[string][]string)
	saved.ToolPolicy = record.toolPolicy
	saved.ToolReceipts = maps.Clone(record.toolReceipts)
	var err error
	saved.ModelSelection, err = proto.Marshal(record.model)
	if err != nil {
		return err
	}
	if !record.nativeEvicted {
		saved.Native, err = e.storeNativeObjects(record.id, record.checkpoint, record.controls)
		if err != nil {
			return err
		}
	}
	saved.Info, err = proto.Marshal(record.info)
	if err != nil {
		return err
	}
	for name, definition := range record.customTools {
		data, err := proto.Marshal(definition)
		if err != nil {
			return err
		}
		saved.Tools[name] = data
	}
	var recoveryReserve int64
	for runID, snapshot := range e.snapshots {
		if snapshot.GetAgentId() == record.id {
			data, err := proto.Marshal(snapshot)
			if err != nil {
				return err
			}
			saved.Runs[runID] = data
			if history := e.histories[runID]; history != nil {
				if history.evicted {
					saved.EventRefs[runID] = append([]string(nil), history.objectRefs...)
				} else {
					if len(history.objectRefs) > len(history.events) {
						history.objectRefs = history.objectRefs[:len(history.events)]
					}
					for _, event := range history.events[len(history.objectRefs):] {
						data, err := proto.Marshal(event)
						if err != nil {
							return err
						}
						ref, err := e.putAgentObject(record.id, data)
						if err != nil {
							return err
						}
						history.objectRefs = append(history.objectRefs, ref)
					}
					saved.EventRefs[runID] = append([]string(nil), history.objectRefs...)
				}
			}
			reserve, err := metadataRunRecoveryReserve(snapshot, data, saved.EventRefs)
			if err != nil {
				return err
			}
			recoveryReserve += reserve
		}
	}
	data, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	if err := e.metadataQuota.check(record.id, int64(len(data)), recoveryReserve); err != nil {
		return err
	}
	if err := writeStateFile(e.stateRoot, "agent-"+record.id+".json", data); err != nil {
		return e.metadataMutationErrorLocked(err)
	}
	e.metadataQuota.commit(record.id, int64(len(data)), recoveryReserve)
	record.native = saved.Native
	e.pruneAgentObjects(&saved)
	return nil
}

/**
 * @brief Remove a durable agent document before publishing deletion in memory.
 * @param id Internal validated random agent identifier.
 * @return Filesystem error other than an already absent file.
 */
func (e *Engine) deleteSavedAgentLocked(id string) error {
	if e.stateRoot == "" {
		return nil
	}
	if err := e.requireStateWriter(); err != nil {
		return err
	}
	if err := e.requireMetadataQuotaLocked(); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(e.stateRoot, "agent-"+id+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete cursor durable agent: %w", err)
	}
	if err := syncStateDirectory(e.stateRoot); err != nil {
		return e.metadataMutationErrorLocked(err)
	}
	e.metadataQuota.remove(id)
	return os.RemoveAll(e.agentObjectDirectory(id))
}

/**
 * @brief Persist a detached native snapshot before acknowledging KV writes or completion.
 * @param record Current execution options and owning agent identity.
 * @param checkpoint Conversation structure owned by the frame reader.
 * @param controls Blob and attachment state owned by that reader.
 * @return Save failure, or failure if the owning agent was deleted.
 */
func (e *Engine) persistNativeState(record *agentRecord, checkpoint []byte, controls *agentwire.ControlState) error {
	if e.stateRoot == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.agents[record.id] != record {
		return errors.New("agent was deleted while saving its native state")
	}
	refs, err := e.storeNativeObjects(record.id, checkpoint, controls)
	if err != nil {
		return err
	}
	previousRefs, previousCheckpoint, previousControls, previousEvicted := record.native, record.checkpoint, record.controls, record.nativeEvicted
	record.native, record.checkpoint, record.controls, record.nativeEvicted = refs, nil, nil, true
	if err := e.saveAgentLocked(record); err != nil {
		record.native, record.checkpoint, record.controls, record.nativeEvicted = previousRefs, previousCheckpoint, previousControls, previousEvicted
		return err
	}
	return nil
}
