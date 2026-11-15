package localrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Hard per-agent receipt and per-call JSON limits; no receipt is evicted to permit another action.
 */
const (
	maxToolReceipts          = 1024     ///< Retained native callback identities per agent.
	maxToolReceiptBytes      = 8 << 20  ///< Maximum serialized pending or completed receipt.
	maxToolReceiptTotalBytes = 64 << 20 ///< Maximum bytes retained by current receipt references per agent.
	maxToolArgumentBytes     = 1 << 20  ///< Maximum canonical argument document before any callback.
	maxToolCallIDBytes       = 1024     ///< Maximum native identity and tool name length.
)

/**
 * @brief Immutable callback claim or result, independent of exec transport correlation and runtime credentials.
 */
type toolReceipt struct {
	Version   int             `json:"version"`          ///< Supported receipt schema version.
	CallID    string          `json:"call_id"`          ///< Native stable tool call identity.
	Name      string          `json:"name"`             ///< Exact declared tool name.
	Signature string          `json:"signature"`        ///< SHA-256 of canonical tool name and arguments.
	State     string          `json:"state"`            ///< Pending means that execution outcome must be treated as unknown.
	Result    json.RawMessage `json:"result,omitempty"` ///< Detached completed result JSON object.
	Failed    bool            `json:"failed"`           ///< Actual explicit tool error semantics returned by the callback adapter.
}

/**
 * @brief Validate one immutable receipt before restoring or replaying its value.
 * @param data Owned serialized receipt bytes.
 * @param callID Native identity from the owning agent's receipt reference map.
 * @return Validated receipt; nil on failure.
 * @return Unsupported schema, identity, signature, state or result error.
 */
func decodeToolReceipt(data []byte, callID string) (*toolReceipt, error) {
	if len(data) > maxToolReceiptBytes {
		return nil, errors.New("custom tool receipt exceeds size limit")
	}
	var receipt toolReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return nil, errors.New("invalid custom tool receipt JSON")
	}
	if receipt.Version != 1 || receipt.CallID != callID || strings.TrimSpace(callID) == "" || len(callID) > maxToolCallIDBytes || receipt.Name == "" || len(receipt.Name) > maxToolCallIDBytes || !validObjectReference(receipt.Signature) {
		return nil, errors.New("invalid custom tool receipt identity or schema")
	}
	switch receipt.State {
	case "pending":
		if len(receipt.Result) != 0 || receipt.Failed {
			return nil, errors.New("pending custom tool receipt contains a result")
		}
	case "completed":
		var result map[string]any
		if err := json.Unmarshal(receipt.Result, &result); err != nil || result == nil {
			return nil, errors.New("invalid completed custom tool receipt result")
		}
	default:
		return nil, errors.New("invalid custom tool receipt state")
	}
	return &receipt, nil
}

/**
 * @brief Keep current immutable receipt bytes within a per-agent budget while replacing one native identity.
 * @param rec Registered agent protected by the engine mutex.
 * @param callID Native identity being inserted or replaced.
 * @param size Prospective serialized receipt size.
 * @return Object access or explicit count/size limit error.
 * @note Caller holds the engine mutex; no callbacks or network operations occur here.
 */
func (e *Engine) checkToolReceiptBudgetLocked(rec *agentRecord, callID string, size int) error {
	if size > maxToolReceiptBytes {
		return errors.New("custom tool receipt exceeds size limit")
	}
	if _, exists := rec.toolReceipts[callID]; !exists && len(rec.toolReceipts) >= maxToolReceipts {
		return errors.New("custom tool receipt count limit reached; use a new agent")
	}
	total := int64(size)
	for id, ref := range rec.toolReceipts {
		if id == callID {
			continue
		}
		var objectSize int64
		if e.stateRoot == "" {
			data, ok := rec.toolReceiptObjects[ref]
			if !ok {
				return errors.New("custom tool receipt object is missing")
			}
			objectSize = int64(len(data))
		} else {
			if !validObjectReference(ref) {
				return errors.New("invalid custom tool receipt reference")
			}
			info, err := os.Lstat(filepath.Join(e.agentObjectDirectory(rec.id), ref))
			if err != nil {
				return fmt.Errorf("inspect custom tool receipt: %w", err)
			}
			if !info.Mode().IsRegular() {
				return errors.New("custom tool receipt is not a regular file")
			}
			objectSize = info.Size()
		}
		if objectSize > maxToolReceiptBytes {
			return errors.New("custom tool receipt exceeds size limit")
		}
		total += objectSize
		if total > maxToolReceiptTotalBytes {
			return errors.New("custom tool receipt storage limit reached; use a new agent")
		}
	}
	return nil
}

/**
 * @brief Store a detached immutable receipt in the durable object store or agent-private memory.
 * @param rec Registered agent protected by the engine mutex.
 * @param data Serialized immutable receipt bytes.
 * @return SHA-256 object identity.
 * @return Durable object write error.
 */
func (e *Engine) putToolReceiptLocked(rec *agentRecord, data []byte) (string, error) {
	if e.stateRoot != "" {
		return e.putAgentObject(rec.id, data)
	}
	sum := sha256.Sum256(data)
	ref := hex.EncodeToString(sum[:])
	if rec.toolReceiptObjects == nil {
		rec.toolReceiptObjects = make(map[string][]byte)
	}
	rec.toolReceiptObjects[ref] = append([]byte(nil), data...)
	return ref, nil
}

/**
 * @brief Load a receipt without exposing mutable storage to callback callers.
 * @param rec Registered agent protected by the engine mutex.
 * @param ref Immutable receipt object identity.
 * @return Owned serialized bytes.
 * @return Missing or invalid receipt object error.
 */
func (e *Engine) getToolReceiptLocked(rec *agentRecord, ref string) ([]byte, error) {
	if e.stateRoot != "" {
		return e.getAgentObject(rec.id, ref)
	}
	data, ok := rec.toolReceiptObjects[ref]
	if !ok {
		return nil, errors.New("custom tool receipt object is missing")
	}
	return append([]byte(nil), data...), nil
}

/**
 * @brief Claim a stable native callback once and replay only results committed before acknowledgement.
 * @param ctx Callback lifetime; cancellation after claim leaves its outcome unknown.
 * @param agentID Owning registered SDK agent identifier.
 * @param rec Current registered agent with immutable tool declarations.
 * @param call Native tool invocation; a nonempty stable CallID is mandatory.
 * @return Detached callback result, or nil when execution cannot be safely acknowledged.
 * @return Explicit callback failure semantics; meaningful only with a delivered result.
 * @return Identity conflict, unknown prior outcome, callback or persistence error.
 * @note Callback execution never holds the engine mutex. Memory-only engines retain claims only for their lifetime.
 */
func (e *Engine) invokeDurableTool(ctx context.Context, agentID string, rec *agentRecord, call *agentwire.ToolCall) (map[string]any, bool, error) {
	if call == nil || rec == nil || strings.TrimSpace(call.CallID) == "" || len(call.CallID) > maxToolCallIDBytes || call.Name == "" || len(call.Name) > maxToolCallIDBytes {
		return nil, false, errors.New("custom tool requires a bounded nonempty native call ID and name")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, context.Cause(ctx)
	}
	owned := *call
	owned.Args = &structpb.Struct{}
	if call.Args != nil {
		owned.Args = proto.Clone(call.Args).(*structpb.Struct)
	}
	args, err := json.Marshal(owned.Args.AsMap())
	if err != nil {
		return nil, false, errors.New("custom tool arguments cannot be represented as JSON")
	}
	if len(args) > maxToolArgumentBytes {
		return nil, false, errors.New("custom tool arguments exceed size limit")
	}
	identity, err := json.Marshal(struct {
		Name string          `json:"name"` ///< Exact tool declaration identity.
		Args json.RawMessage `json:"args"` ///< Canonical detached JSON arguments.
	}{owned.Name, args})
	if err != nil {
		return nil, false, err
	}
	sum := sha256.Sum256(identity)
	receipt := &toolReceipt{Version: 1, CallID: owned.CallID, Name: owned.Name, Signature: hex.EncodeToString(sum[:]), State: "pending"}
	claim, err := json.Marshal(receipt)
	if err != nil {
		return nil, false, err
	}

	e.mu.Lock()
	if e.closed || rec.id != agentID || e.agents[agentID] != rec {
		e.mu.Unlock()
		return nil, false, errors.New("custom tool agent is no longer available")
	}
	if ref, exists := rec.toolReceipts[owned.CallID]; exists {
		data, readErr := e.getToolReceiptLocked(rec, ref)
		e.mu.Unlock()
		if readErr != nil {
			return nil, false, readErr
		}
		previous, err := decodeToolReceipt(data, owned.CallID)
		if err != nil {
			return nil, false, err
		}
		if previous.Signature != receipt.Signature || previous.Name != owned.Name {
			return nil, false, errors.New("custom tool call ID was reused with a different name or arguments")
		}
		if previous.State != "completed" {
			return nil, false, errors.New("custom tool callback outcome is unknown; automatic replay is refused")
		}
		var result map[string]any
		if err := json.Unmarshal(previous.Result, &result); err != nil {
			return nil, false, err
		}
		return result, previous.Failed, nil
	}
	if err := e.checkToolReceiptBudgetLocked(rec, owned.CallID, len(claim)); err != nil {
		e.mu.Unlock()
		return nil, false, err
	}
	claimRef, err := e.putToolReceiptLocked(rec, claim)
	if err != nil {
		e.mu.Unlock()
		return nil, false, err
	}
	if rec.toolReceipts == nil {
		rec.toolReceipts = make(map[string]string)
	}
	rec.toolReceipts[owned.CallID] = claimRef
	// Keep the claim even if the document durability barrier fails after rename.
	// No callback starts until the claim has been published successfully.
	err = e.saveAgentLocked(rec)
	e.mu.Unlock()
	if err != nil {
		return nil, false, fmt.Errorf("persist custom tool claim: %w", err)
	}

	result, failed, err := e.invokeTool(ctx, agentID, rec, &owned)
	if err != nil {
		return nil, false, err
	}
	if result == nil {
		return nil, false, errors.New("custom tool callback returned no result; outcome is unknown")
	}
	receipt.Result, err = json.Marshal(result)
	if err != nil {
		return nil, false, errors.New("custom tool result cannot be represented as JSON; outcome is unknown")
	}
	receipt.State, receipt.Failed = "completed", failed
	completed, err := json.Marshal(receipt)
	if err != nil {
		return nil, false, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.agents[agentID] != rec || rec.toolReceipts[owned.CallID] != claimRef {
		return nil, false, errors.New("custom tool claim is no longer available; outcome is unknown")
	}
	if err := e.checkToolReceiptBudgetLocked(rec, owned.CallID, len(completed)); err != nil {
		return nil, false, err
	}
	completedRef, err := e.putToolReceiptLocked(rec, completed)
	if err != nil {
		return nil, false, err
	}
	rec.toolReceipts[owned.CallID] = completedRef
	if err := e.saveAgentLocked(rec); err != nil {
		// A failed barrier may have published the new document. Refuse replay in
		// this process until restart determines which durable document survived.
		rec.toolReceipts[owned.CallID] = claimRef
		if e.stateRoot == "" {
			delete(rec.toolReceiptObjects, completedRef)
		}
		return nil, false, fmt.Errorf("persist custom tool result: %w", err)
	}
	if e.stateRoot == "" {
		delete(rec.toolReceiptObjects, claimRef)
	}
	var detached map[string]any
	if err := json.Unmarshal(receipt.Result, &detached); err != nil {
		return nil, false, err
	}
	return detached, failed, nil
}
