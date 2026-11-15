package localrun

import (
	"context"
	"errors"
	"strconv"
	"strings"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

/**
 * @brief Bounded journal capacity including SDK, raw-delta and step copies of a supported tool image.
 */
const maxRunHistoryBytes = 96 << 20

/**
 * @brief A bounded run journal with broadcast wakeups instead of per-observer queues.
 */
type runHistory struct {
	events     []*sdkv1.RunStreamMessage ///< Immutable protobuf snapshots in offset order.
	bytes      int                       ///< Serialized retained event bytes.
	changed    chan struct{}             ///< Closed and replaced under the registry mutex on updates.
	finished   bool                      ///< Terminal events and the final snapshot have been committed in memory.
	objectRefs []string                  ///< Immutable serialized event identities already written to the durable object store.
	evicted    bool                      ///< Completed event payloads are external and must be loaded before replay.
}

/**
 * @brief Allocate an isolated journal before publishing a running snapshot.
 * @return Empty live journal.
 */
func newRunHistory() *runHistory { return &runHistory{changed: make(chan struct{})} }

/**
 * @brief Wake all readers after a journal mutation without blocking its writer.
 * @note Caller holds the registry mutex.
 */
func (h *runHistory) notify() { close(h.changed); h.changed = make(chan struct{}) }

/**
 * @brief Append an immutable event with its exclusive replay offset.
 * @param message Owned event to clone; existing offsets are replaced.
 * @param terminal Whether reserved terminal space may be consumed.
 * @return Retained immutable event, or nil on size/count exhaustion.
 * @return ResourceExhausted when history exceeds its bounds.
 * @note Caller holds the registry mutex. Does not notify observers until commit.
 */
func (h *runHistory) append(message *sdkv1.RunStreamMessage, terminal bool) (*sdkv1.RunStreamMessage, error) {
	if h.evicted {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor run history must be loaded before appending"))
	}
	copy := proto.Clone(message).(*sdkv1.RunStreamMessage)
	offset := strconv.Itoa(len(h.events) + 1)
	copy.Offset = &offset
	limit, count := maxRunHistoryBytes, 65536
	if !terminal {
		limit -= 8192
		count -= 2
	}
	size := proto.Size(copy)
	if len(h.events) >= count || h.bytes+size > limit {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("cursor run history limit exceeded"))
	}
	h.events = append(h.events, copy)
	h.bytes += size
	return copy, nil
}

/**
 * @brief Retain an SDK event before delivery so observers can replay it independently.
 * @param runID Registered run identity.
 * @param message SDK stream event.
 * @return Detached retained event carrying its offset, or nil on failure.
 * @return NotFound, FailedPrecondition or journal size failure.
 */
func (e *Engine) recordRunEvent(runID string, message *sdkv1.RunStreamMessage) (*sdkv1.RunStreamMessage, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h := e.histories[runID]
	if h == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run was not found"))
	}
	if h.finished {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("run history is already complete"))
	}
	retained, err := h.append(message, false)
	if err != nil {
		return nil, err
	}
	if snapshot := e.snapshots[runID]; snapshot != nil {
		if record := e.agents[snapshot.AgentId]; record != nil {
			if err := e.saveAgentLocked(record); err != nil {
				h.events = h.events[:len(h.events)-1]
				h.bytes -= proto.Size(retained)
				if len(h.objectRefs) > len(h.events) {
					h.objectRefs = h.objectRefs[:len(h.events)]
				}
				return nil, err
			}
		}
	}
	h.notify()
	return proto.Clone(retained).(*sdkv1.RunStreamMessage), nil
}

/**
 * @brief Convert a detached lifecycle snapshot into the identical SDK result contract.
 * @param snapshot Retained run snapshot.
 * @return Detached result with usage, model, status, timing and error fields preserved.
 */
func resultFromSnapshot(snapshot *sdkv1.RunSnapshot) *sdkv1.RunResult {
	s := proto.Clone(snapshot).(*sdkv1.RunSnapshot)
	return &sdkv1.RunResult{RunId: s.RunId, AgentId: s.AgentId, Status: s.Status, Result: s.Result, Model: s.Model, DurationMs: s.DurationMs, Git: s.Git, CreatedAt: s.CreatedAt, Usage: s.Usage, ErrorCode: s.ErrorCode, ErrorMessage: s.ErrorMessage}
}

/**
 * @brief Commit one terminal result and done pair before waking observers or sending final bytes.
 * @param runID Run with a finalized snapshot and current native checkpoint.
 * @return Detached terminal events in offset order.
 * @return Journal or durable storage failure; failures are represented by an error result in memory.
 * @note Caller holds the registry mutex; repeated calls do not duplicate terminal events.
 */
func (e *Engine) sealRunHistoryLocked(runID string) ([]*sdkv1.RunStreamMessage, error) {
	snapshot := e.snapshots[runID]
	if snapshot == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run was not found"))
	}
	h := e.histories[runID]
	if h == nil {
		h = newRunHistory()
		e.histories[runID] = h
	}
	if h.finished {
		return nil, nil
	}
	if err := e.hydrateHistoryLocked(runID); err != nil {
		return nil, err
	}
	start, size := len(h.events), h.bytes
	appendTerminal := func() error {
		result := resultFromSnapshot(snapshot)
		if snapshot.Status != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED {
			result.Result = ""
		}
		if _, err := h.append(&sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_Result{Result: &sdkv1.RunStreamResult{AgentId: snapshot.AgentId, RunId: runID, Status: snapshot.Status, ErrorCode: snapshot.ErrorCode, Result: result}}}, true); err != nil {
			return err
		}
		_, err := h.append(&sdkv1.RunStreamMessage{Envelope: &sdkv1.RunStreamMessage_Done{Done: &sdkv1.RunStreamDone{AgentId: snapshot.AgentId, RunId: runID}}}, true)
		return err
	}
	err := appendTerminal()
	if err == nil {
		if record := e.agents[snapshot.AgentId]; record != nil {
			err = e.saveAgentLocked(record)
		}
	}
	if err != nil {
		h.events = h.events[:start]
		if len(h.objectRefs) > start {
			h.objectRefs = h.objectRefs[:start]
		}
		h.bytes = size
		snapshot.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_ERROR
		code, message := connect.CodeOf(err).String(), "failed to commit cursor run history"
		snapshot.ErrorCode = &code
		snapshot.ErrorMessage = &message
		if record := e.agents[snapshot.AgentId]; record != nil && record.info != nil {
			record.info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_ERROR
		}
		if terminalErr := appendTerminal(); terminalErr != nil {
			return nil, errors.Join(err, terminalErr)
		}
	}
	h.finished = true
	h.notify()
	terminal := make([]*sdkv1.RunStreamMessage, len(h.events)-start)
	for i, event := range h.events[start:] {
		terminal[i] = proto.Clone(event).(*sdkv1.RunStreamMessage)
	}
	return terminal, err
}

/**
 * @brief Wait for a known run without canceling generation when the observer disconnects.
 * @param ctx Observer lifetime only.
 * @param id Registered or retained run identifier.
 * @return Detached final result, or nil on observer cancellation or deletion.
 * @return Context error or NotFound for unknown/deleted runs.
 */
func (e *Engine) WaitRun(ctx context.Context, id string) (*sdkv1.RunResult, error) {
	for {
		e.mu.Lock()
		snapshot := e.snapshots[id]
		if snapshot == nil {
			e.mu.Unlock()
			return nil, connect.NewError(connect.CodeNotFound, errors.New("run was not found"))
		}
		h := e.histories[id]
		if (h != nil && h.finished) || (h == nil && snapshot.Status != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING) {
			result := resultFromSnapshot(snapshot)
			e.mu.Unlock()
			return result, nil
		}
		if h == nil {
			h = newRunHistory()
			e.histories[id] = h
		}
		changed := h.changed
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

/**
 * @brief Match the official SDK's positive decimal-prefix offset parsing and zero fallback.
 * @param offset Exclusive stream offset string, or empty for the beginning.
 * @return Parsed positive offset, or zero when absent or invalid.
 */
func parseRunOffset(offset string) uint64 {
	offset = strings.TrimSpace(offset)
	offset = strings.TrimPrefix(offset, "+")
	i := 0
	for i < len(offset) && offset[i] >= '0' && offset[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0
	}
	parsed, err := strconv.ParseUint(offset[:i], 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

/**
 * @brief Replay retained events and follow a live run without holding locks during observer callbacks.
 * @param ctx Observer cancellation; does not abort the run.
 * @param id Run identity.
 * @param after Exclusive offset, following official SDK parsing semantics.
 * @param emit Observer callback; its failure only terminates this observer.
 * @return Observer/context error, NotFound, or nil after all terminal events.
 */
func (e *Engine) ObserveRun(ctx context.Context, id, after string, emit func(*sdkv1.RunStreamMessage) error) error {
	cursor := parseRunOffset(after)
	for {
		e.mu.Lock()
		snapshot := e.snapshots[id]
		if snapshot == nil {
			e.mu.Unlock()
			return connect.NewError(connect.CodeNotFound, errors.New("run was not found"))
		}
		h := e.histories[id]
		if h == nil {
			h = newRunHistory()
			e.histories[id] = h
			if snapshot.Status != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING {
				if _, err := e.sealRunHistoryLocked(id); err != nil {
					e.mu.Unlock()
					return err
				}
			}
		}
		if err := e.hydrateHistoryLocked(id); err != nil {
			e.mu.Unlock()
			return err
		}
		var batch []*sdkv1.RunStreamMessage
		for i := cursor; i < uint64(len(h.events)); i++ {
			batch = append(batch, proto.Clone(h.events[i]).(*sdkv1.RunStreamMessage))
		}
		finished, changed := h.finished, h.changed
		e.releaseDetachedHistoryLocked(id)
		e.mu.Unlock()
		for _, event := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := emit(event); err != nil {
				return err
			}
			cursor = parseRunOffset(event.GetOffset())
		}
		if finished {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
