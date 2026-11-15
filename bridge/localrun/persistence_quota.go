package localrun

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Existing durable metadata limits and the shared interrupted-run recovery diagnostic.
 */
const (
	maxDurableMetadataAgents        = 4096                                        ///< Maximum committed agent documents.
	maxDurableMetadataDocumentBytes = 96 << 20                                    ///< Maximum actual bytes in any committed agent document.
	maxDurableMetadataTotalBytes    = 512 << 20                                   ///< Maximum actual bytes across committed agent documents.
	interruptedRunCode              = "unavailable"                               ///< SDK failure code used after process interruption.
	interruptedRunMessage           = "bridge restarted before the run completed" ///< Stable recovery explanation included in reserved bytes.
)

/**
 * @brief Actual committed document size and its separate prepaid restart growth.
 */
type durableMetadataSize struct {
	bytes   int64 ///< Actual JSON document bytes.
	reserve int64 ///< Maximum additional interrupted-run metadata bytes.
}

/**
 * @brief Bounded metadata index owned by the engine mutex and its exclusive writer lease.
 */
type durableMetadataQuota struct {
	documents     map[string]durableMetadataSize ///< Committed sizes, indexed before any restore may rewrite a document.
	bytes         int64                          ///< Actual committed metadata total, excluding recovery reservations.
	reserved      int64                          ///< Prepaid interrupted-run recovery growth.
	totalLimit    int64                          ///< Aggregate hard byte ceiling; private instance overrides support small boundary fixtures.
	documentLimit int64                          ///< Per-document hard byte ceiling.
	agentLimit    int                            ///< Maximum committed agent count.
	recovering    bool                           ///< Startup may consume reserved headroom, while retaining all actual hard limits.
	uncertain     bool                           ///< A failed filesystem durability barrier must be reconciled before further mutations.
}

/**
 * @brief Create an empty metadata index with the repository's existing hard limits.
 * @return Private quota owned by one engine.
 */
func newDurableMetadataQuota() *durableMetadataQuota {
	return &durableMetadataQuota{documents: make(map[string]durableMetadataSize), totalLimit: maxDurableMetadataTotalBytes, documentLimit: maxDurableMetadataDocumentBytes, agentLimit: maxDurableMetadataAgents}
}

/**
 * @brief Check a prospective replacement without changing accounting or the previous file.
 * @param id Stable agent document identity.
 * @param size Serialized new document bytes.
 * @param reserve Growth prepaid for interrupted-run recovery.
 * @return Count, document or aggregate capacity error; otherwise nil.
 */
func (q *durableMetadataQuota) check(id string, size, reserve int64) error {
	old, exists := q.documents[id]
	if size < 0 || reserve < 0 || size > q.documentLimit {
		return errors.New("cursor durable agent exceeds size limit")
	}
	if !exists && len(q.documents) >= q.agentLimit {
		return errors.New("cursor state root exceeds agent limit")
	}
	actual := q.bytes - old.bytes + size
	if actual > q.totalLimit {
		return errors.New("cursor durable metadata aggregate size limit reached")
	}
	if q.recovering {
		return nil
	}
	// Existing legacy state may lack prepaid recovery room. Shrinking it remains
	// possible, while a new write may never increase an already excessive budget.
	if size+reserve > q.documentLimit && size+reserve > old.bytes+old.reserve {
		return errors.New("cursor durable agent lacks interrupted-run recovery headroom")
	}
	effective := actual + q.reserved - old.reserve + reserve
	if effective > q.totalLimit && effective > q.bytes+q.reserved {
		return errors.New("cursor durable metadata lacks interrupted-run recovery headroom")
	}
	return nil
}

/**
 * @brief Publish accounting only after a document replacement has crossed its durability barrier.
 * @param id Committed document identity.
 * @param size Actual committed JSON bytes.
 * @param reserve Prepaid recovery growth associated with those exact bytes.
 */
func (q *durableMetadataQuota) commit(id string, size, reserve int64) {
	old := q.documents[id]
	q.bytes += size - old.bytes
	q.reserved += reserve - old.reserve
	q.documents[id] = durableMetadataSize{bytes: size, reserve: reserve}
}

/**
 * @brief Release capacity only after the document deletion has become durable.
 * @param id Deleted agent document identity; absent identities are harmless.
 */
func (q *durableMetadataQuota) remove(id string) {
	old := q.documents[id]
	q.bytes -= old.bytes
	q.reserved -= old.reserve
	delete(q.documents, id)
}

/**
 * @brief Compute recovery growth using only the active snapshot's already encoded protobuf and fixed-size refs.
 * @param snapshot Running SDK snapshot, without mutating its current execution state.
 * @param encoded Existing serialized snapshot already prepared for the saved document.
 * @param eventRefs Current saved per-run event references.
 * @return Nonnegative JSON growth sufficient for the restart error and terminal pair.
 * @return Protobuf encoding error.
 */
func metadataRunRecoveryReserve(snapshot *sdkv1.RunSnapshot, encoded []byte, eventRefs map[string][]string) (int64, error) {
	if snapshot.GetStatus() != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING {
		return 0, nil
	}
	recovered := proto.Clone(snapshot).(*sdkv1.RunSnapshot)
	recovered.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_ERROR
	code, message := interruptedRunCode, interruptedRunMessage
	recovered.ErrorCode, recovered.ErrorMessage = &code, &message
	data, err := proto.Marshal(recovered)
	if err != nil {
		return 0, err
	}
	growth := base64.StdEncoding.EncodedLen(len(data)) - base64.StdEncoding.EncodedLen(len(encoded))
	refs, exists := eventRefs[snapshot.GetRunId()]
	if exists {
		before := 67*len(refs) + 1
		if len(refs) == 0 {
			before = 2
			if refs == nil {
				before = 4
			}
		}
		growth += 67*(len(refs)+2) + 1 - before
	} else {
		key, _ := json.Marshal(snapshot.GetRunId())
		growth += len(key) + 1 + 135
		if len(eventRefs) == 0 {
			growth += len(`,"event_refs":{}`)
		} else {
			growth++
		}
	}
	if growth < 0 {
		return 0, nil
	}
	return int64(growth), nil
}

/**
 * @brief Reject nonregular metadata paths and decode one bounded document without retaining unrelated payloads.
 * @param path Private agent JSON path.
 * @return Parsed document, its actual byte size, and any file/type/JSON error.
 */
func readDurableMetadataDocument(path string) (*savedAgent, int64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxDurableMetadataDocumentBytes {
		return nil, 0, errors.New("cursor durable agent exceeds size limit or is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxDurableMetadataDocumentBytes+1))
	if err != nil || len(data) > maxDurableMetadataDocumentBytes {
		return nil, 0, errors.New("cursor durable state exceeds size limit or cannot be read")
	}
	var saved savedAgent
	if err := json.Unmarshal(data, &saved); err != nil {
		return nil, 0, errors.New("invalid cursor durable agent document")
	}
	return &saved, int64(len(data)), nil
}

/**
 * @brief Enumerate a bounded metadata index while propagating directory read failures.
 * @param root Private state directory owned by the writer lease.
 * @param limit Maximum matching metadata entries to retain.
 * @return Sorted agent document paths, or nil on directory/count failure.
 * @return Directory read or agent count error.
 */
func durableMetadataPaths(root string, limit int) ([]string, error) {
	directory, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.Close() }()
	var paths []string
	for {
		entries, readErr := directory.ReadDir(256)
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "agent-") || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			if len(paths) >= limit {
				return nil, errors.New("cursor state root exceeds agent limit")
			}
			paths = append(paths, filepath.Join(root, entry.Name()))
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	sort.Strings(paths)
	return paths, nil
}

/**
 * @brief Index all existing metadata before any restoration or migration may publish replacements.
 * @param paths Complete private agent document set.
 * @param limits Existing quota whose instance limits should be preserved, or nil for production defaults.
 * @return Complete actual-size and recovery-reservation index.
 * @return Bounded document, protobuf, count or aggregate limit error.
 */
func loadDurableMetadataQuota(paths []string, limits *durableMetadataQuota) (*durableMetadataQuota, error) {
	q := newDurableMetadataQuota()
	if limits != nil {
		q.totalLimit, q.documentLimit, q.agentLimit = limits.totalLimit, limits.documentLimit, limits.agentLimit
	}
	q.recovering = true
	if len(paths) > q.agentLimit {
		return nil, errors.New("cursor state root exceeds agent limit")
	}
	for _, path := range paths {
		saved, size, err := readDurableMetadataDocument(path)
		if err != nil {
			return nil, err
		}
		var reserve int64
		for _, data := range saved.Runs {
			snapshot := &sdkv1.RunSnapshot{}
			if err := proto.Unmarshal(data, snapshot); err != nil {
				return nil, errors.New("invalid cursor durable run snapshot")
			}
			growth, err := metadataRunRecoveryReserve(snapshot, data, saved.EventRefs)
			if err != nil {
				return nil, err
			}
			reserve += growth
		}
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "agent-"), ".json")
		if err := q.check(id, size, reserve); err != nil {
			return nil, err
		}
		q.commit(id, size, reserve)
	}
	return q, nil
}

/**
 * @brief Reconcile ambiguous rename/unlink outcomes only after the directory is known durable again.
 * @return Filesystem or bounded index error; accounting remains blocked on failure.
 * @note Caller owns the engine mutex or is performing unpublished startup restoration.
 */
func (e *Engine) reconcileMetadataQuotaLocked() error {
	if err := syncStateDirectory(e.stateRoot); err != nil {
		return err
	}
	paths, err := durableMetadataPaths(e.stateRoot, e.metadataQuota.agentLimit)
	if err != nil {
		return err
	}
	next, err := loadDurableMetadataQuota(paths, e.metadataQuota)
	if err != nil {
		return err
	}
	next.recovering = e.metadataQuota.recovering
	e.metadataQuota = next
	return nil
}

/**
 * @brief Require trustworthy committed accounting before any further metadata mutation.
 * @return Missing index or reconciliation failure; otherwise nil.
 */
func (e *Engine) requireMetadataQuotaLocked() error {
	if e.metadataQuota == nil {
		return errors.New("cursor durable metadata accounting is unavailable")
	}
	if e.metadataQuota.uncertain {
		return e.reconcileMetadataQuotaLocked()
	}
	return nil
}

/**
 * @brief Preserve the original mutation failure while reconciling a possibly published rename or unlink.
 * @param mutationErr Failed metadata publication or deletion barrier.
 * @return Original error plus any failure that keeps future metadata writes blocked.
 */
func (e *Engine) metadataMutationErrorLocked(mutationErr error) error {
	e.metadataQuota.uncertain = true
	if err := e.reconcileMetadataQuotaLocked(); err != nil {
		return errors.Join(mutationErr, fmt.Errorf("reconcile cursor durable metadata: %w", err))
	}
	return mutationErr
}
