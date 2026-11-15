package localrun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Quota boundaries include reserved recovery growth and release space only through explicit commits.
 * @param t Small arithmetic fixture; no large files are allocated.
 */
func TestDurableMetadataQuotaBoundaries(t *testing.T) {
	q := newDurableMetadataQuota()
	q.totalLimit, q.documentLimit, q.agentLimit = 100, 80, 2
	if err := q.check("a", 60, 20); err != nil {
		t.Fatal(err)
	}
	q.commit("a", 60, 20)
	if err := q.check("b", 20, 0); err != nil {
		t.Fatal(err)
	}
	q.commit("b", 20, 0)
	for _, update := range []struct {
		id            string
		size, reserve int64
	}{{"a", 61, 20}, {"b", 21, 0}, {"c", 1, 0}, {"a", 81, 0}} {
		if err := q.check(update.id, update.size, update.reserve); err == nil {
			t.Fatal("over-quota update accepted")
		}
	}
	if q.bytes != 80 || q.reserved != 20 || len(q.documents) != 2 {
		t.Fatal("rejected check changed committed accounting")
	}
	if err := q.check("a", 50, 20); err != nil {
		t.Fatal(err)
	}
	q.commit("a", 50, 20)
	q.remove("b")
	q.remove("b")
	if q.bytes != 50 || q.reserved != 20 || len(q.documents) != 1 {
		t.Fatal("shrink/delete accounting is not idempotent")
	}
}

/**
 * @brief Actual committed file lengths agree with the engine's metadata index.
 * @param t Test harness.
 * @param e Private persistent engine.
 */
func assertMetadataAccounting(t *testing.T, e *Engine) {
	t.Helper()
	if e.metadataQuota == nil {
		t.Fatal("persistent engine has no aggregate metadata accounting")
	}
	paths, err := filepath.Glob(filepath.Join(e.stateRoot, "agent-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "agent-"), ".json")
		if e.metadataQuota.documents[id].bytes != info.Size() {
			t.Fatal("document accounting differs from committed file")
		}
		total += info.Size()
	}
	if total != e.metadataQuota.bytes || len(paths) != len(e.metadataQuota.documents) {
		t.Fatal("aggregate accounting differs from committed metadata")
	}
}

/**
 * @brief Runtime quota rejection preserves previous files and shrink/delete operations free real capacity.
 * @param t Private small-limit persistent engine.
 */
func TestPersistentMetadataQuotaEnforcesWrites(t *testing.T) {
	e, err := NewPersistentEngine(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake", Name: strings.Repeat("n", 2048)})
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataAccounting(t, e)
	e.metadataQuota.totalLimit = e.metadataQuota.bytes
	path := filepath.Join(e.stateRoot, "agent-"+id+".json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	record := e.agents[id]
	record.info.Name += "growth"
	err = e.saveAgentLocked(record)
	e.mu.Unlock()
	if err == nil {
		t.Fatal("aggregate quota did not reject growing metadata")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("quota rejection changed previous committed document")
	}
	assertMetadataAccounting(t, e)
	if _, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"}); err == nil {
		t.Fatal("quota did not reject new agent")
	}
	e.mu.Lock()
	record.info.Name = "small"
	err = e.saveAgentLocked(record)
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatalf("shrink did not release capacity: %v", err)
	}
	assertMetadataAccounting(t, e)
	previous := e.metadataQuota.bytes
	if err := e.DeleteAgent(second); err != nil {
		t.Fatal(err)
	}
	if e.metadataQuota.bytes >= previous {
		t.Fatal("delete did not release metadata bytes")
	}
	assertMetadataAccounting(t, e)
}

/**
 * @brief A callback claim that cannot fit metadata capacity never invokes the client action.
 * @param t Private durable callback harness with a full small quota.
 */
func TestPersistentMetadataQuotaBlocksCallbackClaim(t *testing.T) {
	fixture := &receiptCallbackFixture{fn: func(context.Context, *connect.Request[sdkv1.CallCustomToolRequest]) (*connect.Response[sdkv1.CallCustomToolResponse], error) {
		t.Error("over-quota claim executed callback")
		return nil, nil
	}}
	e, rec := receiptTestEngine(t, t.TempDir(), fixture)
	assertMetadataAccounting(t, e)
	e.metadataQuota.totalLimit = e.metadataQuota.bytes
	before, err := os.ReadFile(filepath.Join(e.stateRoot, "agent-"+rec.id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.invokeDurableTool(context.Background(), rec.id, rec, receiptTestCall(t, "over-quota", nil)); err == nil {
		t.Fatal("over-quota callback claim accepted")
	}
	after, err := os.ReadFile(filepath.Join(e.stateRoot, "agent-"+rec.id+".json"))
	if err != nil || !bytes.Equal(before, after) || fixture.calls.Load() != 0 {
		t.Fatal("over-quota claim changed document or executed action")
	}
	assertMetadataAccounting(t, e)
}

/**
 * @brief All document sizes are indexed before interrupted-run recovery can publish a replacement.
 * @param t Two private agent documents, with a deliberately invalid later file.
 */
func TestPersistentMetadataQuotaPreflightsBeforeRecovery(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	var ids []string
	for range 2 {
		id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	e.snapshots["interrupted"] = &sdkv1.RunSnapshot{AgentId: ids[0], RunId: "interrupted", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["interrupted"] = newRunHistory()
	e.mu.Lock()
	err = e.saveAgentLocked(e.agents[ids[0]])
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	first := filepath.Join(root, "agent-"+ids[0]+".json")
	before, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "agent-"+ids[1]+".json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if next, err := NewPersistentEngine(nil, root); err == nil {
		next.Close()
		t.Fatal("invalid later metadata was accepted")
	}
	after, err := os.ReadFile(first)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("recovery rewrote metadata before preflighting the complete index")
	}
}

/**
 * @brief Reserved bytes exactly cover ordinary interrupted-run snapshot and terminal-ref metadata growth.
 * @param t A real committed running document and its recovered counterpart.
 */
func TestPersistentMetadataQuotaReservesRestartHeadroom(t *testing.T) {
	root := t.TempDir()
	e, err := NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	e.snapshots["running"] = &sdkv1.RunSnapshot{AgentId: id, RunId: "running", Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING}
	e.histories["running"] = newRunHistory()
	e.mu.Lock()
	err = e.saveAgentLocked(e.agents[id])
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataAccounting(t, e)
	before, reserve := e.metadataQuota.bytes, e.metadataQuota.reserved
	if reserve <= 0 {
		t.Fatal("running snapshot did not reserve restart metadata headroom")
	}
	e.metadataQuota.totalLimit = before + reserve
	if err := e.metadataQuota.check("extra", 1, 0); err == nil {
		t.Fatal("new metadata consumed reserved restart headroom")
	}
	e.Close()
	e, err = NewPersistentEngine(nil, root)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	assertMetadataAccounting(t, e)
	if e.metadataQuota.bytes > before+reserve || e.metadataQuota.reserved != 0 {
		t.Fatal("recovery exceeded prepaid metadata growth or retained its reservation")
	}
	data, err := os.ReadFile(filepath.Join(root, "agent-"+id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved savedAgent
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	var snapshot sdkv1.RunSnapshot
	if err := proto.Unmarshal(saved.Runs["running"], &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.GetErrorCode() != interruptedRunCode || snapshot.GetErrorMessage() != interruptedRunMessage || len(saved.EventRefs["running"]) != 2 {
		t.Fatal("quota accounting changed restart recovery semantics")
	}
}

/**
 * @brief A failed durability acknowledgement reconciles the actual published rename or unlink before future writes.
 * @param t Private real metadata files and a synthetic lost acknowledgement.
 */
func TestPersistentMetadataQuotaReconcilesMutationOutcome(t *testing.T) {
	for _, operation := range []string{"unpublished", "published", "deleted"} {
		t.Run(operation, func(t *testing.T) {
			e, err := NewPersistentEngine(nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			before := e.metadataQuota.bytes
			path := filepath.Join(e.stateRoot, "agent-"+id+".json")
			switch operation {
			case "published":
				saved, _, err := readDurableMetadataDocument(path)
				if err != nil {
					t.Fatal(err)
				}
				info := &sdkv1.SdkAgentInfo{}
				if err := proto.Unmarshal(saved.Info, info); err != nil {
					t.Fatal(err)
				}
				info.Name = strings.Repeat("published", 100)
				saved.Info, err = proto.Marshal(info)
				if err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(saved)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeStateFile(e.stateRoot, filepath.Base(path), data); err != nil {
					t.Fatal(err)
				}
			case "deleted":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			lostAck := errors.New("synthetic metadata directory sync failure")
			e.mu.Lock()
			err = e.metadataMutationErrorLocked(lostAck)
			e.mu.Unlock()
			if !errors.Is(err, lostAck) {
				t.Fatal("mutation failure was hidden")
			}
			assertMetadataAccounting(t, e)
			if e.metadataQuota.uncertain {
				t.Fatal("successful durability reconciliation remained blocked")
			}
			switch operation {
			case "unpublished":
				if e.metadataQuota.bytes != before {
					t.Fatal("failed unpublished write changed committed accounting")
				}
			case "published":
				if e.metadataQuota.bytes <= before {
					t.Fatal("published larger document was not accounted")
				}
			case "deleted":
				if e.metadataQuota.bytes != 0 {
					t.Fatal("committed deletion did not release capacity")
				}
			}
		})
	}
}

/**
 * @brief Unreadable or corrupt metadata keeps uncertain accounting blocked until it can be reconciled safely.
 * @param t Private document temporarily replaced with invalid JSON.
 */
func TestPersistentMetadataQuotaBlocksUncertainAccounting(t *testing.T) {
	e, err := NewPersistentEngine(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, _, err := e.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.stateRoot, "agent-"+id+".json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before := e.metadataQuota.bytes
	if err := writeStateFile(e.stateRoot, filepath.Base(path), []byte("{")); err != nil {
		t.Fatal(err)
	}
	e.metadataQuota.uncertain = true
	if err := e.ArchiveAgent(id, true); err == nil {
		t.Fatal("unreconciled metadata permitted another publication")
	}
	if !e.metadataQuota.uncertain || e.metadataQuota.bytes != before {
		t.Fatal("failed reconciliation discarded its conservative accounting")
	}
	if err := writeStateFile(e.stateRoot, filepath.Base(path), original); err != nil {
		t.Fatal(err)
	}
	if err := e.ArchiveAgent(id, true); err != nil {
		t.Fatal(err)
	}
	assertMetadataAccounting(t, e)
	if e.metadataQuota.uncertain {
		t.Fatal("repaired metadata did not unblock writes")
	}
}
