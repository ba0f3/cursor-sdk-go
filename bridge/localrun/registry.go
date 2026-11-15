package localrun

import (
	"encoding/base64"
	"errors"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Return public agent metadata without exposing credentials or shared protobuf pointers.
 * @param id Agent identifier.
 * @return Detached agent metadata, or nil for an unknown agent.
 * @return NotFound when the agent is unknown or deleted.
 */
func (e *Engine) AgentInfo(id string) (*sdkv1.SdkAgentInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	record := e.agents[id]
	if record == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	return proto.Clone(record.info).(*sdkv1.SdkAgentInfo), nil
}

/**
 * @brief Change archive visibility without deleting history or canceling execution.
 * @param id Agent identifier.
 * @param archived Desired archive state; repeated calls are idempotent.
 * @return NotFound for an unknown agent; nil after applying the state.
 */
func (e *Engine) ArchiveAgent(id string, archived bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor runtime is closed"))
	}
	record := e.agents[id]
	if record == nil {
		return connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	if record.info.Archived != archived {
		previous := proto.Clone(record.info).(*sdkv1.SdkAgentInfo)
		record.info.Archived = archived
		record.info.LastModified = timestamppb.Now()
		if err := e.saveAgentLocked(record); err != nil {
			record.info = previous
			return err
		}
	}
	return nil
}

/**
 * @brief List local agent metadata with deterministic, exclusive pagination and workspace filtering.
 * @param options Optional page size, workspace, archive visibility and continuation token.
 * @return Detached metadata page.
 * @return Next exclusive cursor, or empty at the end.
 * @return InvalidArgument for an invalid cursor, limit or unsupported runtime filter.
 */
func (e *Engine) ListAgents(options *sdkv1.ListAgentsOptions) ([]*sdkv1.SdkAgentInfo, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if options.GetRuntime() == sdkv1.Runtime_RUNTIME_CLOUD || options.GetPrUrl() != "" {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, errors.New("cloud agent filters are unavailable for local agents"))
	}
	var items []*sdkv1.SdkAgentInfo
	for _, record := range e.agents {
		if record.info.GetArchived() && !options.GetIncludeArchived() {
			continue
		}
		if options.GetCwd() != "" && options.GetCwd() != record.workdir {
			continue
		}
		items = append(items, proto.Clone(record.info).(*sdkv1.SdkAgentInfo))
	}
	sort.Slice(items, func(i, j int) bool {
		if delta := items[i].GetCreatedAt().AsTime().Compare(items[j].GetCreatedAt().AsTime()); delta != 0 {
			return delta > 0
		}
		return items[i].GetAgentId() > items[j].GetAgentId()
	})
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.GetAgentId()
	}
	start, end, next, err := registryPage(ids, options.GetLimit(), options.GetCursor(), "agents")
	if err != nil {
		return nil, "", err
	}
	return items[start:end], next, nil
}

/**
 * @brief List retained run snapshots for one local agent.
 * @param id Owning agent identifier.
 * @param options Optional pagination, workspace and runtime filters.
 * @return Detached run snapshots ordered newest first.
 * @return Exclusive next cursor, or empty at the end.
 * @return NotFound for an unknown agent or InvalidArgument for invalid filters/cursors.
 */
func (e *Engine) ListRuns(id string, options *sdkv1.ListRunsOptions) ([]*sdkv1.RunSnapshot, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	record := e.agents[id]
	if record == nil {
		return nil, "", connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	if options.GetRuntime() == sdkv1.Runtime_RUNTIME_CLOUD || (options.GetCwd() != "" && options.GetCwd() != record.workdir) {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, errors.New("run filters do not match the local agent"))
	}
	var items []*sdkv1.RunSnapshot
	for _, snapshot := range e.snapshots {
		if snapshot.GetAgentId() == id {
			items = append(items, proto.Clone(snapshot).(*sdkv1.RunSnapshot))
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if delta := items[i].GetCreatedAt().AsTime().Compare(items[j].GetCreatedAt().AsTime()); delta != 0 {
			return delta > 0
		}
		return items[i].GetRunId() > items[j].GetRunId()
	})
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.GetRunId()
	}
	start, end, next, err := registryPage(ids, options.GetLimit(), options.GetCursor(), "runs:"+id)
	if err != nil {
		return nil, "", err
	}
	return items[start:end], next, nil
}

/**
 * @brief Validate a scoped continuation token and select an exclusive page from a stable ordering.
 * @param ids Ordered record identifiers matching the requested filters.
 * @param limit Requested size; zero defaults to 100 and values over 1000 are rejected.
 * @param cursor Opaque exclusive cursor, or empty for the first page.
 * @param scope Cursor owner, preventing reuse across agents or collection types.
 * @return Inclusive start index.
 * @return Exclusive end index.
 * @return Continuation token, or empty when exhausted.
 * @return InvalidArgument for malformed, out-of-scope or stale cursors and invalid limits.
 */
func registryPage(ids []string, limit uint32, cursor, scope string) (int, int, string, error) {
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid registry page limit or cursor"))
	if limit > 1000 {
		return 0, 0, "", invalid
	}
	if limit == 0 {
		limit = 100
	}
	start := 0
	if cursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return 0, 0, "", invalid
		}
		prefix := scope + "\x00"
		if !strings.HasPrefix(string(data), prefix) {
			return 0, 0, "", invalid
		}
		last := strings.TrimPrefix(string(data), prefix)
		found := false
		for i, id := range ids {
			if id == last {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return 0, 0, "", invalid
		}
	}
	end := min(start+int(limit), len(ids))
	next := ""
	if end < len(ids) {
		next = base64.RawURLEncoding.EncodeToString([]byte(scope + "\x00" + ids[end-1]))
	}
	return start, end, next, nil
}
