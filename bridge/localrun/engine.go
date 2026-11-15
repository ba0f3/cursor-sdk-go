package localrun

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
	"github.com/ba0f3/cursor-sdk-go/media"
)

/**
 * @brief Marks an individual exec cancellation requested by Cursor upstream.
 */
var errUpstreamExecAborted = errors.New("cursor canceled exec operation")

/**
 * @brief Local agent registry and run execution for the native sdk.v1 bridge.
 */
type Engine struct {
	API *cursorapi.Client ///< Cursor HTTP transport owner.

	mu            sync.Mutex                    ///< Protects registry, lifecycle and callback state.
	agents        map[string]*agentRecord       ///< Agent definitions retained until deletion.
	activeRuns    map[string]context.CancelFunc ///< Cancellation owner for each active run.
	runAgents     map[string]string             ///< Active run to owning agent.
	runDone       map[string]chan struct{}      ///< Physical run cleanup completion, before a forced replacement may start.
	closed        bool                          ///< Rejects creation and execution after shutdown.
	snapshots     map[string]*sdkv1.RunSnapshot ///< Lifecycle snapshots removed with their agent.
	histories     map[string]*runHistory        ///< Immutable replay journals and observer wakeups, under the registry mutex.
	stateRoot     string                        ///< Private credential-free persistent state directory, or empty for memory only.
	stateLease    *os.File                      ///< Exclusive process-lifetime state writer lease.
	metadataQuota *durableMetadataQuota         ///< Committed document sizes and reserved recovery headroom, protected by the registry mutex.
	runTasks      sync.WaitGroup                ///< Registered runs, including their callback cleanup.
	closeOnce     sync.Once                     ///< Ensures shutdown waits and releases the writer lease once.
	toolCallback  struct {
		url   string ///< Registered loopback callback endpoint.
		token string ///< Private callback bearer token.
	} ///< SDK callback identity.
}

/**
 * @brief Immutable execution options and mutex-protected public metadata for a registered agent.
 */
type agentRecord struct {
	id                 string                                 ///< SDK agent identifier.
	apiKey             string                                 ///< Cursor API key retained only for this agent lifetime.
	workdir            string                                 ///< Client workspace reference.
	model              *sdkv1.ModelSelection                  ///< Complete selected model, including opaque catalog parameters.
	mode               sdkv1.AgentModeOption                  ///< Create-time or explicit resume mode.
	modePending        bool                                   ///< Seed the configured mode once before relying on checkpoint state.
	conversationID     string                                 ///< Native conversation identifier.
	customTools        map[string]*sdkv1.CustomToolDefinition ///< Available client-owned MCP tools.
	info               *sdkv1.SdkAgentInfo                    ///< Public lifecycle metadata, protected by the engine mutex.
	checkpoint         []byte                                 ///< Native conversation structure, protected by the engine mutex.
	controls           *agentwire.ControlState                ///< Checkpoint blob and image storage, protected by the engine mutex.
	native             *nativeReferences                      ///< Latest committed references to independently stored context objects.
	nativeEvicted      bool                                   ///< Context is stored externally and must be loaded before the next turn.
	toolReceipts       map[string]string                      ///< Immutable callback receipt references keyed by native tool call identity.
	toolReceiptObjects map[string][]byte                      ///< Agent-private receipt values for memory-only engines; absent in durable engines.
	detached           bool                                   ///< Closed handle or restored agent requires Resume before Send.
	toolPolicy         agentwire.ToolPolicy                   ///< Validated native allow/exclude restrictions for this handle.
}

/**
 * @brief Create an isolated agent registry and cancellation owner.
 * @param api Cursor control and streaming HTTP client.
 * @return An empty engine; Close cancels its active runs.
 */
func NewEngine(api *cursorapi.Client) *Engine {
	return &Engine{
		API:        api,
		agents:     make(map[string]*agentRecord),
		activeRuns: make(map[string]context.CancelFunc),
		runAgents:  make(map[string]string),
		runDone:    make(map[string]chan struct{}),
		snapshots:  make(map[string]*sdkv1.RunSnapshot),
		histories:  make(map[string]*runHistory),
	}
}

/**
 * @brief Register the authenticated SDK tool callback endpoint.
 * @param url Loopback callback origin.
 * @param token Bearer token shared with the SDK runner.
 * @note Concurrent-safe; updates affect subsequent callbacks.
 */
func (e *Engine) SetToolCallback(url, token string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.toolCallback.url = strings.TrimSpace(url)
	e.toolCallback.token = strings.TrimSpace(token)
}

/**
 * @brief Register immutable model and client tool options for a new agent.
 * @param options SDK agent options including the Cursor API key.
 * @return Agent identifier, or empty on failure.
 * @return Selected model, or nil on failure.
 * @return Missing options, missing key or closed-engine error; nil on success.
 */
func (e *Engine) CreateAgent(options *sdkv1.AgentOptions) (string, *sdkv1.ModelSelection, error) {
	if e == nil || options == nil {
		return "", nil, errors.New("agent options are required")
	}
	options = proto.Clone(options).(*sdkv1.AgentOptions)
	if err := validateAgentCapabilities(options, ""); err != nil {
		return "", nil, err
	}
	policy, err := resolveAgentToolPolicy(options, agentwire.ToolPolicy{})
	if err != nil {
		return "", nil, err
	}
	apiKey := strings.TrimSpace(options.GetApiKey())
	if apiKey == "" {
		return "", nil, errors.New("options.api_key is required")
	}
	model, err := resolveModelSelection(options.GetModel(), nil)
	if err != nil {
		return "", nil, err
	}
	if _, err := nativeAgentMode(options.GetMode()); err != nil {
		return "", nil, err
	}
	workdir := ""
	if local := options.GetLocal(); local != nil && len(local.GetCwd()) > 0 {
		workdir = strings.TrimSpace(local.GetCwd()[0])
	}
	customTools := map[string]*sdkv1.CustomToolDefinition{}
	if local := options.GetLocal(); local != nil {
		for name, def := range local.GetCustomTools() {
			if def != nil {
				customTools[name] = def
			}
		}
	}
	agentID := agentwire.RandomExecID()
	rec := &agentRecord{
		toolPolicy: policy,
		id:         agentID, apiKey: apiKey, workdir: workdir,
		model: model, mode: options.GetMode(), modePending: true, customTools: customTools,
		conversationID: "agent-" + uuid.NewString(),
		info:           &sdkv1.SdkAgentInfo{AgentId: agentID, Name: options.GetName(), CreatedAt: timestamppb.Now(), LastModified: timestamppb.Now()},
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return "", nil, errors.New("cursor runtime is closed")
	}
	e.agents[agentID] = rec
	if e.stateRoot != "" && len(e.agents) > 4096 {
		delete(e.agents, agentID)
		e.mu.Unlock()
		return "", nil, errors.New("cursor durable agent limit exceeded")
	}
	if err := e.saveAgentLocked(rec); err != nil {
		delete(e.agents, agentID)
		e.mu.Unlock()
		return "", nil, err
	}
	e.mu.Unlock()
	return agentID, proto.Clone(model).(*sdkv1.ModelSelection), nil
}

/**
 * @brief Remove an agent and cancel every run it owns.
 * @param agentID Agent identifier; an unknown identifier is harmless.
 * @return Durable deletion failure, or nil after deleting or for an unknown identifier.
 * @note Concurrent-safe; cancellation callbacks execute outside the registry lock.
 */
func (e *Engine) DeleteAgent(agentID string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor runtime is closed"))
	}
	if e.agents[agentID] != nil {
		if err := e.deleteSavedAgentLocked(agentID); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	delete(e.agents, agentID)
	for id, snapshot := range e.snapshots {
		if snapshot.GetAgentId() == agentID {
			if history := e.histories[id]; history != nil {
				history.notify()
				delete(e.histories, id)
			}
			delete(e.snapshots, id)
		}
	}
	var cancels []context.CancelFunc
	for runID, owner := range e.runAgents {
		if owner == agentID {
			cancels = append(cancels, e.activeRuns[runID])
		}
	}
	e.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

/**
 * @brief Cancel an active run; repeated cancellation of a retained run is harmless.
 * @param runID Run identifier.
 * @return False only when the run is unknown.
 * @note Concurrent-safe; caller need not wait for upstream cleanup.
 */
func (e *Engine) CancelRun(runID string) bool {
	e.mu.Lock()
	cancel := e.activeRuns[runID]
	_, known := e.snapshots[runID]
	e.mu.Unlock()
	if cancel == nil {
		return known
	}
	cancel()
	return true
}

/**
 * @brief Execute a bidirectional Cursor turn with client tool callbacks.
 * @param ctx Run lifetime; cancellation closes upstream and pending callbacks.
 * @param agentID Previously registered agent.
 * @param prompt Ordered conversation supplied by the SDK adapter.
 * @param images Inline image attachments; at most twelve, each at most 1 MiB.
 * @param options Optional model, mode and SDK callback selection for this turn.
 * @param emit SDK event consumer; an error terminates the run.
 * @return Protocol, transport, callback or context error; nil after successful terminal output.
 * @note Registers run cancellation before network I/O and retains an isolated lifecycle snapshot.
 */
func (e *Engine) Send(
	ctx context.Context,
	agentID, prompt string,
	images []*sdkv1.SdkImage,
	options *sdkv1.SendOptions,
	emit func(*sdkv1.RunStreamMessage) error,
) (retErr error) {
	if e == nil {
		return errors.New("local run engine is unavailable")
	}
	if options == nil {
		options = &sdkv1.SendOptions{}
	} else {
		options = proto.Clone(options).(*sdkv1.SendOptions)
	}
	if options.GetCloud() != nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("cloud send options are unavailable for local agents"))
	}
	if len(options.GetMcpServers()) > 0 {
		return connect.NewError(connect.CodeUnimplemented, errors.New("external MCP servers are unavailable in the native client-tool bridge"))
	}
	e.mu.Lock()
	rec, ok := e.agents[agentID]
	e.mu.Unlock()
	if !ok {
		return connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("user message is required"))
	}

	if len(images) > 12 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("too many cursor input images"))
	}
	nativeImages := make([]agentwire.InputImage, 0, len(images))
	for _, img := range images {
		data := img.GetData()
		if data == nil || !strings.HasPrefix(data.GetMimeType(), "image/") {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("cursor requires inline image data and MIME type"))
		}
		if len(data.GetData()) > base64.StdEncoding.EncodedLen(1<<20) {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("cursor image exceeds size limit"))
		}
		decoded, err := base64.StdEncoding.DecodeString(data.GetData())
		if err != nil || len(decoded) == 0 || len(decoded) > 1<<20 {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("invalid cursor image data"))
		}
		dimension := img.GetDimension()
		extension := map[string]string{"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp"}[data.GetMimeType()]
		if extension == "" {
			extension = ".img"
		}
		path := "/ccload/attachments/" + agentwire.RandomExecID() + extension
		nativeImages = append(nativeImages, agentwire.InputImage{Data: decoded, MimeType: data.GetMimeType(), Width: dimension.GetWidth(), Height: dimension.GetHeight(), Path: path})
	}
	if err := e.prepareForcedSend(ctx, agentID, options.GetLocal().GetForce()); err != nil {
		return err
	}
	runID := agentwire.RandomExecID()
	started := time.Now()
	var resultText string
	var runUsage *sdkv1.TokenUsage
	var textBuilder strings.Builder
	var checkpoint []byte
	var controls agentwire.ControlState
	var registered bool
	var apiKey string
	var runModel *sdkv1.ModelSelection
	var runMode uint32
	var err error
	runCtx, cancelCause := context.WithCancelCause(ctx)
	cancelRun := func() { cancelCause(context.Canceled) }
	var toolTasks sync.WaitGroup
	defer func() {
		cancelRun()
		toolTasks.Wait()
		if registered {
			e.mu.Lock()
			if done := e.runDone[runID]; done != nil {
				close(done)
				delete(e.runDone, runID)
			}
			e.mu.Unlock()
			e.runTasks.Done()
		}
	}()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("cursor runtime is closed")
	}
	if e.agents[agentID] != rec {
		e.mu.Unlock()
		return connect.NewError(connect.CodeNotFound, errors.New("agent was closed before the run started"))
	}
	if rec.detached || rec.apiKey == "" {
		e.mu.Unlock()
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("agent must be resumed with a credential before sending"))
	}
	for _, owner := range e.runAgents {
		if owner == agentID {
			e.mu.Unlock()
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("agent already has an active run"))
		}
	}
	runModel, err = resolveModelSelection(options.GetModel(), rec.model)
	if err != nil {
		e.mu.Unlock()
		return err
	}
	mode := options.GetMode()
	if mode == sdkv1.AgentModeOption_AGENT_MODE_OPTION_UNSPECIFIED && rec.modePending {
		mode = rec.mode
	}
	runMode, err = nativeAgentMode(mode)
	if err != nil {
		e.mu.Unlock()
		return err
	}
	if err := e.hydrateNativeLocked(rec); err != nil {
		e.mu.Unlock()
		return connect.NewError(connect.CodeInternal, err)
	}
	checkpoint = append([]byte(nil), rec.checkpoint...)
	controls = rec.controls.Clone()
	for _, image := range nativeImages {
		controls.Images[image.Path] = image
	}
	var imageBytes int
	for _, image := range controls.Images {
		imageBytes += len(image.Data)
	}
	if imageBytes > 32<<20 || len(controls.Images) > 128 {
		e.mu.Unlock()
		return connect.NewError(connect.CodeResourceExhausted, errors.New("agent attachment storage limit exceeded"))
	}
	e.activeRuns[runID] = cancelRun
	e.runDone[runID] = make(chan struct{})
	apiKey = rec.apiKey
	e.runTasks.Add(1)
	registered = true
	e.runAgents[runID] = agentID
	e.snapshots[runID] = &sdkv1.RunSnapshot{RunId: runID, AgentId: agentID, Status: sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING, Model: runModel, CreatedAt: timestamppb.New(started)}
	if options.GetModel() != nil {
		rec.model = proto.Clone(runModel).(*sdkv1.ModelSelection)
	}
	rec.modePending = false
	e.histories[runID] = newRunHistory()
	if rec.info != nil {
		rec.info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_RUNNING
		rec.info.LastModified = timestamppb.New(started)
	}
	e.mu.Unlock()
	defer func() {
		cancelRun()
		toolTasks.Wait()
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.activeRuns, runID)
		delete(e.runAgents, runID)
		defer func() {
			if e.agents[agentID] == rec {
				if err := e.evictNativeLocked(rec); err != nil {
					retErr = errors.Join(retErr, err)
				}
			}
		}()
		if history := e.histories[runID]; history != nil && history.finished {
			return
		}
		if e.agents[agentID] == rec {
			rec.checkpoint = checkpoint
			rec.controls = &controls
			rec.nativeEvicted = false
		}
		if snapshot := e.snapshots[runID]; snapshot != nil {
			if runUsage != nil {
				snapshot.Usage = proto.Clone(runUsage).(*sdkv1.TokenUsage)
			}
			snapshot.DurationMs = uint64(time.Since(started).Milliseconds())
			snapshot.Result = textBuilder.String()
			snapshot.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED
			if retErr != nil {
				snapshot.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_ERROR
				code := connect.CodeOf(retErr).String()
				message := retErr.Error()
				snapshot.ErrorCode = &code
				snapshot.ErrorMessage = &message
				if errors.Is(retErr, context.Canceled) {
					snapshot.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_CANCELLED
				}
			}
			if e.agents[agentID] == rec && rec.info != nil {
				rec.info.LastModified = timestamppb.Now()
				rec.info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_FINISHED
				if retErr != nil {
					rec.info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_ERROR
				}
				for id, owner := range e.runAgents {
					if id != runID && owner == agentID {
						rec.info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_RUNNING
						break
					}
				}
			}
		}
		if e.snapshots[runID] != nil {
			if _, err := e.sealRunHistoryLocked(runID); err != nil {
				retErr = errors.Join(retErr, err)
			}
		}
	}()
	rawEmit := emit
	var emitMu sync.Mutex
	emit = func(message *sdkv1.RunStreamMessage) error {
		emitMu.Lock()
		defer emitMu.Unlock()
		retained, err := e.recordRunEvent(runID, message)
		if err != nil {
			return err
		}
		return rawEmit(retained)
	}
	callbacks := runUpdateCallbacks{deltas: options.GetEnableDeltas(), steps: options.GetEnableSteps(), lastFired: -1, emit: emit}
	defer func() {
		cancelRun()
		toolTasks.Wait()
		if err := callbacks.flushStep(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	userEvent, err := sdkMessage("user", map[string]any{"agent_id": agentID, "run_id": runID, "message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": prompt}}}})
	if err != nil {
		return err
	}
	if err := emit(userEvent); err != nil {
		return err
	}
	ctx = runCtx
	if err := e.persistNativeState(rec, checkpoint, &controls); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	accessToken, runURL, err := e.API.ResolveRunEndpoint(ctx, apiKey)
	if err != nil {
		if errors.Is(err, cursorapi.ErrCredentialRejected) {
			return connect.NewError(connect.CodeUnauthenticated, err)
		}
		return connect.NewError(connect.CodeUnavailable, err)
	}
	var tools []agentwire.Tool
	names := make([]string, 0, len(rec.customTools))
	for name := range rec.customTools {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		def := rec.customTools[name]
		v, err := structpb.NewValue(def.GetInputSchema().AsMap())
		if err != nil {
			return err
		}
		schema, err := proto.Marshal(v)
		if err != nil {
			return err
		}
		tools = append(tools, agentwire.Tool{Name: name, Description: def.GetDescription(), Schema: schema})
	}
	if len(nativeImages) > 0 {
		var attachments strings.Builder
		attachments.WriteString("\n\nAttached image files (read-only; available to the native read tool):\n")
		for i, image := range nativeImages {
			fmt.Fprintf(&attachments, "%d. %s\n", i+1, image.Path)
		}
		prompt += attachments.String()
	}
	first := agentwire.EncodeRunRequest(agentwire.RunInput{
		Checkpoint:     checkpoint,
		ConversationID: rec.conversationID,
		Tools:          tools,
		UserText:       prompt,
		Images:         nativeImages,
		ModelID:        runModel.GetId(),
		Parameters:     nativeModelParameters(runModel),
		Mode:           runMode,
	})
	body, writer, err := e.API.OpenRunStream(ctx, runURL, accessToken, first, rec.toolPolicy)
	if err != nil {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	defer func() { _ = body.Close() }()
	defer func() { _ = writer.Close() }()
	var controlReplies, toolCalls, thinkingBytes int
	var turnEnded bool
	execOps, kvOps := make(map[int]int), make(map[int]int)
	updateOps := make(map[int]int)
	var activeExecsMu sync.Mutex
	activeExecs := make(map[uint32]context.CancelCauseFunc)

	// Cursor can send checkpoint and KV control frames after turnEnded. Keep
	// both halves open until the upstream Connect end frame is received.
	for {
		frame, err := readConnectDataFrame(body)
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if strings.Contains(err.Error(), "cursor agent stream canceled: KV stream closed") {
				log.Printf("[WARN] Cursor KV stream closed: agent_id=%s run_id=%s duration=%s control_replies=%d exec_ops=%v kv_ops=%v tool_calls=%d thinking_bytes=%d text_bytes=%d turn_ended=%t", agentID, runID, time.Since(started).Round(time.Millisecond), controlReplies, execOps, kvOps, toolCalls, thinkingBytes, textBuilder.Len(), turnEnded)
			} else if failure, ok := errors.AsType[*connect.Error](err); ok && failure.Code() == connect.CodeResourceExhausted {
				log.Printf("[WARN] Cursor upstream resource exhausted: agent_id=%s run_id=%s duration=%s control_replies=%d exec_ops=%v kv_ops=%v tool_calls=%d thinking_bytes=%d text_bytes=%d turn_ended=%t detail_count=%d", agentID, runID, time.Since(started).Round(time.Millisecond), controlReplies, execOps, kvOps, toolCalls, thinkingBytes, textBuilder.Len(), turnEnded, len(failure.Details()))
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if connectErr, ok := errors.AsType[*connect.Error](err); ok {
				return connectErr
			}
			return connect.NewError(connect.CodeUnknown, err)
		}
		if kind := agentwire.InteractionUpdateKind(frame); kind != 0 {
			updateOps[kind]++
		}
		if state, ok, err := agentwire.DecodeCheckpoint(frame); err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		} else if ok {
			checkpoint = state
			if err := e.persistNativeState(rec, checkpoint, &controls); err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			continue
		}
		if query, ok := agentwire.InteractionQuery(frame); ok {
			reply, err := agentwire.ReplyInteraction(frame)
			if err != nil {
				return connect.NewError(connect.CodeUnknown, err)
			}
			if reply != nil {
				if _, err := writer.Write(reply); err != nil {
					return err
				}
				controlReplies++
				continue
			}
			return connect.NewError(connect.CodeUnimplemented, fmt.Errorf("cursor interaction query %d is not supported by this bridge", query))
		}
		top, operation, err := agentwire.ControlOperation(frame)
		if err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		}
		if abortedID, ok := agentwire.DecodeExecAbort(frame); ok {
			activeExecsMu.Lock()
			cancel := activeExecs[abortedID]
			activeExecsMu.Unlock()
			if cancel != nil {
				cancel(errUpstreamExecAborted)
			}
			continue
		}
		switch top {
		case 2:
			execOps[operation]++
		case 4:
			kvOps[operation]++
		}
		call, err := agentwire.DecodeToolCall(frame)
		if err != nil {
			return err
		}
		if call != nil {
			toolCalls++
			toolCtx, cancelTool := context.WithCancelCause(ctx)
			activeExecsMu.Lock()
			if _, exists := activeExecs[call.ID]; exists {
				activeExecsMu.Unlock()
				cancelTool(context.Canceled)
				return errors.New("cursor sent a duplicate active exec identifier")
			}
			activeExecs[call.ID] = cancelTool
			activeExecsMu.Unlock()
			// Tool execution may wait for another client HTTP turn. Keep reading
			// Cursor control frames so KV and parallel callbacks can progress.
			toolTasks.Go(func() {
				defer cancelTool(context.Canceled)
				releaseExec := sync.OnceFunc(func() {
					activeExecsMu.Lock()
					delete(activeExecs, call.ID)
					activeExecsMu.Unlock()
				})
				defer releaseExec()
				if err := e.recordToolHistory(runID, agentID, call, "running", nil, false, nil, emit); err != nil {
					cancelCause(err)
					return
				}
				var execMu sync.Mutex
				heartbeatDone := make(chan struct{})
				go func() {
					defer close(heartbeatDone)
					ticker := time.NewTicker(3 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-toolCtx.Done():
							return
						case <-ticker.C:
							execMu.Lock()
							if toolCtx.Err() != nil {
								execMu.Unlock()
								return
							}
							_, err := writer.Write(agentwire.EncodeExecHeartbeat(call.ID))
							execMu.Unlock()
							if err != nil {
								return
							}
						}
					}
				}()
				result, failed, err := e.invokeDurableTool(toolCtx, agentID, rec, call)
				if historyErr := e.recordToolHistory(runID, agentID, call, "completed", result, failed, err, emit); historyErr != nil && err == nil {
					err = historyErr
				}
				execMu.Lock()
				cause := context.Cause(toolCtx)
				cancelTool(context.Canceled)
				// A terminal reply lets Cursor immediately reuse this identifier.
				// Remove the old owner before publishing it, and never delete a
				// replacement owner from this goroutine's deferred cleanup.
				releaseExec()
				if errors.Is(cause, errUpstreamExecAborted) {
					_, err = writer.Write(agentwire.EncodeExecThrow(call.ID, errUpstreamExecAborted.Error()))
				} else if err == nil {
					var reply []byte
					reply, err = agentwire.EncodeToolResult(call, result, failed)
					if err == nil {
						_, err = writer.Write(reply)
					}
					if err == nil {
						_, err = writer.Write(agentwire.EncodeExecStreamClose(call.ID))
					}
				}
				execMu.Unlock()
				<-heartbeatDone
				if err != nil {
					cancelCause(err)
				}
			})
		}
		response := controls.ReplyControl(frame, tools)
		if response != nil {
			if top == 4 && operation == 3 {
				if err := e.persistNativeState(rec, checkpoint, &controls); err != nil {
					return connect.NewError(connect.CodeInternal, err)
				}
			}
			if _, err := writer.Write(response); err != nil {
				return err
			}
			controlReplies++
			if top == 2 {
				id, ok := agentwire.ExecControlID(frame)
				if ok {
					if _, err := writer.Write(agentwire.EncodeExecStreamClose(id)); err != nil {
						return err
					}
				}
			}
		} else if top == 2 && operation != 0 && call == nil {
			id, _ := agentwire.ExecControlID(frame)
			if _, err := writer.Write(agentwire.EncodeExecThrow(id, "Unsupported Cursor exec operation")); err != nil {
				return err
			}
			if _, err := writer.Write(agentwire.EncodeExecStreamClose(id)); err != nil {
				return err
			}
		}
		update, err := agentwire.DecodeSDKInteractionUpdate(frame, rec.conversationID)
		if err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		}
		if update != nil {
			if err := callbacks.apply(update); err != nil {
				return err
			}
			if err := callbacks.lifecycle(update, agentID, runID); err != nil {
				return err
			}
		}
		for _, delta := range agentwire.DecodeStreamDeltas(frame) {
			if u := delta.Usage; u != nil {
				if runUsage == nil {
					runUsage = &sdkv1.TokenUsage{}
				}
				for _, count := range []struct {
					target *int64
					value  int64
				}{{&runUsage.InputTokens, u.InputTokens}, {&runUsage.OutputTokens, u.OutputTokens}, {&runUsage.CacheReadTokens, u.CacheReadTokens}, {&runUsage.CacheWriteTokens, u.CacheWriteTokens}} {
					if *count.target > math.MaxInt64-count.value {
						return errors.New("cursor cumulative usage exceeds supported range")
					}
					*count.target += count.value
				}
				if runUsage.InputTokens > math.MaxInt64-runUsage.OutputTokens {
					return errors.New("cursor cumulative usage exceeds supported range")
				}
				runUsage.TotalTokens = runUsage.InputTokens + runUsage.OutputTokens
				usage := map[string]any{"inputTokens": u.InputTokens, "outputTokens": u.OutputTokens, "cacheReadTokens": u.CacheReadTokens, "cacheWriteTokens": u.CacheWriteTokens, "totalTokens": u.InputTokens + u.OutputTokens}
				if u.ReasoningTokens != nil {
					n := runUsage.GetReasoningTokens()
					if n > math.MaxInt64-*u.ReasoningTokens {
						return errors.New("cursor reasoning usage exceeds supported range")
					}
					n += *u.ReasoningTokens
					runUsage.ReasoningTokens = &n
					usage["reasoningTokens"] = *u.ReasoningTokens
				}
				msg, err := sdkMessage("usage", map[string]any{"agent_id": agentID, "run_id": runID, "usage": usage})
				if err != nil {
					return err
				}
				if err := emit(msg); err != nil {
					return err
				}
			}
			if delta.Thinking != "" {
				thinkingBytes += len(delta.Thinking)
				msg, err := sdkMessage("thinking", map[string]any{
					"agent_id": agentID, "run_id": runID, "text": delta.Thinking,
				})
				if err != nil {
					return err
				}
				if err := emit(msg); err != nil {
					return err
				}
			}
			if delta.Text != "" {
				textBuilder.WriteString(delta.Text)
				msg, err := sdkMessage("assistant", map[string]any{
					"agent_id": agentID, "run_id": runID,
					"message": map[string]any{
						"role":    "assistant",
						"content": []any{map[string]any{"type": "text", "text": delta.Text}},
					},
				})
				if err != nil {
					return err
				}
				if err := emit(msg); err != nil {
					return err
				}
			}
			if delta.Done {
				turnEnded = true
			}
		}
	}
	if !turnEnded {
		return connect.NewError(connect.CodeUnavailable, errors.New("cursor agent stream ended without turn completion"))
	}
	resultText = callbacks.finalAssistantText()
	if err := e.persistNativeState(rec, checkpoint, &controls); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if strings.TrimSpace(resultText) == "" {
		log.Printf("[WARN] Cursor turn ended without assistant output: agent_id=%s run_id=%s control_replies=%d exec_ops=%v kv_ops=%v update_ops=%v thinking_bytes=%d checkpoint_bytes=%d missing_blobs=%d output_tokens=%d reasoning_tokens=%d", agentID, runID, controlReplies, execOps, kvOps, updateOps, thinkingBytes, len(checkpoint), controls.MissingBlobs, runUsage.GetOutputTokens(), runUsage.GetReasoningTokens())
		return connect.NewError(connect.CodeUnavailable, errors.New("cursor agent stream ended without assistant output"))
	}
	cancelRun()
	toolTasks.Wait()
	e.mu.Lock()
	snapshot := e.snapshots[runID]
	if snapshot == nil {
		e.mu.Unlock()
		return connect.NewError(connect.CodeNotFound, errors.New("run was deleted before completion"))
	}
	snapshot.Result = resultText
	rec.checkpoint = checkpoint
	rec.controls = &controls
	rec.nativeEvicted = false
	snapshot.Status = sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_FINISHED
	snapshot.DurationMs = uint64(time.Since(started).Milliseconds())
	if runUsage != nil {
		snapshot.Usage = proto.Clone(runUsage).(*sdkv1.TokenUsage)
	}
	if rec.info != nil {
		rec.info.Status = sdkv1.AgentInfoStatus_AGENT_INFO_STATUS_FINISHED
		rec.info.LastModified = timestamppb.Now()
	}
	terminal, err := e.sealRunHistoryLocked(runID)
	delete(e.activeRuns, runID)
	delete(e.runAgents, runID)
	e.mu.Unlock()
	if err != nil {
		return err
	}
	for _, event := range terminal {
		if err := rawEmit(event); err != nil {
			return err
		}
	}
	return nil
}

func sdkMessage(typ string, payload map[string]any) (*sdkv1.RunStreamMessage, error) {
	st, err := structpb.NewStruct(payload)
	if err != nil {
		return nil, fmt.Errorf("build sdk message: %w", err)
	}
	return &sdkv1.RunStreamMessage{
		Envelope: &sdkv1.RunStreamMessage_SdkMessage{
			SdkMessage: &sdkv1.SdkMessage{Type: typ, Message: st},
		},
	}, nil
}

/**
 * @brief Read one Connect stream data envelope and propagate upstream terminal errors.
 * @param r Upstream stream.
 * @return Data payload, or nil at a successful end envelope.
 * @return io.EOF for a successful end envelope; an error for truncation or upstream failure.
 */
func readConnectDataFrame(r io.Reader) ([]byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[1:5])
	if length > 8<<20 {
		return nil, errors.New("cursor agent stream frame exceeds size limit")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if header[0] == 2 {
		var end struct {
			Error *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
				Details []struct {
					Type  string `json:"type"`
					Value string `json:"value"`
				} `json:"details"`
			} `json:"error"`
		}
		if err := json.Unmarshal(payload, &end); err != nil {
			return nil, fmt.Errorf("invalid cursor agent end frame: %w", err)
		}
		if end.Error != nil {
			message := fmt.Errorf("cursor agent stream %s: %s", end.Error.Code, end.Error.Message)
			var code connect.Code
			if err := code.UnmarshalText([]byte(end.Error.Code)); err != nil {
				code = connect.CodeUnknown
			}
			upstream := connect.NewError(code, message)
			for _, wireDetail := range end.Error.Details {
				data, err := connect.DecodeBinaryHeader(wireDetail.Value)
				if err != nil {
					return nil, errors.New("invalid cursor agent error detail encoding")
				}
				typeURL := wireDetail.Type
				if !strings.Contains(typeURL, "/") {
					typeURL = "type.googleapis.com/" + typeURL
				}
				detail, err := connect.NewErrorDetail(&anypb.Any{TypeUrl: typeURL, Value: data})
				if err != nil {
					return nil, errors.New("invalid cursor agent error detail")
				}
				upstream.AddDetail(detail)
			}
			return nil, upstream
		}
		return nil, io.EOF
	}
	if header[0] != 0 {
		return nil, fmt.Errorf("unsupported cursor agent stream frame flags: %d", header[0])
	}
	return payload, nil
}

/**
 * @brief Route only declared MCP tools through the SDK callback.
 * @param ctx Owning run context, also bounding image downloads.
 * @param agentID SDK agent identifier.
 * @param rec Immutable agent tool declarations.
 * @param call Native tool invocation and its correlation IDs.
 * @return Typed tool result, or nil when canceled or media resolution fails.
 * @return Whether the tool reported an error.
 * @return Cancellation, unavailable callback or media error; nil on a delivered result.
 */
func (e *Engine) invokeTool(ctx context.Context, agentID string, rec *agentRecord, call *agentwire.ToolCall) (map[string]any, bool, error) {
	if _, ok := rec.customTools[call.Name]; !ok {
		return map[string]any{"error": "Tool is not available"}, true, nil
	}
	e.mu.Lock()
	url, token := e.toolCallback.url, e.toolCallback.token
	e.mu.Unlock()
	if url == "" {
		return nil, false, errors.New("custom tool callback is unavailable")
	}
	client := sdkv1connect.NewSdkCustomToolCallbackServiceClient(http.DefaultClient, url)
	request := connect.NewRequest(&sdkv1.CallCustomToolRequest{AgentId: agentID, ToolName: call.Name, Args: call.Args, ToolCallId: &call.CallID})
	request.Header().Set("Authorization", "Bearer "+token)
	response, err := client.CallCustomTool(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, context.Cause(ctx)
		}
		return nil, false, fmt.Errorf("custom tool callback outcome is unknown: %w", err)
	}
	value := response.Msg.GetResult().AsMap()
	if content, ok := value["content"].([]any); ok {
		for i, item := range content {
			block, ok := item.(map[string]any)
			if !ok {
				continue
			}
			kind, _ := block["type"].(string)
			var uri string
			if kind == "image_url" || kind == "input_image" {
				uri, _ = block["image_url"].(string)
				if obj, ok := block["image_url"].(map[string]any); ok {
					uri, _ = obj["url"].(string)
				}
			}
			if kind == "image" {
				if source, ok := block["source"].(map[string]any); ok && source["type"] == "url" {
					uri, _ = source["url"].(string)
				}
			}
			if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
				data, mime, err := cursormedia.Fetch(ctx, uri)
				if err != nil {
					return nil, false, err
				}
				content[i] = map[string]any{"type": "image", "data": base64.StdEncoding.EncodeToString(data), "mimeType": mime}
			}
		}
	}

	_, isEnvelope := value["content"].([]any)
	flag, _ := value["isError"].(bool)
	failed := isEnvelope && flag
	return value, failed, nil
}

/**
 * @brief Reject new runs and cancel all active upstream requests and callbacks.
 * @note Concurrent-safe and idempotent; cancellation executes outside the lock.
 */
func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		cancels := make([]context.CancelFunc, 0, len(e.activeRuns))
		for _, cancel := range e.activeRuns {
			cancels = append(cancels, cancel)
		}
		e.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		e.runTasks.Wait()
		e.mu.Lock()
		if e.stateLease != nil {
			_ = e.stateLease.Close()
			e.stateLease = nil
		}
		e.mu.Unlock()
	})
}

/**
 * @brief Return a detached snapshot of a known run.
 * @param runID Active or retained run identifier.
 * @return Cloned snapshot, or nil for an unknown or deleted run.
 * @note Concurrent-safe; callers may mutate the returned snapshot.
 */
func (e *Engine) GetRun(runID string) *sdkv1.RunSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	snapshot := e.snapshots[runID]
	if snapshot == nil {
		return nil
	}
	return proto.Clone(snapshot).(*sdkv1.RunSnapshot)
}
