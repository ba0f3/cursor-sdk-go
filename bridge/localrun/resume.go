package localrun

import (
	"context"
	"errors"
	"log"
	"strings"

	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Reopen an existing agent with a caller-supplied credential and optional execution overrides.
 * @param id Stable agent identifier, preserving native conversation and checkpoint identity.
 * @param options Required credential and optional model or local custom tools.
 * @return Selected model, or nil on failure.
 * @return InvalidArgument, NotFound, FailedPrecondition, or durable save error.
 */
func (e *Engine) ResumeAgent(id string, options *sdkv1.AgentOptions) (*sdkv1.ModelSelection, error) {
	if options == nil || strings.TrimSpace(options.GetApiKey()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("options.api_key is required for resume"))
	}
	options = proto.Clone(options).(*sdkv1.AgentOptions)
	if err := validateAgentCapabilities(options, id); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	previous := e.agents[id]
	if previous == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	if e.closed {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor runtime is closed"))
	}
	for _, owner := range e.runAgents {
		if owner == id {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("agent already has an active run"))
		}
	}
	if err := e.hydrateNativeLocked(previous); err != nil {
		return nil, err
	}
	record := *previous
	policy, err := resolveAgentToolPolicy(options, previous.toolPolicy)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	record.toolPolicy = policy
	record.info = proto.Clone(previous.info).(*sdkv1.SdkAgentInfo)
	record.apiKey = strings.TrimSpace(options.GetApiKey())
	record.detached = false
	record.model, err = resolveModelSelection(options.GetModel(), previous.model)
	if err != nil {
		return nil, err
	}
	if _, err := nativeAgentMode(options.GetMode()); err != nil {
		return nil, err
	}
	if options.GetMode() != sdkv1.AgentModeOption_AGENT_MODE_OPTION_UNSPECIFIED {
		record.mode, record.modePending = options.GetMode(), true
	}
	if options.GetName() != "" {
		record.info.Name = options.GetName()
	}
	if local := options.GetLocal(); local != nil {
		if len(local.GetCwd()) > 0 {
			record.workdir = local.GetCwd()[0]
		}
		if local.GetCustomTools() != nil {
			record.customTools = local.GetCustomTools()
		}
	}
	record.info.LastModified = timestamppb.Now()
	if err := e.saveAgentLocked(&record); err != nil {
		return nil, err
	}
	e.agents[id] = &record
	return proto.Clone(record.model).(*sdkv1.ModelSelection), nil
}

/**
 * @brief Resolve optional SDK tool restrictions while retaining inherited policy on resume.
 * @param options Explicit caller restrictions.
 * @param previous Existing validated policy, or zero value for a new agent.
 * @return Policy with nil versus empty allowlist semantics preserved.
 * @return Unknown tool-name error before any network request or state change.
 */
func resolveAgentToolPolicy(options *sdkv1.AgentOptions, previous agentwire.ToolPolicy) (agentwire.ToolPolicy, error) {
	var err error
	if options.GetTools() != nil {
		previous.Allowed, err = agentwire.ResolveToolNames(options.GetTools().GetNames())
		if err != nil {
			return agentwire.ToolPolicy{}, err
		}
	}
	if options.GetDisallowedTools() != nil {
		previous.Excluded, err = agentwire.ResolveToolNames(options.GetDisallowedTools())
		if err != nil {
			return agentwire.ToolPolicy{}, err
		}
	}
	return previous, nil
}

/**
 * @brief Dispose an SDK handle and cancel active runs while preserving durable history for resume.
 * @param id Agent identifier; an absent handle is harmless.
 * @note Credentials are cleared under the registry mutex; in-flight runs own private credential snapshots.
 */
func (e *Engine) CloseAgent(id string) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	if record := e.agents[id]; record != nil {
		record.detached = true
		record.apiKey = ""
		if err := e.evictNativeLocked(record); err != nil {
			log.Printf("[WARN] failed to release durable Cursor context for agent %s: %v", id, err)
		}
	}
	var cancels []context.CancelFunc
	for run, owner := range e.runAgents {
		if owner == id {
			cancels = append(cancels, e.activeRuns[run])
		}
	}
	e.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
