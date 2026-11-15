package cursorbridgeruntime

import (
	"context"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Resume a durable local agent with a newly supplied credential and execution overrides.
 * @param _ Unused RPC context; no upstream request is made.
 * @param req Existing agent identity and fresh options.
 * @return Stable agent identity and selected model, or nil on failure.
 * @return Registry validation or persistence failure.
 */
func (s *bridgeServices) ResumeAgent(_ context.Context, req *connect.Request[sdkv1.ResumeAgentRequest]) (*connect.Response[sdkv1.ResumeAgentResponse], error) {
	model, err := s.engine.ResumeAgent(req.Msg.GetAgentId(), req.Msg.GetOptions())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ResumeAgentResponse{AgentId: req.Msg.GetAgentId(), Model: model}), nil
}

/**
 * @brief Read detached local agent metadata through the authenticated SDK boundary.
 * @param _ Unused RPC context; the operation performs no I/O.
 * @param req Agent lookup request.
 * @return Agent metadata, or nil when not found.
 * @return NotFound for unknown or deleted agents.
 */
func (s *bridgeServices) GetAgent(_ context.Context, req *connect.Request[sdkv1.GetAgentRequest]) (*connect.Response[sdkv1.GetAgentResponse], error) {
	info, err := s.engine.AgentInfo(req.Msg.GetAgentId())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.GetAgentResponse{Agent: info}), nil
}

/**
 * @brief List local agent metadata with archive, workspace and pagination filters.
 * @param _ Unused RPC context; the operation performs no I/O.
 * @param req List request and optional filters.
 * @return Metadata page, or nil for invalid filters.
 * @return InvalidArgument for unsupported or malformed options.
 */
func (s *bridgeServices) ListAgents(_ context.Context, req *connect.Request[sdkv1.ListAgentsRequest]) (*connect.Response[sdkv1.ListAgentsResponse], error) {
	items, next, err := s.engine.ListAgents(req.Msg.GetOptions())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ListAgentsResponse{Items: items, NextCursor: next}), nil
}

/**
 * @brief Hide a local agent from ordinary lists without deleting history.
 * @param _ Unused RPC context; the operation performs no I/O.
 * @param req Agent archive request.
 * @return Idempotent acknowledgement, or nil when not found.
 * @return NotFound for unknown or deleted agents.
 */
func (s *bridgeServices) ArchiveAgent(_ context.Context, req *connect.Request[sdkv1.ArchiveAgentRequest]) (*connect.Response[sdkv1.ArchiveAgentResponse], error) {
	if err := s.engine.ArchiveAgent(req.Msg.GetAgentId(), true); err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ArchiveAgentResponse{}), nil
}

/**
 * @brief Restore a local agent to ordinary list visibility.
 * @param _ Unused RPC context; the operation performs no I/O.
 * @param req Agent unarchive request.
 * @return Idempotent acknowledgement, or nil when not found.
 * @return NotFound for unknown or deleted agents.
 */
func (s *bridgeServices) UnarchiveAgent(_ context.Context, req *connect.Request[sdkv1.UnarchiveAgentRequest]) (*connect.Response[sdkv1.UnarchiveAgentResponse], error) {
	if err := s.engine.ArchiveAgent(req.Msg.GetAgentId(), false); err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.UnarchiveAgentResponse{}), nil
}

/**
 * @brief List detached retained run snapshots for one local agent.
 * @param _ Unused RPC context; the operation performs no I/O.
 * @param req Agent identifier and optional list filters.
 * @return Snapshot page, or nil on invalid input.
 * @return NotFound for an unknown agent or InvalidArgument for malformed filters.
 */
func (s *bridgeServices) ListRuns(_ context.Context, req *connect.Request[sdkv1.ListRunsRequest]) (*connect.Response[sdkv1.ListRunsResponse], error) {
	items, next, err := s.engine.ListRuns(req.Msg.GetAgentId(), req.Msg.GetOptions())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ListRunsResponse{Items: items, NextCursor: next}), nil
}
