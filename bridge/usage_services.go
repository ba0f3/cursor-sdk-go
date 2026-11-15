package cursorbridgeruntime

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Expose backend-reported agent usage and optional cost through the SDK RPC.
 * @param ctx Request cancellation and deadline.
 * @param req Agent identity and optional backend usage UUID.
 * @return Actual usage, or nil on failure.
 * @return Registry, authentication, cancellation or upstream error.
 */
func (s *bridgeServices) GetUsage(ctx context.Context, req *connect.Request[sdkv1.GetUsageRequest]) (*connect.Response[sdkv1.GetUsageResponse], error) {
	usage, err := s.engine.AgentUsage(ctx, req.Msg.GetAgentId(), req.Msg.GetRunId())
	if err != nil {
		if _, ok := errors.AsType[*connect.Error](err); ok {
			return nil, err
		}
		return nil, cursorAccountError(err)
	}
	return connect.NewResponse(&sdkv1.GetUsageResponse{Usage: usage}), nil
}

/**
 * @brief Match the official local SDK's empty artifact collection after validating the agent.
 * @param _ Unused RPC context.
 * @param req Local agent identifier.
 * @return Empty artifact collection, or nil for an unknown agent.
 * @return NotFound when the agent no longer exists.
 */
func (s *bridgeServices) ListArtifacts(_ context.Context, req *connect.Request[sdkv1.ListArtifactsRequest]) (*connect.Response[sdkv1.ListArtifactsResponse], error) {
	if _, err := s.engine.AgentInfo(req.Msg.GetAgentId()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ListArtifactsResponse{}), nil
}

/**
 * @brief Report the official local SDK artifact limitation without reading arbitrary host paths.
 * @param _ Unused RPC context.
 * @param req Agent and requested artifact path.
 * @param _ Unused stream because local artifact downloads are unavailable.
 * @return NotFound for an unknown agent or FailedPrecondition for local artifact downloads.
 */
func (s *bridgeServices) DownloadArtifact(_ context.Context, req *connect.Request[sdkv1.DownloadArtifactRequest], _ *connect.ServerStream[sdkv1.DownloadArtifactChunk]) error {
	if _, err := s.engine.AgentInfo(req.Msg.GetAgentId()); err != nil {
		return err
	}
	return connect.NewError(connect.CodeFailedPrecondition, errors.New("artifacts are not implemented for local SDK agents yet"))
}
