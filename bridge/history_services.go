package cursorbridgeruntime

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Wait for a run without tying its lifetime to this observer RPC.
 * @param ctx Observer deadline and cancellation.
 * @param req Registered or retained run identity.
 * @return Final generation result, or nil on observer failure.
 * @return NotFound, Canceled, DeadlineExceeded or history failure.
 */
func (s *bridgeServices) WaitLiveRun(ctx context.Context, req *connect.Request[sdkv1.WaitLiveRunRequest]) (*connect.Response[sdkv1.WaitLiveRunResponse], error) {
	result, err := s.engine.WaitRun(ctx, req.Msg.GetRunId())
	if err != nil {
		return nil, historyRPCError(err)
	}
	return connect.NewResponse(&sdkv1.WaitLiveRunResponse{Result: result}), nil
}

/**
 * @brief Replay and follow a run from an exclusive SDK offset.
 * @param ctx Observer cancellation; does not cancel generation.
 * @param req Run identity and optional after-offset token.
 * @param stream Authenticated observer stream; no registry lock is held while sending.
 * @return Context, observer-send or missing-run error; nil after terminal events.
 */
func (s *bridgeServices) ObserveRun(ctx context.Context, req *connect.Request[sdkv1.ObserveRunRequest], stream *connect.ServerStream[sdkv1.RunStreamMessage]) error {
	return historyRPCError(withRunStreamKeepalive(ctx, runStreamKeepaliveIdle, func(observerCtx context.Context, emit func(*sdkv1.RunStreamMessage) error) error {
		return s.engine.ObserveRun(observerCtx, req.Msg.GetRunId(), req.Msg.GetAfterOffset(), emit)
	}, stream.Send))
}

/**
 * @brief Classify observer cancellation while preserving typed run errors.
 * @param err Observer or registry error, possibly nil.
 * @return Connect context error or the original error; nil on success.
 */
func historyRPCError(err error) error {
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	return err
}

/**
 * @brief Return normalized conversation turns after the selected run reaches a terminal state.
 * @param ctx Observer deadline and cancellation.
 * @param req Run identity.
 * @return JSON conversation response, or nil on failure.
 * @return Missing-run, observer cancellation or serialization failure.
 */
func (s *bridgeServices) GetRunConversation(ctx context.Context, req *connect.Request[sdkv1.GetRunConversationRequest]) (*connect.Response[sdkv1.GetRunConversationResponse], error) {
	conversation, err := s.engine.RunConversation(ctx, req.Msg.GetRunId())
	if err != nil {
		return nil, historyRPCError(err)
	}
	return connect.NewResponse(&sdkv1.GetRunConversationResponse{ConversationJson: conversation}), nil
}

/**
 * @brief List structured local conversation turns with stable message identities.
 * @param _ Unused RPC context; no network or filesystem work is performed.
 * @param req Agent identity and optional turn pagination/workspace filters.
 * @return SDK message page, or nil on invalid input.
 * @return Missing-agent, invalid-filter or serialization error.
 */
func (s *bridgeServices) ListAgentMessages(_ context.Context, req *connect.Request[sdkv1.ListAgentMessagesRequest]) (*connect.Response[sdkv1.ListAgentMessagesResponse], error) {
	messages, err := s.engine.AgentMessages(req.Msg.GetAgentId(), req.Msg.GetOptions())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ListAgentMessagesResponse{Messages: messages}), nil
}
