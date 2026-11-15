package cursorbridgeruntime

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
	"github.com/ba0f3/cursor-sdk-go/bridge/localrun"
)

type bridgeServices struct {
	sdkv1connect.UnimplementedSdkBridgeControlServiceHandler
	sdkv1connect.UnimplementedSdkCursorServiceHandler
	sdkv1connect.UnimplementedSdkAgentServiceHandler

	engine   *localrun.Engine
	shutdown func() ///< Stops this bridge instance without terminating the host process.
}

func newBridgeServices(engine *localrun.Engine) *bridgeServices {
	return &bridgeServices{engine: engine}
}

func (s *bridgeServices) Ping(_ context.Context, _ *connect.Request[sdkv1.PingRequest]) (*connect.Response[sdkv1.PingResponse], error) {
	return connect.NewResponse(&sdkv1.PingResponse{Message: "pong"}), nil
}

/**
 * @brief Acknowledge shutdown while asynchronously draining this RPC and active runs.
 * @param _ Unused request context; shutdown must outlive the acknowledgement.
 * @param _ Authenticated shutdown request.
 * @return Empty acknowledgement.
 * @return Always nil.
 */
func (s *bridgeServices) Shutdown(context.Context, *connect.Request[sdkv1.ShutdownRequest]) (*connect.Response[sdkv1.ShutdownResponse], error) {
	if s.shutdown != nil {
		go s.shutdown()
	} else if s.engine != nil {
		s.engine.Close()
	}
	return connect.NewResponse(&sdkv1.ShutdownResponse{}), nil
}

func (s *bridgeServices) GetVersion(context.Context, *connect.Request[sdkv1.GetVersionRequest]) (*connect.Response[sdkv1.GetVersionResponse], error) {
	return connect.NewResponse(&sdkv1.GetVersionResponse{
		BridgeVersion:   BridgeVersion,
		ProtocolVersion: ProtocolVersion,
		Capabilities:    append([]string(nil), bridgeCapabilities...),
	}), nil
}

func (s *bridgeServices) SetToolCallback(_ context.Context, req *connect.Request[sdkv1.SetToolCallbackRequest]) (*connect.Response[sdkv1.SetToolCallbackResponse], error) {
	if s.engine != nil {
		s.engine.SetToolCallback(req.Msg.GetUrl(), req.Msg.GetAuthToken())
	}
	return connect.NewResponse(&sdkv1.SetToolCallbackResponse{}), nil
}

func (s *bridgeServices) ListModels(ctx context.Context, req *connect.Request[sdkv1.ListModelsRequest]) (*connect.Response[sdkv1.ListModelsResponse], error) {
	apiKey := ""
	if req.Msg.GetOptions() != nil {
		apiKey = strings.TrimSpace(req.Msg.GetOptions().GetApiKey())
	}
	if apiKey == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("options.api_key is required"))
	}
	ids, err := s.engine.API.ListModelIDs(ctx, apiKey)
	if err != nil || len(ids) == 0 {
		ids = append([]string(nil), defaultBridgeModels...)
	}
	items := make([]*sdkv1.SdkModel, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		items = append(items, &sdkv1.SdkModel{Id: id})
	}
	return connect.NewResponse(&sdkv1.ListModelsResponse{Items: items}), nil
}

/**
 * @brief Return the account associated with the supplied Cursor API key.
 * @param ctx Request deadline and cancellation.
 * @param req Authenticated SDK account request.
 * @return Account response, or nil on upstream failure.
 * @return InvalidArgument for missing keys, Unauthenticated for rejected keys, or upstream failure.
 */
func (s *bridgeServices) Me(ctx context.Context, req *connect.Request[sdkv1.MeRequest]) (*connect.Response[sdkv1.MeResponse], error) {
	key := strings.TrimSpace(req.Msg.GetOptions().GetApiKey())
	if key == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("options.api_key is required"))
	}
	user, err := s.engine.API.Account(ctx, key)
	if err != nil {
		return nil, cursorAccountError(err)
	}
	return connect.NewResponse(&sdkv1.MeResponse{User: user}), nil
}

/**
 * @brief Return repositories visible to the supplied Cursor API key.
 * @param ctx Request deadline and cancellation.
 * @param req Authenticated SDK repository request.
 * @return Repository response, or nil on upstream failure.
 * @return InvalidArgument for missing keys, Unauthenticated for rejected keys, or upstream failure.
 */
func (s *bridgeServices) ListRepositories(ctx context.Context, req *connect.Request[sdkv1.ListRepositoriesRequest]) (*connect.Response[sdkv1.ListRepositoriesResponse], error) {
	key := strings.TrimSpace(req.Msg.GetOptions().GetApiKey())
	if key == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("options.api_key is required"))
	}
	items, err := s.engine.API.Repositories(ctx, key)
	if err != nil {
		return nil, cursorAccountError(err)
	}
	return connect.NewResponse(&sdkv1.ListRepositoriesResponse{Items: items}), nil
}

/**
 * @brief Preserve credential and context failures at the SDK account boundary.
 * @param err Account transport or response failure.
 * @return Classified Connect error without credential or response-body diagnostics.
 */
func cursorAccountError(err error) error {
	code := connect.CodeUnavailable
	switch {
	case errors.Is(err, cursorapi.ErrCredentialRejected):
		code = connect.CodeUnauthenticated
	case errors.Is(err, context.Canceled):
		code = connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, err)
}

func (s *bridgeServices) CreateAgent(_ context.Context, req *connect.Request[sdkv1.CreateAgentRequest]) (*connect.Response[sdkv1.CreateAgentResponse], error) {
	if req.Msg.GetOptions().GetCloud() != nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("cloud agents are unavailable in the local bridge"))
	}
	if req.Msg.GetIdempotencyKey() != "" {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("Idempotency-Key is only supported for cloud CreateAgent in v1"))
	}
	agentID, model, err := s.engine.CreateAgent(req.Msg.GetOptions())
	if err != nil {
		if _, ok := errors.AsType[*connect.Error](err); ok {
			return nil, err
		}
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(&sdkv1.CreateAgentResponse{AgentId: agentID, Model: model}), nil
}

func (s *bridgeServices) Send(ctx context.Context, req *connect.Request[sdkv1.SendRequest], stream *connect.ServerStream[sdkv1.RunStreamMessage]) error {
	agentID := strings.TrimSpace(req.Msg.GetAgentId())
	if _, err := s.engine.AgentInfo(agentID); err != nil {
		return err
	}
	if req.Msg.GetIdempotencyKey() != "" {
		return connect.NewError(connect.CodeUnimplemented, errors.New("Idempotency-Key is only supported for cloud Send in v1"))
	}
	if req.Msg.GetOptions().GetCloud() != nil {
		return connect.NewError(connect.CodeUnimplemented, errors.New("cloud send options are unavailable in the local bridge"))
	}
	message := req.Msg.GetMessage()
	prompt := ""
	if message != nil {
		prompt = strings.TrimSpace(message.GetText())
	}
	return withRunStreamKeepalive(ctx, runStreamKeepaliveIdle, func(runCtx context.Context, emit func(*sdkv1.RunStreamMessage) error) error {
		return s.engine.Send(runCtx, agentID, prompt, message.GetImages(), req.Msg.GetOptions(), emit)
	}, stream.Send)
}

func (s *bridgeServices) DeleteAgent(_ context.Context, req *connect.Request[sdkv1.DeleteAgentRequest]) (*connect.Response[sdkv1.DeleteAgentResponse], error) {
	if err := s.engine.DeleteAgent(strings.TrimSpace(req.Msg.GetAgentId())); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&sdkv1.DeleteAgentResponse{}), nil
}

/**
 * @brief Dispose an agent handle and cancel its active runs while retaining durable history.
 * @param _ Unused RPC context.
 * @param req Authenticated SDK request.
 * @return Acknowledgement, including for an already absent agent.
 * @return Always nil.
 */
func (s *bridgeServices) CloseAgent(_ context.Context, req *connect.Request[sdkv1.CloseAgentRequest]) (*connect.Response[sdkv1.CloseAgentResponse], error) {
	s.engine.CloseAgent(req.Msg.GetAgentId())
	return connect.NewResponse(&sdkv1.CloseAgentResponse{}), nil
}

/**
 * @brief Cancel an active or retained run idempotently.
 * @param _ Unused RPC context.
 * @param req Authenticated SDK request.
 * @return SDK response, or nil when the run is unknown.
 * @return NotFound for an unknown run; otherwise nil.
 */
func (s *bridgeServices) CancelRun(_ context.Context, req *connect.Request[sdkv1.CancelRunRequest]) (*connect.Response[sdkv1.CancelRunResponse], error) {
	if !s.engine.CancelRun(req.Msg.GetRunId()) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run was not found"))
	}
	return connect.NewResponse(&sdkv1.CancelRunResponse{}), nil
}

/**
 * @brief Read a detached lifecycle snapshot.
 * @param _ Unused RPC context.
 * @param req Authenticated SDK request.
 * @return SDK response, or nil when the run is unknown.
 * @return NotFound for an unknown run; otherwise nil.
 */
func (s *bridgeServices) GetRun(_ context.Context, req *connect.Request[sdkv1.GetRunRequest]) (*connect.Response[sdkv1.GetRunResponse], error) {
	snapshot := s.engine.GetRun(req.Msg.GetRunId())
	if snapshot == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run was not found"))
	}
	return connect.NewResponse(&sdkv1.GetRunResponse{Run: snapshot}), nil
}
