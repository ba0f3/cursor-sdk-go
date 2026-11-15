package cursorbridgeruntime

import (
	"context"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Acknowledge reload of this bridge's stateless client-tool agent handle.
 * @param _ Unused RPC context; reload performs no external work.
 * @param req Authenticated SDK agent identity.
 * @return Empty acknowledgement after engine and agent validation.
 * @return NotFound for an unknown agent or FailedPrecondition for a closed engine.
 * @note This bridge has no cached host workspace executor or host plugin/rules loader to refresh.
 */
func (s *bridgeServices) ReloadAgent(_ context.Context, req *connect.Request[sdkv1.ReloadAgentRequest]) (*connect.Response[sdkv1.ReloadAgentResponse], error) {
	if err := s.engine.ReloadAgent(req.Msg.GetAgentId()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&sdkv1.ReloadAgentResponse{}), nil
}
