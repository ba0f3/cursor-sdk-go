package localrun

import (
	"errors"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

/**
 * @brief Validate and detach a complete model selection without reducing it to a fast flag.
 * @param selected Explicit override, or nil to inherit the current model.
 * @param current Existing model selection, or nil for the SDK default.
 * @return Owned model selection, or nil for invalid configuration.
 * @return InvalidArgument for an empty model or parameter identity.
 */
func resolveModelSelection(selected, current *sdkv1.ModelSelection) (*sdkv1.ModelSelection, error) {
	if selected == nil {
		selected = current
	}
	if selected == nil {
		return &sdkv1.ModelSelection{Id: "default"}, nil
	}
	if strings.TrimSpace(selected.GetId()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model.id is required"))
	}
	for _, parameter := range selected.GetParams() {
		if strings.TrimSpace(parameter.GetId()) == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model parameter id is required"))
		}
	}
	return proto.Clone(selected).(*sdkv1.ModelSelection), nil
}

/**
 * @brief Map the SDK's explicit modes to native enum values instead of forcing every turn into agent mode.
 * @param mode SDK mode, where unspecified preserves the native conversation's current mode.
 * @return Native mode enum value.
 * @return InvalidArgument for unknown SDK mode values.
 */
func nativeAgentMode(mode sdkv1.AgentModeOption) (uint32, error) {
	switch mode {
	case sdkv1.AgentModeOption_AGENT_MODE_OPTION_UNSPECIFIED:
		return 0, nil
	case sdkv1.AgentModeOption_AGENT_MODE_OPTION_AGENT:
		return 1, nil
	case sdkv1.AgentModeOption_AGENT_MODE_OPTION_PLAN:
		return 3, nil
	default:
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("unsupported agent mode"))
	}
}

/**
 * @brief Convert detached SDK model parameters at the native protocol boundary.
 * @param model Validated immutable model selection for this run.
 * @return Exact native parameter sequence, preserving IDs, values and order.
 */
func nativeModelParameters(model *sdkv1.ModelSelection) []agentwire.ModelParameter {
	parameters := make([]agentwire.ModelParameter, 0, len(model.GetParams()))
	for _, parameter := range model.GetParams() {
		parameters = append(parameters, agentwire.ModelParameter{ID: parameter.GetId(), Value: parameter.GetValue()})
	}
	return parameters
}
