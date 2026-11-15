package localrun

import (
	"errors"
	"strings"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"

	"connectrpc.com/connect"
)

/**
 * @brief Reject explicit host services that this client-tool bridge cannot provide.
 * @param options Immutable requested configuration.
 * @param resumedID Existing identity when resuming, or empty when creating.
 * @return Typed capability error before modifying state or executing callbacks.
 */
func validateAgentCapabilities(options *sdkv1.AgentOptions, resumedID string) error {
	var unavailable string
	switch {
	case options.GetCloud() != nil:
		unavailable = "cloud agents"
	case options.GetAgentId() != "" && options.GetAgentId() != resumedID:
		unavailable = "caller-selected agent identities"
	case len(options.GetMcpServers()) > 0:
		unavailable = "external MCP servers"
	case len(options.GetAgents()) > 0:
		unavailable = "host subagent definitions"
	default:
		local := options.GetLocal()
		switch {
		case len(local.GetSettingSources()) > 0:
			unavailable = "host setting sources"
		case local.GetSandboxOptions() != nil:
			unavailable = "host sandbox configuration"
		case local.GetAutoReview():
			unavailable = "host automatic tool review"
		case len(local.GetDirs()) > 0 || len(local.GetCwd()) > 1:
			unavailable = "host multi-root workspaces"
		case local.GetStore() != nil:
			store := local.GetStore()
			if kind := strings.TrimSpace(store.GetType()); kind != "" && kind != "sqlite" || store.GetRootDir() != "" {
				unavailable = "custom local store backends"
			}
		}
	}
	if unavailable != "" {
		return connect.NewError(connect.CodeUnimplemented, errors.New(unavailable+" are unavailable in the native client-tool bridge"))
	}
	return nil
}
