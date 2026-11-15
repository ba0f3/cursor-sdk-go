package agentwire

import (
	"errors"
	"strings"
)

/**
 * @brief Validated upstream tool restrictions, retaining nil versus explicitly empty allowlists.
 */
type ToolPolicy struct {
	Allowed  []string `json:"allowed"`  ///< Nil means SDK default; empty means no native tools.
	Excluded []string `json:"excluded"` ///< Explicit native tools to exclude.
}

/**
 * @brief Resolve public, protobuf and camel-case aliases exactly as the official local SDK.
 * @param names Tool names, with capability groups expanded and duplicate proto names removed.
 * @return Non-nil ordered list, including for an explicitly empty selection.
 * @return Error for unknown names; invalid input is never reflected into diagnostic output.
 */
func ResolveToolNames(names []string) ([]string, error) {
	aliases := make(map[string][]string)
	for name := range strings.FieldsSeq(nativeToolSchema) {
		parts := strings.Split(name, "_")
		camel := parts[0]
		for _, part := range parts[1:] {
			camel += strings.ToUpper(part[:1]) + part[1:]
		}
		public := strings.TrimSuffix(camel, "ToolCall")
		aliases[name] = []string{name}
		aliases[camel] = []string{name}
		aliases[public] = []string{name}
	}
	aliases["shell"] = []string{"shell_tool_call", "write_shell_stdin_tool_call"}
	aliases["mcp"] = []string{"mcp_tool_call", "get_mcp_tools_tool_call", "list_mcp_resources_tool_call", "read_mcp_resource_tool_call", "mcp_auth_tool_call"}
	resolved := make([]string, 0)
	seen := make(map[string]bool)
	for _, name := range names {
		mapped, ok := aliases[strings.TrimSpace(name)]
		if !ok {
			return nil, errors.New("unknown Cursor tool name in agent options")
		}
		for _, tool := range mapped {
			if !seen[tool] {
				resolved = append(resolved, tool)
				seen[tool] = true
			}
		}
	}
	return resolved, nil
}
