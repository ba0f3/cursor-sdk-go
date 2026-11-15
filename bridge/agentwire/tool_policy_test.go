package agentwire

import (
	"reflect"
	"testing"
)

/**
 * @brief Match official public aliases, capability groups and explicit empty allowlists.
 * @param t Synthetic tool names without credentials or executable callbacks.
 */
func TestResolveToolNames(t *testing.T) {
	got, err := ResolveToolNames([]string{"shell", "shellToolCall", "read_tool_call", "webFetch", "mcp"})
	want := []string{"shell_tool_call", "write_shell_stdin_tool_call", "read_tool_call", "web_fetch_tool_call", "mcp_tool_call", "get_mcp_tools_tool_call", "list_mcp_resources_tool_call", "read_mcp_resource_tool_call", "mcp_auth_tool_call"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved=%v error=%v", got, err)
	}
	got, err = ResolveToolNames(nil)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatal("explicit empty tool selection lost its identity")
	}
	if _, err := ResolveToolNames([]string{"unknown"}); err == nil {
		t.Fatal("unsupported tool name was silently ignored")
	}
}
