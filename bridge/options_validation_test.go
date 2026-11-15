package cursorbridgeruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

/**
 * @brief Refuse unsupported storage configuration before publishing a usable server.
 * @param t Private workspace without any external requests.
 */
func TestNativeBridgeRejectsUnavailableStorage(t *testing.T) {
	for name, options := range map[string]Options{
		"callback":       {StoreCallbackURL: "http://127.0.0.1:1", StoreCallbackToken: "fake"},
		"custom":         {LocalStoreJSON: `{"type":"custom"}`},
		"jsonl":          {LocalStoreJSON: `{"type":"jsonl","rootDir":"/unused"}`},
		"malformed":      {LocalStoreJSON: `{"type":`},
		"extra_document": {LocalStoreJSON: `{"type":"sqlite"}{}`},
	} {
		t.Run(name, func(t *testing.T) {
			options.Workspace, options.Stderr = t.TempDir(), io.Discard
			server, err := Start(context.Background(), options)
			if server != nil {
				_ = server.Close(context.Background())
			}
			if err == nil {
				t.Fatal("unsupported storage silently accepted")
			}
		})
	}
}

/**
 * @brief Apply initial callback configuration and publish only capacity values actually enforced.
 * @param t Private discovery output and fake callback credentials.
 */
func TestNativeBridgeInitialCallbackAndDiscovery(t *testing.T) {
	var output bytes.Buffer
	server, err := Start(context.Background(), Options{Workspace: t.TempDir(), Stderr: &output, ToolCallbackURL: "http://127.0.0.1:1", ToolCallbackToken: "fake-callback", LocalStoreJSON: `{"type":"custom","type":"sqlite"}`})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	var discovery map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(output.String(), "cursor-sdk-bridge ready "))), &discovery); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"maxConcurrentAgents", "maxMessageBytes"} {
		if _, advertised := discovery[field]; advertised {
			t.Fatalf("unenforced SDK capacity advertised: %s", field)
		}
	}
}
