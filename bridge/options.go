package cursorbridgeruntime

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

/**
 * @brief Validate storage topology before publishing a native bridge server.
 * @param opts Requested startup options; the legacy sqlite selector denotes bridge-owned storage.
 * @return Error for malformed configuration or a backend this bridge cannot provide.
 */
func validateStoreOptions(opts Options) error {
	if strings.TrimSpace(opts.StoreCallbackURL) != "" || strings.TrimSpace(opts.StoreCallbackToken) != "" {
		return errors.New("external store callbacks are unavailable in the native client-tool bridge")
	}
	if strings.TrimSpace(opts.LocalStoreJSON) == "" {
		return nil
	}
	var config map[string]json.RawMessage
	if err := json.Unmarshal([]byte(opts.LocalStoreJSON), &config); err != nil || config == nil {
		return errors.New("invalid native bridge local store configuration")
	}
	for key := range config {
		if key != "type" {
			return errors.New("custom local store configuration is unavailable in the native client-tool bridge")
		}
	}
	var kind string
	if value, ok := config["type"]; ok {
		if err := json.Unmarshal(value, &kind); err != nil {
			return errors.New("invalid native bridge local store type")
		}
	}
	if kind != "" && kind != "sqlite" {
		return errors.New("custom local store backends are unavailable in the native client-tool bridge")
	}
	return nil
}

/**
 * @brief CLI and environment configuration for a native Cursor SDK bridge process.
 */
type Options struct {
	Host               string ///< Loopback bind host; empty defaults to 127.0.0.1.
	Port               int    ///< TCP port; zero selects an ephemeral port.
	Workspace          string
	StateRoot          string
	ToolCallbackURL    string
	ToolCallbackToken  string
	StoreCallbackURL   string
	StoreCallbackToken string
	LocalStoreJSON     string
	Verbose            bool
	Stderr             io.Writer ///< Discovery and diagnostics sink; defaults to os.Stderr.
}

/**
 * @brief Populate options from CLI flags and Cursor SDK bridge environment variables.
 * @return Parsed options with defaults applied.
 */
func OptionsFromEnv() Options {
	opts := Options{
		Host:               strings.TrimSpace(os.Getenv("CURSOR_SDK_BRIDGE_HOST")),
		Workspace:          strings.TrimSpace(os.Getenv("CURSOR_SDK_BRIDGE_WORKSPACE")),
		StateRoot:          strings.TrimSpace(os.Getenv("CURSOR_SDK_BRIDGE_STATE_ROOT")),
		ToolCallbackURL:    strings.TrimSpace(os.Getenv("CURSOR_SDK_TOOL_CALLBACK_URL")),
		ToolCallbackToken:  strings.TrimSpace(os.Getenv("CURSOR_SDK_TOOL_CALLBACK_AUTH_TOKEN")),
		StoreCallbackURL:   strings.TrimSpace(os.Getenv("CURSOR_SDK_STORE_CALLBACK_URL")),
		StoreCallbackToken: strings.TrimSpace(os.Getenv("CURSOR_SDK_STORE_CALLBACK_AUTH_TOKEN")),
		LocalStoreJSON:     strings.TrimSpace(os.Getenv("CURSOR_SDK_LOCAL_STORE")),
		Stderr:             os.Stderr,
	}
	if v := strings.TrimSpace(os.Getenv("CURSOR_SDK_BRIDGE_PORT")); v != "" {
		if port, err := parsePort(v); err == nil {
			opts.Port = port
		}
	}
	if strings.TrimSpace(os.Getenv("CURSOR_SDK_BRIDGE_LOG")) == "1" {
		opts.Verbose = true
	}
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	if opts.LocalStoreJSON == "" {
		opts.LocalStoreJSON = `{"type":"sqlite"}`
	}
	return opts
}

func parsePort(raw string) (int, error) {
	port, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || port < 0 || port > 65535 {
		return 0, strconv.ErrSyntax
	}
	return port, nil
}
