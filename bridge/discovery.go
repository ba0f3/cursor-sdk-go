package cursorbridgeruntime

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

type readyPayload struct {
	SchemaVersion   int    `json:"schemaVersion"`
	ServerVersion   string `json:"serverVersion"`
	PID             int    `json:"pid"`
	Transport       string `json:"transport"`
	Protocol        string `json:"protocol"`
	Host            string `json:"host"`
	Port            int    `json:"port"`
	URL             string `json:"url"`
	AuthTokenFile   string `json:"authTokenFile"`
	WorkspaceRef    string `json:"workspaceRef"`
	StateRoot       string `json:"stateRoot"`
	MaxMessageBytes int    `json:"maxMessageBytes,omitempty"`
	MaxConcurrent   int    `json:"maxConcurrentAgents,omitempty"`
}

/**
 * @brief Emit the sdk-bridge discovery line required by cursorauth bridge startup.
 */
func writeReadyLine(w io.Writer, host string, port int, workspace, stateRoot, tokenFile string) error {
	if w == nil {
		w = os.Stderr
	}
	payload := readyPayload{
		SchemaVersion: 1,
		ServerVersion: BridgeVersion,
		PID:           os.Getpid(),
		Transport:     "tcp",
		Protocol:      "connect",
		Host:          host,
		Port:          port,
		URL:           fmt.Sprintf("http://%s", net.JoinHostPort(host, fmt.Sprint(port))),
		AuthTokenFile: tokenFile,
		WorkspaceRef:  workspace,
		StateRoot:     stateRoot,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, "cursor-sdk-bridge ready "+string(data)+"\n")
	return err
}
