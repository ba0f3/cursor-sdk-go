package cursorapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
)

const (
	apiBaseURL        = "https://api2.cursor.sh"
	clientVersion     = "2026.10.01-e373342"
	clientType        = "cli"
	ghostMode         = "true"
	requestTimeout    = 30 * time.Second
	heartbeatInterval = 5 * time.Second
	exchangeKeyPath   = "/auth/exchange_user_api_key"
	serverConfigPath  = "/aiserver.v1.ServerConfigService/GetServerConfig"
	usableModelsPath  = "/agent.v1.AgentService/GetUsableModels"
	runPath           = "/agent.v1.AgentService/Run"
)

/**
 * @brief Cursor control-plane and agent-service HTTP/Connect client for the native bridge.
 */
type Client struct {
	HTTP         *http.Client
	APIBaseURL   string
	CloudBaseURL string ///< Public account API origin; empty uses https://api.cursor.com.
}

/**
 * @brief Create a production Cursor API client with default timeouts.
 */
func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: requestTimeout}
	}
	return &Client{HTTP: httpClient, APIBaseURL: apiBaseURL}
}

/**
 * @brief Indicates Cursor explicitly rejected a credential.
 */
var ErrCredentialRejected = errors.New("cursor rejected the API key")

type tokenPair struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

type agentURLs struct {
	Agent  string
	AgentN string
}

var urlCache sync.Map // map[string]agentURLs

func (c *Client) exchangeAPIKey(ctx context.Context, apiKey string) (string, error) {
	body, status, err := c.do(ctx, http.MethodPost, c.APIBaseURL+exchangeKeyPath, apiKey, []byte("{}"))
	if err != nil {
		return "", err
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return "", ErrCredentialRejected
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("cursor API key exchange returned HTTP %d", status)
	}
	var pair tokenPair
	if err := json.Unmarshal(body, &pair); err != nil {
		return "", fmt.Errorf("decode cursor API key exchange response: %w", err)
	}
	accessToken := strings.TrimSpace(pair.AccessToken)
	if accessToken == "" {
		return "", fmt.Errorf("cursor API key exchange response is incomplete")
	}
	return accessToken, nil
}

func (c *Client) do(ctx context.Context, method, url, apiKey string, payload []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	if apiKey != "" {
		req.Header.Set("authorization", "Bearer "+apiKey)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cursor-client-type", clientType)
	req.Header.Set("x-cursor-client-version", "cli-"+clientVersion)
	req.Header.Set("x-ghost-mode", ghostMode)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return body, resp.StatusCode, err
}

/**
 * @brief Exchange a user API key and resolve shard-specific agent Run origins.
 */
func (c *Client) ResolveRunEndpoint(ctx context.Context, apiKey string) (string, string, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "", "", fmt.Errorf("cursor API key is required")
	}
	accessToken, err := c.exchangeAPIKey(ctx, apiKey)
	if err != nil {
		return "", "", err
	}
	urls, err := c.cachedAgentURLs(ctx, accessToken)
	if err != nil {
		return "", "", err
	}
	return accessToken, strings.TrimRight(urls.Agent, "/") + runPath, nil
}

func (c *Client) cachedAgentURLs(ctx context.Context, accessToken string) (agentURLs, error) {
	if cached, ok := urlCache.Load(accessToken); ok {
		return cached.(agentURLs), nil
	}
	urls, err := c.fetchAgentURLs(ctx, accessToken)
	if err != nil {
		return agentURLs{}, err
	}
	urlCache.Store(accessToken, urls)
	return urls, nil
}

func (c *Client) fetchAgentURLs(ctx context.Context, accessToken string) (agentURLs, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.APIBaseURL, "/")+serverConfigPath, bytes.NewReader(nil))
	if err != nil {
		return agentURLs{}, err
	}
	setConnectHeaders(req, accessToken)
	req.Header.Set("content-type", "application/proto")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return agentURLs{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return agentURLs{}, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return agentURLs{}, fmt.Errorf("cursor server config returned HTTP %d: %w", resp.StatusCode, ErrCredentialRejected)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return agentURLs{}, fmt.Errorf("cursor server config returned HTTP %d", resp.StatusCode)
	}
	return parseAgentURLs(body)
}

func parseAgentURLs(body []byte) (agentURLs, error) {
	fields, err := agentwire.DecodeFieldsExported(body)
	if err != nil {
		return agentURLs{}, err
	}
	cfg, ok := agentwire.FindFieldExported(fields, 27)
	if !ok || len(cfg.Bytes) == 0 {
		return agentURLs{}, fmt.Errorf("cursor server config did not include agent URLs")
	}
	inner, err := agentwire.DecodeFieldsExported(cfg.Bytes)
	if err != nil {
		return agentURLs{}, err
	}
	a1, _ := agentwire.FindFieldExported(inner, 1)
	a2, _ := agentwire.FindFieldExported(inner, 2)
	if len(a1.Bytes) == 0 || len(a2.Bytes) == 0 {
		return agentURLs{}, fmt.Errorf("cursor server config included incomplete agent URLs")
	}
	return agentURLs{Agent: string(a1.Bytes), AgentN: string(a2.Bytes)}, nil
}

/**
 * @brief List model IDs exposed to SDK consumers for one API key.
 */
func (c *Client) ListModelIDs(ctx context.Context, apiKey string) ([]string, error) {
	accessToken, endpoint, err := c.ResolveRunEndpoint(ctx, apiKey)
	if err != nil {
		return nil, err
	}
	base := endpoint[:strings.LastIndex(endpoint, "/agent.v1.")]
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+usableModelsPath, bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	setConnectHeaders(req, accessToken)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GetUsableModels returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	return parseModelIDs(body), nil
}

func parseModelIDs(body []byte) []string {
	fields, err := agentwire.DecodeFieldsExported(body)
	if err != nil {
		return nil
	}
	var models []string
	for _, f := range fields {
		if f.Number != 1 || len(f.Bytes) == 0 {
			continue
		}
		inner, err := agentwire.DecodeFieldsExported(f.Bytes)
		if err != nil {
			continue
		}
		idField, ok := agentwire.FindFieldExported(inner, 1)
		if ok && len(idField.Bytes) > 0 {
			models = append(models, string(idField.Bytes))
		}
	}
	return models
}

/**
 * @brief Open a bidirectional AgentService Run with caller-owned cancellation.
 * @param ctx Run lifetime, independent of the control-plane timeout.
 * @param runURL Resolved Cursor agent endpoint.
 * @param accessToken Exchanged Cursor access token.
 * @param firstFrame Unframed AgentClientMessage run request.
 * @param policy Validated native tool restrictions; nil allowlist uses SDK default, empty disables tools.
 * @return Response stream; caller must close it, or nil on failure.
 * @return Request writer for control and tool replies; caller must close it, or nil on failure.
 * @return Transport or rejected HTTP response error; nil on success.
 * @note The request body stays open until the turn finishes; no fixed 30-second stream limit applies.
 */
func (c *Client) OpenRunStream(ctx context.Context, runURL, accessToken string, firstFrame []byte, policy agentwire.ToolPolicy) (io.ReadCloser, *io.PipeWriter, error) {
	reader, writer := io.Pipe()
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	firstWritten := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		select {
		case <-firstWritten:
		case <-heartbeatCtx.Done():
			return
		}
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		frame := agentwire.EncodeClientHeartbeat()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if _, err := writer.Write(frame); err != nil {
					return
				}
			}
		}
	}()
	stopWatching := context.AfterFunc(ctx, func() {
		_ = reader.CloseWithError(context.Cause(ctx))
		_ = writer.CloseWithError(context.Cause(ctx))
	})
	transferred := false
	defer func() {
		if !transferred {
			stopHeartbeat()
			stopWatching()
			_ = reader.Close()
			_ = writer.Close()
			<-heartbeatDone
		}
	}()
	go func() {
		defer close(firstWritten)
		_, err := writer.Write(agentwire.EncodeConnectDataFrame(firstFrame))
		if err != nil {
			_ = writer.CloseWithError(err)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, runURL, reader)
	if err != nil {
		_ = writer.CloseWithError(err)
		return nil, nil, err
	}
	setConnectHeaders(req, accessToken)
	if policy.Allowed != nil {
		req.Header.Set("x-cursor-agent-allowed-tools", strings.Join(policy.Allowed, ","))
	}
	if len(policy.Excluded) > 0 {
		req.Header.Set("x-cursor-agent-exclude-tools", strings.Join(policy.Excluded, ","))
	}
	req.Header.Set("connect-content-encoding", "identity")
	streamClient := *c.HTTP
	streamClient.Timeout = 0
	resp, err := streamClient.Do(req)
	if err != nil {
		_ = writer.CloseWithError(err)
		return nil, nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = writer.Close()
		defer func() { _ = resp.Body.Close() }()
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, nil, fmt.Errorf("agent Run returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	transferred = true
	return &runStreamBody{ReadCloser: resp.Body, writer: writer, stopWatching: stopWatching, stopHeartbeat: stopHeartbeat, heartbeatDone: heartbeatDone}, writer, nil
}

/**
 * @brief Own the cancellation watcher and both directions of a Cursor stream.
 */
type runStreamBody struct {
	io.ReadCloser                    ///< Upstream response body.
	writer        *io.PipeWriter     ///< Request body kept open for tool and control replies.
	stopWatching  func() bool        ///< Detach the context watcher after normal completion.
	stopHeartbeat context.CancelFunc ///< Stop the periodic client heartbeat.
	heartbeatDone <-chan struct{}    ///< Heartbeat sender has exited.
}

/**
 * @brief Release the streaming request and its cancellation watcher.
 * @return Upstream response close error, or nil on success.
 */
func (b *runStreamBody) Close() error {
	b.stopHeartbeat()
	b.stopWatching()
	_ = b.writer.Close()
	err := b.ReadCloser.Close()
	<-b.heartbeatDone
	return err
}

func setConnectHeaders(req *http.Request, accessToken string) {
	req.Header.Set("authorization", "Bearer "+accessToken)
	req.Header.Set("connect-protocol-version", "1")
	req.Header.Set("content-type", "application/connect+proto")
	req.Header.Set("user-agent", "connect-es/1.6.1")
	req.Header.Set("x-cursor-client-type", clientType)
	req.Header.Set("x-cursor-client-version", "cli-"+clientVersion)
	req.Header.Set("x-ghost-mode", ghostMode)
}

// Sleep is used by tests to shorten waits.
var Sleep = time.Sleep
