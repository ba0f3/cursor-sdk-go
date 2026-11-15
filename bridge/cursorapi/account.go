package cursorapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Read the authenticated account using the official SDK public API.
 * @param ctx Request cancellation and deadline.
 * @param apiKey Cursor API key; never included in diagnostics.
 * @return Account details, or nil on failure.
 * @return Authentication, transport, HTTP status or response decoding error.
 */
func (c *Client) Account(ctx context.Context, apiKey string) (*sdkv1.SdkUser, error) {
	user := &sdkv1.SdkUser{}
	if err := c.readCloudJSON(ctx, apiKey, "/v1/me", user); err != nil {
		return nil, err
	}
	return user, nil
}

/**
 * @brief List repositories visible to the authenticated Cursor account.
 * @param ctx Request cancellation and deadline.
 * @param apiKey Cursor API key; never included in diagnostics.
 * @return Visible repositories, or nil on failure.
 * @return Authentication, transport, HTTP status or response decoding error.
 */
func (c *Client) Repositories(ctx context.Context, apiKey string) ([]*sdkv1.SdkRepository, error) {
	response := &sdkv1.ListRepositoriesResponse{}
	if err := c.readCloudJSON(ctx, apiKey, "/v1/repositories", response); err != nil {
		return nil, err
	}
	return response.GetItems(), nil
}

/**
 * @brief Decode a bounded public API response without logging upstream bodies or credentials.
 * @param ctx Request cancellation and deadline.
 * @param apiKey Caller credential, required and sent only as a Bearer header.
 * @param path Fixed SDK endpoint path.
 * @param destination Protobuf response with the same JSON field names as the public API.
 * @return Credential, transport, HTTP, size or JSON validation failure; nil on success.
 */
func (c *Client) readCloudJSON(ctx context.Context, apiKey, path string, destination proto.Message) error {
	data, err := c.readCloudResponse(ctx, apiKey, path)
	if err != nil {
		return err
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, destination); err != nil {
		// Decoder errors may quote response fields containing private account data.
		return errors.New("invalid cursor public API JSON response")
	}
	return nil
}

/**
 * @brief Read a bounded authenticated response from a fixed public API route.
 * @param ctx Request cancellation and deadline.
 * @param apiKey Caller credential, sent only as a Bearer header.
 * @param path SDK endpoint path and encoded query.
 * @return Response bytes, or nil on failure.
 * @return Credential, transport, HTTP or size error without response contents.
 */
func (c *Client) readCloudResponse(ctx context.Context, apiKey, path string) ([]byte, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, ErrCredentialRejected
	}
	origin := strings.TrimRight(c.CloudBaseURL, "/")
	if origin == "" {
		origin = "https://api.cursor.com"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("x-ghost-mode", ghostMode)
	req.Header.Set("x-cursor-client-version", clientVersion)
	req.Header.Set("x-cursor-client-type", "sdk")
	response, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return nil, ErrCredentialRejected
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("cursor public API returned HTTP %d", response.StatusCode)
	}
	const maxResponseBytes = 2 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResponseBytes {
		return nil, errors.New("cursor public API response exceeds size limit")
	}
	return data, nil
}
