package cursorapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"

	"google.golang.org/protobuf/encoding/protojson"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Read actual backend usage with the public API-to-SDK field mapping from the pinned reference.
 * @param ctx Request cancellation and deadline.
 * @param apiKey Credential retained only in memory.
 * @param agentID Native conversation group identifier sent to Cursor.
 * @param runID Optional backend usage UUID, never a client-only run identifier.
 * @return Backend token counters and optional cost, or nil on failure.
 * @return Authentication, transport, HTTP or malformed response error.
 */
func (c *Client) AgentUsage(ctx context.Context, apiKey, agentID, runID string) (*sdkv1.AgentUsage, error) {
	path := "/v1/agents/" + url.PathEscape(agentID) + "/usage"
	if runID != "" {
		path += "?" + url.Values{"runId": {runID}}.Encode()
	}
	data, err := c.readCloudResponse(ctx, apiKey, path)
	if err != nil {
		return nil, err
	}
	var response struct {
		TotalUsage json.RawMessage `json:"totalUsage"`
		Cost       json.RawMessage `json:"cost"`
		Runs       []struct {
			ID    string          `json:"id"`
			Usage json.RawMessage `json:"usage"`
			Cost  json.RawMessage `json:"cost"`
		} `json:"runs"`
	}
	invalid := errors.New("invalid cursor public API usage response")
	if err := json.Unmarshal(data, &response); err != nil || len(response.TotalUsage) == 0 || string(response.TotalUsage) == "null" || response.Runs == nil {
		return nil, invalid
	}
	usage := &sdkv1.AgentUsage{Usage: &sdkv1.TokenUsage{}}
	decoder := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := decoder.Unmarshal(response.TotalUsage, usage.Usage); err != nil {
		return nil, invalid
	}
	if len(response.Cost) > 0 && string(response.Cost) != "null" {
		usage.Cost = &sdkv1.UsageCost{}
		if err := decoder.Unmarshal(response.Cost, usage.Cost); err != nil {
			return nil, invalid
		}
	}
	for _, run := range response.Runs {
		entry := &sdkv1.RunUsage{RunId: run.ID, Usage: &sdkv1.TokenUsage{}}
		if run.ID == "" || len(run.Usage) == 0 || string(run.Usage) == "null" {
			return nil, invalid
		}
		if err := decoder.Unmarshal(run.Usage, entry.Usage); err != nil {
			return nil, invalid
		}
		if len(run.Cost) > 0 && string(run.Cost) != "null" {
			entry.Cost = &sdkv1.UsageCost{}
			if err := decoder.Unmarshal(run.Cost, entry.Cost); err != nil {
				return nil, invalid
			}
		}
		usage.Runs = append(usage.Runs, entry)
	}
	return usage, nil
}
