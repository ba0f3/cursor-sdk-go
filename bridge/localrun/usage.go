package localrun

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
)

/**
 * @brief Query actual provider usage using a private credential snapshot and the native backend identity.
 * @param ctx Request cancellation and deadline, without affecting active generation.
 * @param id Public SDK agent identifier.
 * @param runID Optional backend usage UUID.
 * @return Actual provider counters and optional cost, or nil on failure.
 * @return NotFound, InvalidArgument, Unauthenticated or upstream transport error.
 */
func (e *Engine) AgentUsage(ctx context.Context, id, runID string) (*sdkv1.AgentUsage, error) {
	e.mu.Lock()
	record := e.agents[id]
	if record == nil {
		e.mu.Unlock()
		return nil, connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	if strings.HasPrefix(runID, "run-") || e.snapshots[runID] != nil {
		e.mu.Unlock()
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("local usage requires a backend usage UUID from getUsage().runs[].runId, not a client-only run identifier"))
	}
	key, nativeID := record.apiKey, record.conversationID
	e.mu.Unlock()
	if key == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, cursorapi.ErrCredentialRejected)
	}
	return e.API.AgentUsage(ctx, key, nativeID, runID)
}
