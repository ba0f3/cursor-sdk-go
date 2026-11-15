package localrun

import (
	"errors"

	"connectrpc.com/connect"
)

/**
 * @brief Acknowledge reload of a known stateless client-tool handle without changing its conversation or runs.
 * @param id Registered SDK agent identifier.
 * @return NotFound for an unknown agent, FailedPrecondition for a closed engine, otherwise nil.
 * @note The pinned SDK delegates reload to an optional cached workspace executor. This bridge has no such executor:
 * custom tools remain client-owned, and each Send rebuilds native declarations and context. Reload therefore
 * performs no host settings, plugin or rules discovery and preserves all existing agent and run state.
 */
func (e *Engine) ReloadAgent(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor runtime is closed"))
	}
	if e.agents[id] == nil {
		return connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	return nil
}
