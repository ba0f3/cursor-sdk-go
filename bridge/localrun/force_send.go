package localrun

import (
	"context"
	"errors"

	"connectrpc.com/connect"
)

/**
 * @brief Cancel the initially active run and await this agent's captured physical cleanup before a forced Send may register.
 * @param ctx Replacement caller lifetime, bounding cleanup wait without altering other agents.
 * @param agentID Registered local SDK agent identity.
 * @param force Whether replacement of this agent's existing run was explicitly requested.
 * @return Agent lifecycle, inconsistent completion ownership or caller cancellation error; otherwise nil.
 * @note Cancellation and waiting occur outside the mutex. A competing run started after the captured target
 * is left to Send's registration guard, rather than being canceled in a retry loop. Nonforced sends do no work.
 */
func (e *Engine) prepareForcedSend(ctx context.Context, agentID string, force bool) error {
	if !force {
		return nil
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("cursor runtime is closed"))
	}
	rec := e.agents[agentID]
	if rec == nil {
		e.mu.Unlock()
		return connect.NewError(connect.CodeNotFound, errors.New("agent was not found"))
	}
	if rec.detached || rec.apiKey == "" {
		e.mu.Unlock()
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("agent must be resumed with a credential before sending"))
	}
	active := make(map[string]struct{})
	var cancels []context.CancelFunc
	doneChannels := make([]<-chan struct{}, 0, 1)
	for run, owner := range e.runAgents {
		if owner != agentID {
			continue
		}
		done, cancel := e.runDone[run], e.activeRuns[run]
		if done == nil || cancel == nil {
			e.mu.Unlock()
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("agent run cleanup owner is unavailable; forced send is refused"))
		}
		active[run] = struct{}{}
		cancels = append(cancels, cancel)
		doneChannels = append(doneChannels, done)
	}
	// Logical completion removes active ownership before terminal SDK delivery
	// finishes. Retained snapshots still associate that physical cleanup with its agent.
	for run, done := range e.runDone {
		_, isActive := active[run]
		if isActive || e.snapshots[run].GetAgentId() != agentID {
			continue
		}
		if done == nil {
			e.mu.Unlock()
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("agent run cleanup owner is unavailable; forced send is refused"))
		}
		doneChannels = append(doneChannels, done)
	}
	e.mu.Unlock()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	for _, cancel := range cancels {
		cancel()
	}
	for _, done := range doneChannels {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-done:
		}
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}
