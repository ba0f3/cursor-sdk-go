package cursorbridgeruntime

import (
	"context"
	"errors"
	"time"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
)

/**
 * @brief Pinned SDK idle transport interval; empty frames never become agent history.
 */
const runStreamKeepaliveIdle = 15 * time.Second

/**
 * @brief One real stream message awaiting serialized transport acknowledgement.
 */
type runStreamDelivery struct {
	message *sdkv1.RunStreamMessage ///< Producer-owned immutable SDK message.
	result  chan error              ///< Buffered acknowledgement prevents producer cancellation from blocking the writer.
}

/**
 * @brief Serialize real messages and empty idle frames while bounding and joining the producer's lifetime.
 * @param ctx RPC lifetime, also bounding underlying transport writes.
 * @param idle Private positive idle interval, normally the pinned SDK default.
 * @param source Run or observer producer; it must honor the supplied context and emit failures.
 * @param send Synchronous transport writer that honors RPC cancellation; called only by this function.
 * @return Original source, transport or cancellation error; nil after successful producer completion.
 * @note The caller is the only transport writer. Real emits wait for delivery acknowledgement, and cancellation
 * always joins the source goroutine. Idle frames bypass the producer, contain no envelope or offset, and cannot
 * enter its journal. Canceling an ObserveRun producer stops only that observer, never its underlying generation.
 */
func withRunStreamKeepalive(ctx context.Context, idle time.Duration, source func(context.Context, func(*sdkv1.RunStreamMessage) error) error, send func(*sdkv1.RunStreamMessage) error) error {
	if idle <= 0 {
		return errors.New("stream keepalive interval must be positive")
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	deliveries := make(chan runStreamDelivery)
	finished := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		finished <- source(runCtx, func(message *sdkv1.RunStreamMessage) error {
			delivery := runStreamDelivery{message: message, result: make(chan error, 1)}
			select {
			case <-runCtx.Done():
				return context.Cause(runCtx)
			case deliveries <- delivery:
			}
			select {
			case <-runCtx.Done():
				return context.Cause(runCtx)
			case err := <-delivery.result:
				return err
			}
		})
	}()
	defer func() { cancel(context.Canceled); <-joined }()
	timer := time.NewTimer(idle)
	defer timer.Stop()
	write := func(message *sdkv1.RunStreamMessage) error {
		if runCtx.Err() != nil {
			return context.Cause(runCtx)
		}
		if err := send(message); err != nil {
			cancel(err)
			return err
		}
		timer.Reset(idle)
		return nil
	}
	for {
		// A queued real message or completed producer wins over an already ready
		// timer, so an idle tick does not insert a frame ahead of actual traffic.
		select {
		case err := <-finished:
			return err
		default:
		}
		select {
		case delivery := <-deliveries:
			err := write(delivery.message)
			delivery.result <- err
			if err != nil {
				return err
			}
			continue
		default:
		}
		select {
		case err := <-finished:
			return err
		case <-runCtx.Done():
			return context.Cause(runCtx)
		case delivery := <-deliveries:
			err := write(delivery.message)
			delivery.result <- err
			if err != nil {
				return err
			}
		case <-timer.C:
			select {
			case err := <-finished:
				return err
			default:
			}
			select {
			case delivery := <-deliveries:
				err := write(delivery.message)
				delivery.result <- err
				if err != nil {
					return err
				}
			default:
				if err := write(&sdkv1.RunStreamMessage{}); err != nil {
					return err
				}
			}
		}
	}
}
