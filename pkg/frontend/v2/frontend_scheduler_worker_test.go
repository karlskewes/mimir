// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestSchedulerWorkerForSend builds a worker registered under address, without starting its
// goroutines. Nothing receives from its targetedRequestsCh, which is the state a scheduler is in
// while all of its workers sit in backoff after a stream failure.
func newTestSchedulerWorkerForSend(address string) (*frontendSchedulerWorkers, *frontendSchedulerWorker) {
	w := &frontendSchedulerWorker{
		schedulerAddr:      address,
		targetedRequestsCh: make(chan *frontendRequest),
	}
	w.ctx, w.cancel = context.WithCancelCause(context.Background())

	return &frontendSchedulerWorkers{
		workers: map[string]*frontendSchedulerWorker{address: w},
	}, w
}

func TestFrontendSchedulerWorkers_SendRequestToScheduler(t *testing.T) {
	const address = "scheduler-1"

	t.Run("hands the request over when a worker is receiving", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			workers, w := newTestSchedulerWorkerForSend(address)
			defer w.cancel(errFrontendSchedulerWorkerStopping)

			received := make(chan *frontendRequest, 1)
			go func() {
				received <- <-w.targetedRequestsCh
			}()

			freq := &frontendRequest{}
			sent, err := workers.sendRequestToScheduler(context.Background(), address, freq)
			require.NoError(t, err)
			require.True(t, sent)
			require.Same(t, freq, <-received)
		})
	})

	t.Run("reports the scheduler as unavailable when it is not in use", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			workers, w := newTestSchedulerWorkerForSend(address)
			defer w.cancel(errFrontendSchedulerWorkerStopping)

			start := time.Now()
			sent, err := workers.sendRequestToScheduler(context.Background(), "other-scheduler", &frontendRequest{})
			require.NoError(t, err)
			require.False(t, sent)
			require.Zero(t, time.Since(start), "should not wait for an unknown scheduler")
		})
	})

	t.Run("gives up immediately when the scheduler has been removed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			workers, w := newTestSchedulerWorkerForSend(address)

			// stop() cancels this context before waiting for the worker goroutines to exit, so after
			// removal nothing will ever drain targetedRequestsCh.
			w.cancel(errFrontendSchedulerWorkerStopping)

			// A request context with no deadline: giving up must not depend on the caller's timeout.
			start := time.Now()
			sent, err := workers.sendRequestToScheduler(context.Background(), address, &frontendRequest{})
			require.NoError(t, err)
			require.False(t, sent)
			require.Zero(t, time.Since(start), "should not wait for a removed scheduler")
		})
	})

	// Known limitation: a scheduler whose workers are all in backoff after a stream failure is still
	// alive, so its worker context stays open and there is nothing to distinguish it from a healthy
	// but momentarily busy scheduler. The send is bounded only by the request context.
	t.Run("stalls until the request context is done when the scheduler is alive but not receiving", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			workers, w := newTestSchedulerWorkerForSend(address)
			defer w.cancel(errFrontendSchedulerWorkerStopping)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			start := time.Now()
			sent, err := workers.sendRequestToScheduler(ctx, address, &frontendRequest{})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.False(t, sent)
			require.Equal(t, 2*time.Second, time.Since(start), "should block for the whole request deadline")
		})
	})
}
