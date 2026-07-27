// SPDX-License-Identifier: AGPL-3.0-only

// End-to-end (queueBroker) counterparts to the tree-level investigation tests in
// pkg/queue/tree/queue_component_fairness_investigation_test.go. These drive the
// real dequeueItemForConsumer path (including createOrUpdateTenant on enqueue and
// the workerID -> query-component mapping) synchronously, so the fairness result
// is deterministic and not subject to dispatcher-goroutine scheduling.
//
// They confirm:
//   - with a per-worker cursor (each worker threads its own LastTenantIndex, as
//     scheduler.QuerierLoop does), two store-gateway tenants are served fairly;
//   - if the cursor is instead shared across query components, one store-gateway
//     tenant is starved -- reproducing the hypothesized behaviour on the real path.

package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fairnessTestBroker builds a broker with no shuffle sharding, seeded so that
// nodeOrder=[store-gateway, ingester] (workerID%2==0 -> store-gateway, ==1 ->
// ingester) and tenantIDOrder=[T, X, I]. T and X are store-gateway-only; I is
// ingester-only. Each queue is seeded deep enough that none empties during a run.
func fairnessTestBroker(t *testing.T) *queueBroker {
	t.Helper()
	qb := newQueueBroker(1_000_000, 0)
	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), "c"))

	enqueue := func(tenantID, dim string, n int) {
		for i := range n {
			err := qb.enqueueItemBack(&tenantItem{tenantID: tenantID, queueDimension: dim, item: i}, 0)
			require.NoError(t, err)
		}
	}
	enqueue("T", "store-gateway", 5000)
	enqueue("X", "store-gateway", 5000)
	enqueue("I", "ingester", 5000)
	return qb
}

func dequeueTenant(t *testing.T, qb *queueBroker, workerID, lastTenantIndex int) (tenantID string, idx int) {
	t.Helper()
	item, _, idx, err := qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "c", WorkerID: workerID},
		lastTenantIndex:    TenantIndex{last: lastTenantIndex},
	})
	require.NoError(t, err)
	if item == nil {
		return "", lastTenantIndex
	}
	return item.tenantID, idx
}

// TestInvestigation_BrokerPerWorkerCursorIsFair: worker 0 (store-gateway) and
// worker 1 (ingester) each thread their own cursor. The store-gateway worker
// round-robins T and X evenly despite heavy ingester traffic.
func TestInvestigation_BrokerPerWorkerCursorIsFair(t *testing.T) {
	qb := fairnessTestBroker(t)

	served := map[string]int{}
	last0, last1 := -1, -1
	for range 2000 {
		var tid string
		tid, last0 = dequeueTenant(t, qb, 0, last0) // store-gateway worker
		served["sg/"+tid]++
		tid, last1 = dequeueTenant(t, qb, 1, last1) // ingester worker
		served["ing/"+tid]++
	}
	t.Logf("broker per-worker served=%v", served)
	assert.InDelta(t, served["sg/T"], served["sg/X"], 2,
		"per-worker cursor serves store-gateway tenants fairly")
	assert.Equal(t, 2000, served["ing/I"])
}

// TestInvestigation_BrokerSharedCursorStarves: threading a single cursor across
// both the store-gateway and ingester workers (the hypothesized wiring) starves
// one store-gateway tenant, because ingester dequeues keep parking the shared
// cursor at the ingester tenant's slot.
func TestInvestigation_BrokerSharedCursorStarves(t *testing.T) {
	qb := fairnessTestBroker(t)

	served := map[string]int{}
	last := -1
	for range 2000 {
		var tid string
		tid, last = dequeueTenant(t, qb, 0, last) // store-gateway worker
		served["sg/"+tid]++
		tid, last = dequeueTenant(t, qb, 1, last) // ingester worker, same cursor
		served["ing/"+tid]++
	}
	t.Logf("broker shared-cursor served=%v", served)
	require.Less(t, served["sg/X"], served["sg/T"]/4,
		"shared cursor starves one store-gateway tenant")
}
