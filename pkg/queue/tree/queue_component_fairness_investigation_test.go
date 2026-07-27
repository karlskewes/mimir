// SPDX-License-Identifier: AGPL-3.0-only

// This file collects the reproduction tests written while investigating whether
// the ConsumerWorkerQueuePriorityAlgo (query-component prioritization) combined
// with the shared TenantConsumerQueuingAlgorithm tenant order can starve a
// store-gateway-only tenant when ingester dequeues "cycle fast".
//
// Findings the tests below demonstrate:
//
//  1. The starvation is real IF the tenant cursor (tenantOrderIndex) is shared
//     across query-component subtrees, i.e. advanced by dequeues from every
//     component. Which store-gateway tenant starves depends on its position in
//     the shared order relative to the ingester-only tenant.
//
//  2. In the actual scheduler the cursor is NOT shared this way: it is threaded
//     per-worker via DequeueArgs.LastTenantIndex (scheduler.QuerierLoop keeps
//     lastTenantIdx as a per-connection local). With a per-worker cursor and a
//     stable worker->component mapping, store-gateway tenants share fairly.
//
//  3. A worker does NOT scavenge other components on a non-empty shuffle-shard
//     miss; it only moves on when its prioritized component node fully empties.
//     This contradicts the doc comment on ConsumerWorkerQueuePriorityAlgo.
//
//  4. A tree-level-only re-enqueue of a fully-drained tenant leaves it invisible
//     because tenantNodes keeps a lingering key; in production the broker's
//     removeTenant + createOrUpdateTenant->AddTenant path re-inserts it. The
//     invisible-tenant result is therefore a test artifact of bypassing the broker.

package tree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// simulateSharedCursor runs dequeues threading a SINGLE cursor across all workers
// and components (the hypothesized-but-incorrect wiring). pick(i) returns the
// worker id for iteration i. Every seeded path is topped up each iteration so no
// node empties. Returns served counts keyed by "component/tenant".
func simulateSharedCursor(t *testing.T, seed []QueuePath, iterations int, pick func(i int) int) map[string]int {
	t.Helper()
	priority := NewConsumerWorkerQueuePriorityAlgo()
	tenantAlgo := NewTenantConsumerQueuingAlgorithm()
	tr, err := NewTree(priority, tenantAlgo)
	require.NoError(t, err)
	for _, p := range seed {
		require.NoError(t, tr.EnqueueBackByPath(p, "seed"))
	}

	served := map[string]int{}
	last := -1
	for i := range iterations {
		for _, p := range seed {
			_ = tr.EnqueueBackByPath(p, "x")
		}
		path, v := tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: pick(i), LastTenantIndex: last})
		if v != nil && len(path) == 2 {
			served[path[0]+"/"+path[1]]++
			last = tenantAlgo.TenantOrderIndex()
		}
	}
	return served
}

// TestInvestigation_SharedCursorStarvesStoreGatewayTenant demonstrates finding (1):
// with a single shared cursor and an ingester-only tenant sitting between the two
// store-gateway tenants in the order, one store-gateway tenant is starved.
func TestInvestigation_SharedCursorStarvesStoreGatewayTenant(t *testing.T) {
	// order becomes [T, I, X]: T(sg), I(ingester), X(sg)
	served := simulateSharedCursor(t,
		[]QueuePath{{"store-gateway", "T"}, {"ingester", "I"}, {"store-gateway", "X"}},
		3000,
		func(i int) int {
			// 3 ingester-prioritized dequeues per store-gateway dequeue.
			// nodeOrder=[store-gateway, ingester]; wid%2==1 -> ingester.
			if i%4 == 3 {
				return 0 // store-gateway
			}
			return 1 // ingester
		},
	)
	t.Logf("shared-cursor served=%v", served)
	// X (immediately after the ingester tenant in the ring) is favored; T starves.
	require.GreaterOrEqual(t, served["store-gateway/X"], 2*served["store-gateway/T"]+1,
		"shared cursor should skew store-gateway serving toward X")
}

// TestInvestigation_SharedCursorControlIsFair is the control for finding (1):
// remove the ingester tenant from the order and the two store-gateway tenants
// split evenly, confirming the ingester tenant's presence is what skews the split.
func TestInvestigation_SharedCursorControlIsFair(t *testing.T) {
	served := simulateSharedCursor(t,
		[]QueuePath{{"store-gateway", "T"}, {"store-gateway", "X"}},
		3000,
		func(int) int { return 0 },
	)
	t.Logf("control served=%v", served)
	assert.InDelta(t, served["store-gateway/T"], served["store-gateway/X"], 1)
}

// TestInvestigation_SharedCursorVictimDependsOnPosition demonstrates that the
// starved store-gateway tenant is determined by position: moving the ingester
// tenant to the end of the order flips which store-gateway tenant starves.
func TestInvestigation_SharedCursorVictimDependsOnPosition(t *testing.T) {
	// order becomes [T, X, I]
	served := simulateSharedCursor(t,
		[]QueuePath{{"store-gateway", "T"}, {"store-gateway", "X"}, {"ingester", "I"}},
		3000,
		func(i int) int {
			if i%4 == 3 {
				return 1 // ingester (nodeOrder=[store-gateway, ingester])
			}
			return 0 // store-gateway
		},
	)
	t.Logf("victim-position served=%v", served)
	// With I last, T (after I in the ring) is now favored and X starves.
	require.GreaterOrEqual(t, served["store-gateway/T"], 2*served["store-gateway/X"])
}

// TestInvestigation_PerWorkerCursorIsFair demonstrates finding (2): with each
// worker threading its OWN cursor and pinned to a stable component, the two
// store-gateway tenants split evenly regardless of ingester traffic.
func TestInvestigation_PerWorkerCursorIsFair(t *testing.T) {
	priority := NewConsumerWorkerQueuePriorityAlgo()
	tenantAlgo := NewTenantConsumerQueuingAlgorithm()
	tr, err := NewTree(priority, tenantAlgo)
	require.NoError(t, err)
	seed := []QueuePath{{"store-gateway", "T"}, {"ingester", "I"}, {"store-gateway", "X"}}
	for _, p := range seed {
		require.NoError(t, tr.EnqueueBackByPath(p, "seed"))
	}

	const numWorkers = 4
	last := make([]int, numWorkers)
	for i := range last {
		last[i] = -1
	}
	served := map[string]int{}
	for i := range 4000 {
		for _, p := range seed {
			_ = tr.EnqueueBackByPath(p, "x")
		}
		w := i % numWorkers // even -> store-gateway, odd -> ingester (nodeOrder len 2)
		path, v := tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: w, LastTenantIndex: last[w]})
		if v != nil && len(path) == 2 {
			served[path[0]+"/"+path[1]]++
			last[w] = tenantAlgo.TenantOrderIndex()
		}
	}
	t.Logf("per-worker served=%v", served)
	assert.InDelta(t, served["store-gateway/T"], served["store-gateway/X"], 1,
		"per-worker cursor must serve store-gateway tenants fairly")
}

// TestInvestigation_NoScavengeOnNonEmptyMiss demonstrates finding (3): a worker
// prioritizing a non-empty-but-unservable component returns nil rather than
// scavenging servable work in another component; it only moves on once its
// prioritized component node empties and is deleted.
func TestInvestigation_NoScavengeOnNonEmptyMiss(t *testing.T) {
	t.Run("non-empty miss does not scavenge", func(t *testing.T) {
		priority := NewConsumerWorkerQueuePriorityAlgo()
		tenantAlgo := NewTenantConsumerQueuingAlgorithm()
		tr, err := NewTree(priority, tenantAlgo)
		require.NoError(t, err)
		require.NoError(t, tr.EnqueueBackByPath(QueuePath{"ingester", "A"}, "a"))
		require.NoError(t, tr.EnqueueBackByPath(QueuePath{"store-gateway", "B"}, "b"))
		tenantAlgo.SetConsumersForTenant("A", map[ConsumerID]struct{}{"other": {}}) // c cannot serve A
		tenantAlgo.SetConsumersForTenant("B", map[ConsumerID]struct{}{"c": {}})     // c can serve B

		// worker 0 prioritizes ingester (nodeOrder[0]); ingester holds only unservable A.
		path, v := tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: 0, LastTenantIndex: -1})
		require.Equal(t, "ingester", priority.nodeOrder[0])
		assert.Nil(t, v, "worker did not scavenge servable store-gateway work")
		assert.Equal(t, QueuePath{"ingester"}, path)
	})

	t.Run("empty prioritized node lets worker move on", func(t *testing.T) {
		priority := NewConsumerWorkerQueuePriorityAlgo()
		tenantAlgo := NewTenantConsumerQueuingAlgorithm()
		tr, err := NewTree(priority, tenantAlgo)
		require.NoError(t, err)
		require.NoError(t, tr.EnqueueBackByPath(QueuePath{"ingester", "A"}, "a"))
		require.NoError(t, tr.EnqueueBackByPath(QueuePath{"store-gateway", "B"}, "b"))
		tenantAlgo.SetConsumersForTenant("A", map[ConsumerID]struct{}{"c": {}})
		tenantAlgo.SetConsumersForTenant("B", map[ConsumerID]struct{}{"c": {}})

		// drain ingester so its component node is deleted, then dequeue again.
		tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: 0, LastTenantIndex: -1})
		path, v := tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: 0, LastTenantIndex: -1})
		assert.Equal(t, "b", v)
		assert.Equal(t, QueuePath{"store-gateway", "B"}, path)
	})
}

// TestInvestigation_ReaddAfterDrainIsBrokerResponsibility demonstrates finding (4):
// at the bare-tree level, re-enqueueing a fully-drained tenant does NOT repopulate
// tenantIDOrder because tenantNodes retains a lingering key, so the tenant becomes
// invisible. This is an artifact of bypassing the broker: production re-adds the
// tenant via createOrUpdateTenant -> AddTenant (see the broker-level test).
func TestInvestigation_ReaddAfterDrainIsBrokerResponsibility(t *testing.T) {
	tenantAlgo := NewTenantConsumerQueuingAlgorithm()
	tr, err := NewTree(NewRoundRobinState(), tenantAlgo)
	require.NoError(t, err)

	require.NoError(t, tr.EnqueueBackByPath(QueuePath{"comp", "T"}, "1"))
	_, v := tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: 0, LastTenantIndex: -1})
	require.Equal(t, "1", v)

	// T fully drained: order no longer contains T, but the tenantNodes key lingers.
	require.NotContains(t, tenantAlgo.TenantIDOrder(), "T")
	_, keyLingers := tenantAlgo.tenantNodes["T"]
	require.True(t, keyLingers, "tenantNodes retains the drained tenant's key")

	// Re-enqueue via the bare tree (addChildNode path only): T is NOT re-added to order.
	require.NoError(t, tr.EnqueueBackByPath(QueuePath{"comp", "T"}, "2"))
	require.NotContains(t, tenantAlgo.TenantIDOrder(), "T")
	_, v2 := tr.Dequeue(&DequeueArgs{ConsumerID: "c", WorkerID: 0, LastTenantIndex: -1})
	assert.Nil(t, v2, "bare-tree re-enqueue leaves the drained tenant invisible (broker's AddTenant fixes this)")
}
