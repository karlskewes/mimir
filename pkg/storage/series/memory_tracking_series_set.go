// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// MemoryTrackingSeriesSet is a storage.SeriesSet wrapper that decreases memory
// consumption as series are consumed from the wrapped SeriesSet.
//
// The given SeriesLabelsDeduplicator counts the labels of each series before the wrapped SeriesSet yields it.
// The ingester and store-gateway clients do this during Select.
//
// MemoryTrackingSeriesSet releases the labels of each series through the deduplicator on the first At() call after Next().
// The wrapped SeriesSet can yield a series more than once, or yield a series that the deduplicator did not count.
// In both cases, the labels memory does not go below zero.
// But the tracker can report less labels memory than the query holds, until the caller consumes the remaining series.
type MemoryTrackingSeriesSet struct {
	inner                    storage.SeriesSet
	deduplicator             limiter.SeriesLabelsDeduplicator
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	// This flag prevents multiple decrements for the same series position when At() is called multiple times.
	memoryDecreased bool
}

func NewMemoryTrackingSeriesSet(inner storage.SeriesSet, deduplicator limiter.SeriesLabelsDeduplicator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) storage.SeriesSet {
	return &MemoryTrackingSeriesSet{
		inner:                    inner,
		deduplicator:             deduplicator,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (m *MemoryTrackingSeriesSet) Next() bool {
	m.memoryDecreased = false
	return m.inner.Next()
}

func (m *MemoryTrackingSeriesSet) At() storage.Series {
	at := m.inner.At()

	// Only decrease memory once per each single Next() call and multiple At() call
	if !m.memoryDecreased {
		// New series at this position
		m.memoryDecreased = true
		// The caller should IncreaseMemoryConsumption for labels if they want to retain the labels.
		// In the future we should avoid calling decrease here and make it the caller responsibility.
		m.deduplicator.Release(at.Labels(), m.memoryConsumptionTracker)
	}
	return at
}

func (m *MemoryTrackingSeriesSet) Err() error {
	return m.inner.Err()
}

func (m *MemoryTrackingSeriesSet) Warnings() annotations.Annotations {
	return m.inner.Warnings()
}
