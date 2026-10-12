// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// passThroughQuerier satisfies storage.Querier and mimirSearcher. The search
// methods record which one was called and return a single configured result so
// the test can verify pass-through happened without re-testing the merge logic.
type passThroughQuerier struct {
	searchNamesResults  []storage.SearchResult
	searchValuesResults []storage.SearchResult
	searchValuesName    string
	searchNamesCalls    int
	searchValuesCalls   int
}

func (p *passThroughQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (p *passThroughQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (p *passThroughQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (p *passThroughQuerier) Close() error { return nil }

func (p *passThroughQuerier) SearchLabelNames(_ context.Context, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	p.searchNamesCalls++
	return storage.NewSearchResultSetFromSlice(p.searchNamesResults, nil)
}

func (p *passThroughQuerier) SearchLabelValues(_ context.Context, name string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	p.searchValuesCalls++
	p.searchValuesName = name
	return storage.NewSearchResultSetFromSlice(p.searchValuesResults, nil)
}

// nonSearcherQuerier deliberately omits the mimirSearcher methods so we can
// verify the graceful-degradation path through memoryTrackingQuerier.
type nonSearcherQuerier struct{}

func (n *nonSearcherQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (n *nonSearcherQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (n *nonSearcherQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (n *nonSearcherQuerier) Close() error { return nil }

func TestMemoryTrackingQuerier_SearchLabelNames_PassesThrough(t *testing.T) {
	inner := &passThroughQuerier{searchNamesResults: []storage.SearchResult{{Value: "foo", Score: 1.0}}}
	q := &memoryTrackingQuerier{inner: inner}

	rs := q.SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []string{"foo"}, got)
	assert.Equal(t, 1, inner.searchNamesCalls, "inner.SearchLabelNames must be called exactly once")
}

func TestMemoryTrackingQuerier_SearchLabelValues_PassesThroughLabelName(t *testing.T) {
	inner := &passThroughQuerier{searchValuesResults: []storage.SearchResult{{Value: "prod", Score: 1.0}}}
	q := &memoryTrackingQuerier{inner: inner}

	rs := q.SearchLabelValues(context.Background(), "env", nil, nil)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []string{"prod"}, got)
	assert.Equal(t, "env", inner.searchValuesName, "label name must reach inner querier")
	assert.Equal(t, 1, inner.searchValuesCalls)
}

func TestMemoryTrackingQuerier_SearchLabelNames_InnerWithoutSearchReturnsErr(t *testing.T) {
	q := &memoryTrackingQuerier{inner: &nonSearcherQuerier{}}
	rs := q.SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
}

func TestMemoryTrackingQuerier_SearchLabelValues_InnerWithoutSearchReturnsErr(t *testing.T) {
	q := &memoryTrackingQuerier{inner: &nonSearcherQuerier{}}
	rs := q.SearchLabelValues(context.Background(), "env", nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
}

// deduplicatingQuerier counts the labels of each series with the deduplicator in the context, as the ingester and store-gateway clients do.
// It wraps each source in a series set as given, without sorting.
// It merges the sets.
type deduplicatingQuerier struct {
	nonSearcherQuerier
	sources [][]labels.Labels
}

func (q *deduplicatingQuerier) Select(ctx context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	tracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	deduplicator, err := limiter.SeriesLabelsDeduplicatorFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	sets := make([]storage.SeriesSet, 0, len(q.sources))
	for _, source := range q.sources {
		sourceSeries := make([]storage.Series, 0, len(source))
		for _, lbls := range source {
			uniqueLabels, err := deduplicator.Deduplicate(lbls, tracker)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}

			sourceSeries = append(sourceSeries, series.NewConcreteSeries(uniqueLabels, nil, nil))
		}

		sets = append(sets, series.NewConcreteSeriesSetFromSortedSeries(sourceSeries))
	}

	return storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
}

func TestMemoryTrackingQuerier_Select_MergedSetYieldsSeriesTwice(t *testing.T) {
	seriesA := labels.FromStrings(model.MetricNameLabel, "up", "pod", "a")
	seriesB := labels.FromStrings(model.MetricNameLabel, "up", "pod", "b")

	// The first source is not sorted.
	// So the merge yields seriesA twice.
	// The deduplicator counts the labels of seriesA once.
	inner := &deduplicatingQuerier{sources: [][]labels.Labels{{seriesB, seriesA}, {seriesA}}}
	q := &memoryTrackingQuerier{inner: inner, metrics: limiter.NewSeriesDeduplicatorMetrics(nil)}

	tracker := limiter.NewMemoryConsumptionTracker(t.Context(), 0, nil, "test query")
	ctx := limiter.AddMemoryTrackerToContext(t.Context(), tracker)

	// Keep the labels of each series, as the query engine does.
	set := q.Select(ctx, true, nil)
	var kept []labels.Labels
	for set.Next() {
		lbls := set.At().Labels()
		require.NoError(t, tracker.IncreaseMemoryConsumptionForLabels(lbls))
		kept = append(kept, lbls)
	}
	require.NoError(t, set.Err())
	require.Len(t, kept, 3)

	// The query engine returns the labels that it kept.
	require.NotPanics(t, func() {
		for _, lbls := range kept {
			tracker.DecreaseMemoryConsumptionForLabels(lbls)
		}
	})
	require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.Labels))
}

func BenchmarkMemoryTrackingQuerier_Select(b *testing.B) {
	for _, seriesCount := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("series=%d", seriesCount), func(b *testing.B) {
			source := make([]labels.Labels, 0, seriesCount)
			for i := range seriesCount {
				source = append(source, labels.FromStrings(model.MetricNameLabel, "up", "pod", fmt.Sprintf("pod-%06d", i)))
			}

			inner := &deduplicatingQuerier{sources: [][]labels.Labels{source}}
			q := &memoryTrackingQuerier{inner: inner, metrics: limiter.NewSeriesDeduplicatorMetrics(nil)}

			b.ReportAllocs()

			for b.Loop() {
				tracker := limiter.NewMemoryConsumptionTracker(b.Context(), 0, nil, "test query")
				set := q.Select(limiter.AddMemoryTrackerToContext(b.Context(), tracker), true, nil)
				for set.Next() {
					set.At()
				}

				if err := set.Err(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
