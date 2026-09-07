// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_stores_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	filesystemstore "github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestNewBucketStores_PartitionerConfig(t *testing.T) {
	tests := []struct {
		cfgValue uint64
		expected uint64
	}{
		{200, 200},
		{0, 100},
	}

	for _, tt := range tests {
		cfg := prepareStorageConfig(t)
		cfg.BucketStore.PartitionerMaxGapBytes = 100
		cfg.BucketStore.PartitionerMaxGapBytesChunks = tt.cfgValue

		storageDir := t.TempDir()
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
		require.NoError(t, err)

		stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bucket, nil, defaultLimitsOverrides(t), log.NewNopLogger(), nil)
		require.NoError(t, err)

		require.IsType(t, &gapBasedPartitioner{}, stores.partitioners.chunks)
		require.IsType(t, &gapBasedPartitioner{}, stores.partitioners.series)
		require.IsType(t, &gapBasedPartitioner{}, stores.partitioners.postings)
		assert.Equal(t, tt.expected, stores.partitioners.chunks.(*gapBasedPartitioner).maxGapBytes)
		assert.Equal(t, uint64(100), stores.partitioners.series.(*gapBasedPartitioner).maxGapBytes)
		assert.Equal(t, uint64(100), stores.partitioners.postings.(*gapBasedPartitioner).maxGapBytes)
	}
}

func TestBucketStores_InitialSync(t *testing.T) {

	userToMetric := map[string]string{
		"user-1": "series_1",
		"user-2": "series_2",
	}

	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	for userID, metricName := range userToMetric {
		generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	}

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewLogfmtLogger(os.Stdout), reg)
	require.NoError(t, err)

	// Query series before the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := querySeries(t, stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		assert.Empty(t, seriesSet)
	}
	for userID := range userToMetric {
		createBucketIndex(t, bucket, userID)
	}
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query series after the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := querySeries(t, stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		require.Len(t, seriesSet, 1)
		assert.Equal(t, []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: metricName}}, seriesSet[0].Labels)
	}

	// Query series of another user.
	seriesSet, warnings, err := querySeries(t, stores, "user-1", "series_2", 20, 40)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 2

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 2

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 0

			# HELP cortex_bucket_stores_gate_queries_concurrent_max Number of maximum concurrent queries allowed.
			# TYPE cortex_bucket_stores_gate_queries_concurrent_max gauge
			cortex_bucket_stores_gate_queries_concurrent_max{gate="query"} 200
			cortex_bucket_stores_gate_queries_concurrent_max{gate="index_header"} 4

			# HELP cortex_bucket_stores_gate_queries_in_flight Number of queries that are currently in flight.
			# TYPE cortex_bucket_stores_gate_queries_in_flight gauge
			cortex_bucket_stores_gate_queries_in_flight{gate="query"} 0
			cortex_bucket_stores_gate_queries_in_flight{gate="index_header"} 0
	`),
		"cortex_bucket_store_blocks_loaded",
		"cortex_bucket_store_block_loads_total",
		"cortex_bucket_store_block_load_failures_total",
		"cortex_bucket_stores_gate_queries_concurrent_max",
		"cortex_bucket_stores_gate_queries_in_flight",
	))

	assert.Greater(t, testutil.ToFloat64(stores.syncLastSuccess), float64(0))
}

func TestBucketStores_InitialSyncShouldRetryOnFailure(t *testing.T) {
	const tenantID = "user-1"
	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	// Generate a block for the user in the storage.
	generateStorageBlock(t, storageDir, tenantID, "series_1", 10, 100, 15)
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	createBucketIndex(t, bucket, tenantID)

	// Wrap the bucket to fail the 1st Get() request.
	bucket = &failFirstGetBucket{Bucket: bucket}

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Initial sync should succeed even if a transient error occurs.
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query series after the initial sync.
	seriesSet, warnings, err := querySeries(t, stores, tenantID, "series_1", 20, 40)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.Len(t, seriesSet, 1)
	assert.Equal(t, []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "series_1"}}, seriesSet[0].Labels)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE cortex_blocks_meta_syncs_total counter
			cortex_blocks_meta_syncs_total 2

			# HELP cortex_blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE cortex_blocks_meta_sync_failures_total counter
			cortex_blocks_meta_sync_failures_total 1

			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 1

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 1

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 0
	`),
		"cortex_blocks_meta_syncs_total",
		"cortex_blocks_meta_sync_failures_total",
		"cortex_bucket_store_block_loads_total",
		"cortex_bucket_store_block_load_failures_total",
		"cortex_bucket_store_blocks_loaded",
	))

	assert.Greater(t, testutil.ToFloat64(stores.syncLastSuccess), float64(0))
}

func TestBucketStores_SyncBlocks(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "series_1"
	)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Run an initial sync to discover 1 block.
	generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	createBucketIndex(t, bucket, userID)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query a range for which we have no samples.
	seriesSet, warnings, err := querySeries(t, stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	// Generate another block and sync blocks again.
	generateStorageBlock(t, storageDir, userID, metricName, 100, 200, 15)
	createBucketIndex(t, bucket, userID)
	require.NoError(t, stores.SyncBlocks(ctx))

	seriesSet, warnings, err = querySeries(t, stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Len(t, seriesSet, 1)
	assert.Equal(t, []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: metricName}}, seriesSet[0].Labels)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 2

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 2

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 0

			# HELP cortex_bucket_stores_gate_queries_concurrent_max Number of maximum concurrent queries allowed.
			# TYPE cortex_bucket_stores_gate_queries_concurrent_max gauge
			cortex_bucket_stores_gate_queries_concurrent_max{gate="query"} 200
			cortex_bucket_stores_gate_queries_concurrent_max{gate="index_header"} 4

			# HELP cortex_bucket_stores_gate_queries_in_flight Number of queries that are currently in flight.
			# TYPE cortex_bucket_stores_gate_queries_in_flight gauge
			cortex_bucket_stores_gate_queries_in_flight{gate="query"} 0
			cortex_bucket_stores_gate_queries_in_flight{gate="index_header"} 0
	`),
		"cortex_bucket_store_blocks_loaded",
		"cortex_bucket_store_block_loads_total",
		"cortex_bucket_store_block_load_failures_total",
		"cortex_bucket_stores_gate_queries_concurrent_max",
		"cortex_bucket_stores_gate_queries_in_flight",
	))

	assert.Greater(t, testutil.ToFloat64(stores.syncLastSuccess), float64(0))
}

func TestBucketStores_ownedUsers(t *testing.T) {
	allUsers := []string{"user-1", "user-2", "user-3"}

	tests := map[string]struct {
		shardingStrategy ShardingStrategy
		allowedTenants   *util.AllowList
		expectedStores   int
	}{
		"when sharding is disabled all users should be synced": {
			shardingStrategy: newNoShardingStrategy(),
			expectedStores:   3,
		},
		"when sharding is enabled only stores for filtered users should be created": {
			shardingStrategy: func() ShardingStrategy {
				s := &mockShardingStrategy{}
				s.On("FilterUsers", mock.Anything, allUsers).Return([]string{"user-1", "user-2"}, nil)
				return s
			}(),
			expectedStores: 2,
		},
		"when user is disabled, their stores should not be created": {
			shardingStrategy: newNoShardingStrategy(),
			allowedTenants:   util.NewAllowList(nil, []string{"user-2"}),
			expectedStores:   2,
		},

		"when single user is enabled, only their stores should be created": {
			shardingStrategy: newNoShardingStrategy(),
			allowedTenants:   util.NewAllowList([]string{"user-3"}, nil),
			expectedStores:   1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := prepareStorageConfig(t)

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("", allUsers, nil)

			stores, err := NewBucketStores(cfg, "", testData.shardingStrategy, bucketClient, testData.allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), nil)
			require.NoError(t, err)

			// Sync user stores and count the number of times the callback is called.
			ownedUsers, err := stores.ownedUsers(context.Background())

			assert.NoError(t, err)
			bucketClient.AssertNumberOfCalls(t, "Iter", 1)
			assert.Len(t, ownedUsers, testData.expectedStores)
		})
	}
}

func TestBucketStores_Series_ShouldCorrectlyQuerySeriesSpanningMultipleChunks(t *testing.T) {
	for _, lazyLoadingEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy loading enabled = %v", lazyLoadingEnabled), func(t *testing.T) {
			testBucketStoresSeriesShouldCorrectlyQuerySeriesSpanningMultipleChunks(t, lazyLoadingEnabled)
		})
	}
}

func TestBucketStores_ChunksAndSeriesLimiterFactoriesInitializedByEnforcedLimits(t *testing.T) {
	const (
		userID                = "user-1"
		overriddenChunksLimit = 1000000
		overriddenSeriesLimit = 2000
	)

	defaultLimits := defaultLimitsConfig()

	tests := map[string]struct {
		tenantLimits        map[string]*validation.Limits
		expectedChunkLimit  uint64
		expectedSeriesLimit uint64
	}{
		"when max_fetched_chunks_per_query and max_fetched_series_per_query are not overridden, their default values are used as the limit of the Limiter": {
			expectedChunkLimit:  uint64(defaultLimits.MaxChunksPerQuery),
			expectedSeriesLimit: uint64(defaultLimits.MaxFetchedSeriesPerQuery),
		},
		"when max_fetched_chunks_per_query and max_fetched_series_per_query are overridden, the overridden values are used as the limit of the Limiter": {
			tenantLimits: map[string]*validation.Limits{
				userID: {
					MaxChunksPerQuery:        overriddenChunksLimit,
					MaxFetchedSeriesPerQuery: overriddenSeriesLimit,
				},
			},
			expectedChunkLimit:  uint64(overriddenChunksLimit),
			expectedSeriesLimit: uint64(overriddenSeriesLimit),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := prepareStorageConfig(t)

			storageDir := t.TempDir()

			bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
			require.NoError(t, err)

			overrides := validation.NewOverrides(defaultLimits, validation.NewMockTenantLimits(testData.tenantLimits))

			var allowedTenants *util.AllowList
			reg := prometheus.NewPedanticRegistry()
			stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bucket, allowedTenants, overrides, log.NewNopLogger(), reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), stores))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
			})

			store, err := stores.getOrCreateStore(context.Background(), userID)
			require.NoError(t, err)

			chunksLimit := overrides.MaxChunksPerQuery(userID)
			if chunksLimit != 0 {
				chunksLimiter := store.chunksLimiterFactory(promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "chunks"}))
				err = chunksLimiter.Reserve(testData.expectedChunkLimit)
				require.NoError(t, err)

				err = chunksLimiter.Reserve(1)
				require.Error(t, err)
			}

			seriesLimit := overrides.MaxFetchedSeriesPerQuery(userID)
			if seriesLimit != 0 {
				seriesLimiter := store.seriesLimiterFactory(promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "series"}))
				err = seriesLimiter.Reserve(testData.expectedSeriesLimit)
				require.NoError(t, err)

				err = seriesLimiter.Reserve(1)
				require.Error(t, err)
			}

		})
	}
}

func testBucketStoresSeriesShouldCorrectlyQuerySeriesSpanningMultipleChunks(t *testing.T, lazyLoadingEnabled bool) {
	const (
		userID     = "user-1"
		metricName = "series_1"
	)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.IndexHeader.LazyLoadingEnabled = lazyLoadingEnabled
	cfg.BucketStore.IndexHeader.LazyLoadingIdleTimeout = time.Minute

	storageDir := t.TempDir()

	// Generate a single block with 1 series and a lot of samples.
	generateStorageBlock(t, storageDir, userID, metricName, 0, 10000, 1)

	promBlock := openPromBlocks(t, filepath.Join(storageDir, userID))[0]

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	createBucketIndex(t, bucket, userID)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	tests := map[string]struct {
		reqMinTime int64
		reqMaxTime int64
	}{
		"query the entire block": {
			reqMinTime: math.MinInt64,
			reqMaxTime: math.MaxInt64,
		},
		"query the beginning of the block": {
			reqMinTime: 0,
			reqMaxTime: 100,
		},
		"query the middle of the block": {
			reqMinTime: 4000,
			reqMaxTime: 4050,
		},
		"query the end of the block": {
			reqMinTime: 9800,
			reqMaxTime: 10000,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Query a range for which we have no samples.
			seriesSet, warnings, err := querySeries(t, stores, userID, metricName, testData.reqMinTime, testData.reqMaxTime)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Len(t, seriesSet, 1)

			compareToPromChunks(t, seriesSet[0].Chunks, mimirpb.FromLabelAdaptersToLabels(seriesSet[0].Labels), testData.reqMinTime, testData.reqMaxTime, promBlock)
		})
	}
}

func TestBucketStore_Series_ShouldQueryBlockWithOutOfOrderChunks(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test"
	)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)
	fixtureDir := filepath.Join("fixtures", "test-query-block-with-ooo-chunks")
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	seriesWithOutOfOrderChunks := labels.FromStrings("case", "out_of_order", model.MetricNameLabel, metricName)
	seriesWithOverlappingChunks := labels.FromStrings("case", "overlapping", model.MetricNameLabel, metricName)

	// Utility function originally used to generate a block with out of order chunks
	// used by this test. The block has been generated commenting out the checks done
	// by TSDB block Writer to prevent OOO chunks writing.
	_ = func() {
		specs := []*block.SeriesSpec{
			// Series with out of order chunks.
			{
				Labels: seriesWithOutOfOrderChunks,
				Chunks: []chunks.Meta{
					must(chunks.ChunkFromSamples([]chunks.Sample{test.Sample{TS: 20, Val: 20}, test.Sample{TS: 21, Val: 21}})),
					must(chunks.ChunkFromSamples([]chunks.Sample{test.Sample{TS: 10, Val: 10}, test.Sample{TS: 11, Val: 11}})),
				},
			},
			// Series with out of order and overlapping chunks.
			{
				Labels: seriesWithOverlappingChunks,
				Chunks: []chunks.Meta{
					must(chunks.ChunkFromSamples([]chunks.Sample{test.Sample{TS: 20, Val: 20}, test.Sample{TS: 21, Val: 21}})),
					must(chunks.ChunkFromSamples([]chunks.Sample{test.Sample{TS: 10, Val: 10}, test.Sample{TS: 20, Val: 20}})),
				},
			},
		}

		_, err := block.GenerateBlockFromSpec(fixtureDir, specs)
		require.NoError(t, err)
	}

	// Copy blocks from fixtures dir to the test bucket.
	entries, err := os.ReadDir(fixtureDir)
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		blockID, err := ulid.Parse(entry.Name())
		require.NoErrorf(t, err, "parsing block ID from directory name %q", entry.Name())

		_, err = block.Upload(ctx, log.NewNopLogger(), userBkt, filepath.Join(fixtureDir, blockID.String()), nil)
		require.NoError(t, err)
	}

	createBucketIndex(t, bkt, userID)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bkt, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	tests := map[string]struct {
		minT                                int64
		maxT                                int64
		expectedSamplesForOutOfOrderChunks  []test.Sample
		expectedSamplesForOverlappingChunks []test.Sample
	}{
		"query all samples": {
			minT:                                math.MinInt64,
			maxT:                                math.MaxInt64,
			expectedSamplesForOutOfOrderChunks:  []test.Sample{{TS: 20, Val: 20}, {TS: 21, Val: 21}, {TS: 10, Val: 10}, {TS: 11, Val: 11}},
			expectedSamplesForOverlappingChunks: []test.Sample{{TS: 20, Val: 20}, {TS: 21, Val: 21}, {TS: 10, Val: 10}, {TS: 20, Val: 20}},
		},
		"query samples from 1st chunk only": {
			minT:                                21,
			maxT:                                22, // Not included.
			expectedSamplesForOutOfOrderChunks:  []test.Sample{{TS: 20, Val: 20}, {TS: 21, Val: 21}},
			expectedSamplesForOverlappingChunks: []test.Sample{{TS: 20, Val: 20}, {TS: 21, Val: 21}},
		},
		"query samples from 2nd (out of order) chunk only": {
			minT:                                10,
			maxT:                                11, // Not included.
			expectedSamplesForOutOfOrderChunks:  []test.Sample{{TS: 10, Val: 10}, {TS: 11, Val: 11}},
			expectedSamplesForOverlappingChunks: []test.Sample{{TS: 10, Val: 10}, {TS: 20, Val: 20}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			seriesSet, warnings, err := querySeries(t, stores, userID, metricName, testData.minT, testData.maxT)
			require.NoError(t, err)
			assert.Empty(t, warnings)

			expectedSeries := 0
			if testData.expectedSamplesForOutOfOrderChunks != nil {
				expectedSeries++
			}
			if testData.expectedSamplesForOverlappingChunks != nil {
				expectedSeries++
			}
			require.Len(t, seriesSet, expectedSeries)

			// Check returned samples.
			nextSeriesIdx := 0

			if testData.expectedSamplesForOutOfOrderChunks != nil {
				assert.Equal(t, seriesWithOutOfOrderChunks, promLabels(seriesSet[nextSeriesIdx]))

				samples, err := readSamplesFromChunks(seriesSet[nextSeriesIdx].Chunks)
				require.NoError(t, err)
				assert.Equal(t, testData.expectedSamplesForOutOfOrderChunks, samples)

				nextSeriesIdx++
			}

			if testData.expectedSamplesForOverlappingChunks != nil {
				assert.Equal(t, seriesWithOverlappingChunks, promLabels(seriesSet[nextSeriesIdx]))

				samples, err := readSamplesFromChunks(seriesSet[nextSeriesIdx].Chunks)
				require.NoError(t, err)
				assert.Equal(t, testData.expectedSamplesForOverlappingChunks, samples)
			}
		})
	}
}

func promLabels(m *storeTestSeries) labels.Labels {
	return mimirpb.FromLabelAdaptersToLabels(m.Labels)
}

// TestBucketStore_Series_IgnoreDeletionMarkAging generates several Level-1 blocks (mimicking many
// small raw blocks from different sources covering the same time range) plus one Level-2 block
// covering the same range (the "already compacted" replacement), marks each Level-1 block for
// deletion at a different simulated age, and checks via a real Series() call which blocks are
// still served at each age relative to the store-gateway-side IgnoreDeletionMarksInStoreGatewayDelay
// (production default 1h).
func TestBucketStore_Series_IgnoreDeletionMarkAging(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test"
	)

	// Small synthetic timestamps, not real wall-clock based, so blocks aren't dropped by the
	// unrelated IgnoreBlocksWithin filter (excludes blocks whose MinTime isn't more than its
	// configured duration, 10h by default, before real time.Now()).
	const blockMinT, blockMaxT, step = 0, 10000, 100

	ages := []struct {
		name string
		age  time.Duration
	}{
		{"10m", 10 * time.Minute},
		{"20m", 20 * time.Minute},
		{"30m", 30 * time.Minute},
		{"33m", 33 * time.Minute},
		{"40m", 40 * time.Minute},
		{"50m", 50 * time.Minute},
		{"60m", 60 * time.Minute},
		{"70m", 70 * time.Minute},
	}

	ctx := context.Background()
	cfg := prepareStorageConfig(t)
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	// Deletion marks are discovered by the bucket index updater via the global markers
	// convention (markers/<id>-deletion-mark.json), not the per-block path alone.
	bkt = block.BucketWithGlobalMarkers(bkt)
	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	// Generate one Level-1 block per tested age, all covering the same time range.
	for range ages {
		generateStorageBlock(t, storageDir, userID, metricName, blockMinT, blockMaxT, step)
	}

	entries, err := os.ReadDir(filepath.Join(storageDir, userID))
	require.NoError(t, err)

	var l1BlockIDs []ulid.ULID
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id, err := ulid.Parse(entry.Name())
		require.NoError(t, err)
		l1BlockIDs = append(l1BlockIDs, id)
	}
	require.Len(t, l1BlockIDs, len(ages))

	// Generate the Level-2 "already compacted" replacement block covering the same time range.
	// GenerateBlockFromSpec always writes Compaction.Level=1, so override it and rewrite meta.json
	// before uploading.
	l2Dir := t.TempDir()
	specs := []*block.SeriesSpec{{
		Labels: labels.FromStrings(model.MetricNameLabel, metricName),
		Chunks: []chunks.Meta{must(chunks.ChunkFromSamples([]chunks.Sample{
			test.Sample{TS: blockMinT, Val: 1},
			test.Sample{TS: blockMaxT - 1, Val: 1},
		}))},
	}}
	l2Meta, err := block.GenerateBlockFromSpec(l2Dir, specs)
	require.NoError(t, err)
	l2Meta.Compaction.Level = 2
	l2BlockDir := filepath.Join(l2Dir, l2Meta.ULID.String())
	require.NoError(t, l2Meta.WriteToDir(log.NewNopLogger(), l2BlockDir))
	_, err = block.Upload(ctx, log.NewNopLogger(), userBkt, l2BlockDir, nil)
	require.NoError(t, err)

	// Mark every Level-1 block for deletion, each at a different simulated age.
	now := time.Now()
	for i, a := range ages {
		mark := block.DeletionMark{
			ID:           l1BlockIDs[i],
			DeletionTime: now.Add(-a.age).Unix(),
			Version:      block.DeletionMarkVersion1,
		}

		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(&mark))
		require.NoError(t, userBkt.Upload(ctx, path.Join(mark.ID.String(), block.DeletionMarkFilename), &buf))
	}

	createBucketIndex(t, bkt, userID)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bkt, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	req := &storepb.SeriesRequest{
		MinTime: blockMinT,
		MaxTime: blockMaxT,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  model.MetricNameLabel,
			Value: metricName,
		}},
	}

	for i, a := range ages {
		t.Run(a.name, func(t *testing.T) {
			srv := newStoreGatewayTestServer(t, stores)
			_, warnings, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
			require.NoError(t, err)
			assert.Empty(t, warnings)

			queried := make(map[string]bool, len(hints.QueriedBlocks))
			for _, b := range hints.QueriedBlocks {
				queried[b.Id] = true
			}

			assert.True(t, queried[l2Meta.ULID.String()], "Level-2 replacement block should always be served")

			l1ID := l1BlockIDs[i].String()
			switch a.name {
			case "60m":
				// Deletion mark age equals the filter's delay exactly (IgnoreDeletionMarkFilter
				// drops a block only once age strictly exceeds the delay). Real elapsed time
				// between writing the mark and the filter evaluating it makes this boundary
				// non-deterministic, so it's reported but not asserted.
				t.Logf("age=60m boundary: Level-1 block %s queried=%v (informational only)", l1ID, queried[l1ID])
			case "70m":
				assert.False(t, queried[l1ID], "Level-1 block should be dropped at age=%s (past the 1h delay)", a.name)
			default:
				assert.True(t, queried[l1ID], "Level-1 block should still be served at age=%s (within the 1h delay)", a.name)
			}
		})
	}
}

// TestBucketStore_Series_MixedCompactionLevelsAlwaysQueried checks whether block selection at
// request-execution time distinguishes blocks by compaction level at all, using three unmarked
// blocks (Level 1, Level 2, Level 3) that all cover the same time range.
//
// A live Level-1 + Level-2 + Level-3 overlap for the exact same data is not expected to occur
// naturally in production: a Level-1-to-Level-2 merge is exempt from the compactor's "don't
// compact recent blocks prematurely" guard when maxCompactionLevel()==1
// (split_merge_grouper.go:178-182), so it happens within minutes of the source blocks appearing.
// A Level-2-to-Level-3 merge has no such exemption (its inputs are already Level 2), so it only
// becomes eligible once the full higher-range window has closed - by which time the original
// Level-1 sources have long since aged past the store-gateway's 1h deletion-mark delay and been
// physically dropped (see TestBucketStore_Series_IgnoreDeletionMarkAging). This test constructs
// the state synthetically anyway, since nothing in the block-selection code prevents it, to check
// whether the "no compaction-level preference" behavior generalizes past two simultaneous levels.
func TestBucketStore_Series_MixedCompactionLevelsAlwaysQueried(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test"
	)

	const blockMinT, blockMaxT = int64(0), int64(10000)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	// Level-1 block, generated the same way as every other test in this file.
	generateStorageBlock(t, storageDir, userID, metricName, blockMinT, blockMaxT, 100)
	entries, err := os.ReadDir(filepath.Join(storageDir, userID))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	l1ID, err := ulid.Parse(entries[0].Name())
	require.NoError(t, err)

	// Level-2 and Level-3 blocks covering the same range: GenerateBlockFromSpec always writes
	// Compaction.Level=1, so override it and rewrite meta.json before uploading, same approach
	// as the Level-2 block in TestBucketStore_Series_IgnoreDeletionMarkAging.
	uploadBlockAtLevel := func(level int) ulid.ULID {
		dir := t.TempDir()
		specs := []*block.SeriesSpec{{
			Labels: labels.FromStrings(model.MetricNameLabel, metricName),
			Chunks: []chunks.Meta{must(chunks.ChunkFromSamples([]chunks.Sample{
				test.Sample{TS: blockMinT, Val: 1},
				test.Sample{TS: blockMaxT - 1, Val: 1},
			}))},
		}}
		meta, err := block.GenerateBlockFromSpec(dir, specs)
		require.NoError(t, err)
		meta.Compaction.Level = level
		blockDir := filepath.Join(dir, meta.ULID.String())
		require.NoError(t, meta.WriteToDir(log.NewNopLogger(), blockDir))
		_, err = block.Upload(ctx, log.NewNopLogger(), userBkt, blockDir, nil)
		require.NoError(t, err)
		return meta.ULID
	}
	l2ID := uploadBlockAtLevel(2)
	l3ID := uploadBlockAtLevel(3)

	createBucketIndex(t, bkt, userID)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", newNoShardingStrategy(), bkt, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	req := &storepb.SeriesRequest{
		MinTime: blockMinT,
		MaxTime: blockMaxT,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  model.MetricNameLabel,
			Value: metricName,
		}},
	}

	srv := newStoreGatewayTestServer(t, stores)
	_, warnings, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	queried := make(map[string]bool, len(hints.QueriedBlocks))
	for _, b := range hints.QueriedBlocks {
		queried[b.Id] = true
	}

	assert.True(t, queried[l1ID.String()], "Level-1 block should be queried alongside higher levels covering the same range")
	assert.True(t, queried[l2ID.String()], "Level-2 block should be queried alongside other levels covering the same range")
	assert.True(t, queried[l3ID.String()], "Level-3 block should be queried alongside lower levels covering the same range")
}

func prepareStorageConfig(tb testing.TB) mimir_tsdb.BlocksStorageConfig {
	tmpDir := tb.TempDir()

	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = tmpDir

	return cfg
}

func generateStorageBlock(t *testing.T, storageDir, userID string, metricName string, minT, maxT int64, step int) {
	// Create a directory for the user (if doesn't already exist).
	userDir := filepath.Join(storageDir, userID)
	if _, err := os.Stat(userDir); err != nil {
		require.NoError(t, os.Mkdir(userDir, os.ModePerm))
	}

	// Create a temporary directory where the TSDB is opened,
	// then it will be snapshotted to the storage directory.
	tmpDir := t.TempDir()

	db, err := tsdb.Open(tmpDir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	series := labels.FromStrings(model.MetricNameLabel, metricName)

	app := db.Appender(context.Background())
	for ts := minT; ts < maxT; ts += int64(step) {
		_, err = app.Append(0, series, ts, 1)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	// Snapshot TSDB to the storage directory.
	require.NoError(t, db.Snapshot(userDir, true))
}

func querySeries(t *testing.T, stores *BucketStores, userID, metricName string, minT, maxT int64) ([]*storeTestSeries, annotations.Annotations, error) {
	req := &storepb.SeriesRequest{
		MinTime: minT,
		MaxTime: maxT,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  model.MetricNameLabel,
			Value: metricName,
		}},
	}

	srv := newStoreGatewayTestServer(t, stores)
	seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(context.Background(), userID), req)

	return seriesSet, warnings, err
}

func setUserIDToGRPCContext(ctx context.Context, userID string) context.Context {
	return grpc_metadata.AppendToOutgoingContext(ctx, GrpcContextMetadataTenantID, userID)
}

func TestBucketStores_deleteLocalFilesForExcludedTenants(t *testing.T) {
	const (
		user1 = "user-1"
		user2 = "user-2"
	)

	userToMetric := map[string]string{
		user1: "series_1",
		user2: "series_2",
	}

	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	for userID, metricName := range userToMetric {
		generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	}

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	for userID := range userToMetric {
		createBucketIndex(t, bucket, userID)
	}

	sharding := userShardingStrategy{}

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, "", &sharding, bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Perform sync.
	sharding.users = []string{user1, user2}
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})
	require.Equal(t, []string{user1, user2}, getUsersInDir(t, cfg.BucketStore.SyncDir))

	metricNames := []string{"cortex_bucket_store_block_drops_total", "cortex_bucket_store_block_loads_total", "cortex_bucket_store_blocks_loaded"}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 0
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 2
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 2
	`), metricNames...))

	// Single user left in shard.
	sharding.users = []string{user1}
	require.NoError(t, stores.SyncBlocks(ctx))
	require.Equal(t, []string{user1}, getUsersInDir(t, cfg.BucketStore.SyncDir))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 1
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 2
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 1
	`), metricNames...))

	// No users left in this shard.
	sharding.users = nil
	require.NoError(t, stores.SyncBlocks(ctx))
	require.Equal(t, []string(nil), getUsersInDir(t, cfg.BucketStore.SyncDir))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 2
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 2
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 0
	`), metricNames...))

	// We can always get user back.
	sharding.users = []string{user1}
	require.NoError(t, stores.SyncBlocks(ctx))
	require.Equal(t, []string{user1}, getUsersInDir(t, cfg.BucketStore.SyncDir))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 2
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 3
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 1
	`), metricNames...))
}

func getUsersInDir(t *testing.T, dir string) []string {
	fs, err := os.ReadDir(dir)
	require.NoError(t, err)

	var result []string
	for _, fi := range fs {
		if fi.IsDir() {
			result = append(result, fi.Name())
		}
	}
	slices.Sort(result)
	return result
}

type userShardingStrategy struct {
	users []string
}

func (u *userShardingStrategy) FilterUsers(context.Context, []string) ([]string, error) {
	return u.users, nil
}

func (u *userShardingStrategy) FilterBlocks(_ context.Context, userID string, metas map[ulid.ULID]*block.Meta, _ map[ulid.ULID]struct{}, _ block.GaugeVec) error {
	if slices.Contains(u.users, userID) {
		return nil
	}

	for k := range metas {
		delete(metas, k)
	}
	return nil
}

// failFirstGetBucket is an objstore.Bucket wrapper which fails the first Get() request with a mocked error.
type failFirstGetBucket struct {
	objstore.Bucket

	firstGet atomic.Bool
}

func (f *failFirstGetBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if f.firstGet.CompareAndSwap(false, true) {
		return nil, errors.New("Get() request mocked error")
	}

	return f.Bucket.Get(ctx, name)
}

func indexHeaderCachingBucket(
	tb testing.TB, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer,
) (objstore.Bucket, error) {

	// With a non-nil index-header cache, the default cache config will enable index-header caching.
	indexHeaderCache := cache.NewMockCache()
	blocksStorageCfg := prepareStorageConfig(tb)

	return mimir_tsdb.NewStoreCachingBucket(
		"", blocksStorageCfg, nil, indexHeaderCache, nil, bkt, logger, reg,
	)
}

func BenchmarkBucketStoreLabelValues(tb *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := tb.TempDir()

	bkt, err := filesystemstore.NewBucket(filepath.Join(dir, "bkt"))
	assert.NoError(tb, err)
	defer func() { assert.NoError(tb, bkt.Close()) }()

	series := generateSeries([]int{1, 10, 100, 1000})
	highCardinalitySeries := prefixLabels("high_cardinality_", generateSeries([]int{1, 1_000_000}))
	series = append(series, highCardinalitySeries...)
	tb.Logf("Total %d series generated", len(series))

	bucketPostingsOffsets := indexheader.BucketReaderConfig{
		Enabled:             true,
		BucketIndexSections: indexheader.SectionPostingsOffsetsTable,
	}

	storeConfigs := []struct {
		name            string
		bucketReaderCfg indexheader.BucketReaderConfig
		wrapBucket      func(tb testing.TB, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error)
	}{
		{
			name:            "Reader=disk",
			bucketReaderCfg: indexheader.BucketReaderConfig{},
		},
		{
			name:            "Reader=bucket/BucketCache=off",
			bucketReaderCfg: bucketPostingsOffsets,
		},
		{
			name:            "Reader=bucket/BucketCache=index-header",
			bucketReaderCfg: bucketPostingsOffsets,
			wrapBucket:      indexHeaderCachingBucket,
		},
	}

	for _, storeCfg := range storeConfigs {
		tb.Run(storeCfg.name, func(tb *testing.B) {
			dir := tb.TempDir()

			bkt, err := filesystemstore.NewBucket(filepath.Join(dir, "bkt"))
			assert.NoError(tb, err)
			defer func() { assert.NoError(tb, bkt.Close()) }()

			prepareCfg := defaultPrepareStoreConfig(tb)
			prepareCfg.tempDir = dir
			prepareCfg.series = series
			prepareCfg.postingsStrategy = worstCaseFetchedDataStrategy{1.0}
			prepareCfg.bucketStoreConfig.IndexHeader.BucketReader = storeCfg.bucketReaderCfg
			prepareCfg.wrapBucket = storeCfg.wrapBucket

			s := prepareStoreWithTestBlocks(tb, bkt, prepareCfg)
			mint, maxt := s.store.TimeRange()
			assert.Equal(tb, s.minTime, mint)
			assert.Equal(tb, s.maxTime, maxt)

			indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(indexcache.InMemoryIndexCacheConfig{
				MaxItemSizeBytes:  1e5,
				MaxCacheSizeBytes: 2e5,
			}, nil, s.logger)
			assert.NoError(tb, err)

			tb.Run("no cache", func(tb *testing.B) {
				s.cache.SwapIndexCacheWith(noopCache{})
				benchmarkBucketStoreLabelValues(ctx, tb, s.store)
			})

			tb.Run("inmemory cache (without label values cache)", func(tb *testing.B) {
				s.cache.SwapIndexCacheWith(indexCacheMissingLabelValues{indexCache})
				benchmarkBucketStoreLabelValues(ctx, tb, s.store)
			})
		})
	}
}

func benchmarkBucketStoreLabelValues(ctx context.Context, tb *testing.B, store *BucketStore) {
	tb.Run("10-series-matched-with-10-label-values", func(tb *testing.B) {
		ms, err := storepb.PromMatchersToMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "label_2", "0"),
			labels.MustNewMatcher(labels.MatchEqual, "label_3", "0"),
		)
		require.NoError(tb, err)

		req := &storepb.LabelValuesRequest{
			Label:    "label_1",
			Start:    timestamp.FromTime(minTime),
			End:      timestamp.FromTime(maxTime),
			Matchers: ms,
		}
		// warmup cache if any
		resp, err := store.LabelValues(ctx, req)
		require.NoError(tb, err)
		assert.Equal(tb, 10, len(resp.Values))

		tb.ResetTimer()
		for i := 0; i < tb.N; i++ {
			resp, err := store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 10, len(resp.Values))
		}
	})

	tb.Run("1000-series-matched-with-1000-label-values", func(tb *testing.B) {
		ms, err := storepb.PromMatchersToMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "label_1", "0"),
			labels.MustNewMatcher(labels.MatchEqual, "label_2", "0"),
		)
		require.NoError(tb, err)

		req := &storepb.LabelValuesRequest{
			Label:    "label_3",
			Start:    timestamp.FromTime(minTime),
			End:      timestamp.FromTime(maxTime),
			Matchers: ms,
		}

		// warmup cache if any
		resp, err := store.LabelValues(ctx, req)
		require.NoError(tb, err)
		assert.Equal(tb, 1000, len(resp.Values))

		tb.ResetTimer()
		for i := 0; i < tb.N; i++ {
			resp, err := store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 1000, len(resp.Values))
		}
	})

	tb.Run("1_000_000-series-matched-with-10-label-values", func(tb *testing.B) {
		ms, err := storepb.PromMatchersToMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "label_0", "0"), // matches all series
		)
		require.NoError(tb, err)

		req := &storepb.LabelValuesRequest{
			Label:    "label_1",
			Start:    timestamp.FromTime(minTime),
			End:      timestamp.FromTime(maxTime),
			Matchers: ms,
		}
		// warmup cache if any
		resp, err := store.LabelValues(ctx, req)
		require.NoError(tb, err)
		assert.Equal(tb, 10, len(resp.Values))

		tb.ResetTimer()
		for i := 0; i < tb.N; i++ {
			resp, err := store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 10, len(resp.Values))
		}
	})

	tb.Run("1_000_000-series-matched-with-1-label-values", func(tb *testing.B) {
		ms, err := storepb.PromMatchersToMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "high_cardinality_label_1", "0"),   // matches a single series
			labels.MustNewMatcher(labels.MatchNotEqual, "high_cardinality_label_0", ""), // matches 1M series
		)
		require.NoError(tb, err)

		req := &storepb.LabelValuesRequest{
			Label:    "high_cardinality_label_0", // there is only 1 value for this label
			Start:    timestamp.FromTime(minTime),
			End:      timestamp.FromTime(maxTime),
			Matchers: ms,
		}
		// warmup cache if any
		resp, err := store.LabelValues(ctx, req)
		require.NoError(tb, err)
		assert.Equal(tb, 1, len(resp.Values))

		tb.ResetTimer()
		for i := 0; i < tb.N; i++ {
			resp, err := store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 1, len(resp.Values))
		}
	})
}

func prefixLabels(prefix string, series []labels.Labels) []labels.Labels {
	prefixed := make([]labels.Labels, len(series))
	b := labels.NewScratchBuilder(2)
	for i := range series {
		b.Reset()
		series[i].Range(func(l labels.Label) {
			b.Add(prefix+l.Name, l.Value)
		})
		prefixed[i] = b.Labels()
	}
	return prefixed
}

// indexCacheMissingLabelValues wraps an IndexCache returning a miss on all FetchLabelValues calls,
// making it useful to benchmark the LabelValues calls (it still caches the underlying postings calls)
type indexCacheMissingLabelValues struct {
	indexcache.IndexCache
}

func (indexCacheMissingLabelValues) FetchLabelValues(context.Context, string, ulid.ULID, string, indexcache.LabelMatchersKey) ([]byte, bool) {
	return nil, false
}

// generateSeries generated series with len(card) labels, each one called label_n,
// with 0 <= n < len(card) and cardinality(label_n) = card[n]
func generateSeries(card []int) []labels.Labels {
	totalSeries := 1
	for _, c := range card {
		totalSeries *= c
	}
	series := make([]labels.Labels, 0, totalSeries)
	current := make([]labels.Label, len(card))
	for idx := range current {
		current[idx].Name = "label_" + strconv.Itoa(idx)
	}

	var rec func(idx int)
	rec = func(lvl int) {
		if lvl == len(card) {
			series = append(series, labels.New(current...))
			return
		}

		for i := 0; i < card[lvl]; i++ {
			current[lvl].Value = strconv.Itoa(i)
			rec(lvl + 1)
		}
	}
	rec(0)

	return series
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestTimeoutGate_CancellationRace(t *testing.T) {
	gate := timeoutGate{
		delegate: alwaysSuccessfulAfterDelayGate{time.Second},
		timeout:  time.Nanosecond,
	}

	err := gate.Start(context.Background())
	require.NoError(t, err, "must not return failure if delegated gate returns success even after timeout expires")
}

type alwaysSuccessfulAfterDelayGate struct {
	delay time.Duration
}

func (a alwaysSuccessfulAfterDelayGate) Start(_ context.Context) error {
	<-time.After(a.delay)
	return nil
}

func (a alwaysSuccessfulAfterDelayGate) Done() {}
