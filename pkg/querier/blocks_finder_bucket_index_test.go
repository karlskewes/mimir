// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_finder_bucket_index_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
)

func TestBucketIndexBlocksFinder_GetBlocks(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(t)

	// Mock a bucket index.
	block1 := &bucketindex.Block{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 15}
	block2 := &bucketindex.Block{ID: ulid.MustNew(2, nil), MinTime: 12, MaxTime: 20}
	block3 := &bucketindex.Block{ID: ulid.MustNew(3, nil), MinTime: 20, MaxTime: 30}
	block4 := &bucketindex.Block{ID: ulid.MustNew(4, nil), MinTime: 30, MaxTime: 40}
	block5 := &bucketindex.Block{ID: ulid.MustNew(5, nil), MinTime: 30, MaxTime: 40} // Time range overlaps with block4, but this block deletion mark is above the threshold.
	mark3 := &bucketindex.BlockDeletionMark{ID: block3.ID, DeletionTime: time.Now().Unix()}
	mark5 := &bucketindex.BlockDeletionMark{ID: block5.ID, DeletionTime: time.Now().Add(-2 * time.Hour).Unix()}

	idx := &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             bucketindex.Blocks{block1, block2, block3, block4, block5},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{mark3, mark5},
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))

	finder := prepareBucketIndexBlocksFinder(t, bkt)

	tests := map[string]struct {
		minT           int64
		maxT           int64
		expectedBlocks bucketindex.Blocks
	}{
		"no matching block because the range is too low": {
			minT: 0,
			maxT: 5,
		},
		"no matching block because the range is too high": {
			minT: 50,
			maxT: 60,
		},
		"matching all blocks": {
			minT:           0,
			maxT:           60,
			expectedBlocks: bucketindex.Blocks{block4, block3, block2, block1},
		},
		"query range starting at a block maxT": {
			minT:           block3.MaxTime,
			maxT:           60,
			expectedBlocks: bucketindex.Blocks{block4},
		},
		"query range ending at a block minT": {
			minT:           block3.MinTime,
			maxT:           block4.MinTime,
			expectedBlocks: bucketindex.Blocks{block4, block3},
		},
		"query range within a single block": {
			minT:           block3.MinTime + 2,
			maxT:           block3.MaxTime - 2,
			expectedBlocks: bucketindex.Blocks{block3},
		},
		"query range within multiple blocks": {
			minT:           13,
			maxT:           16,
			expectedBlocks: bucketindex.Blocks{block2, block1},
		},
		"query range matching exactly a single block": {
			minT:           block3.MinTime,
			maxT:           block3.MaxTime - 1,
			expectedBlocks: bucketindex.Blocks{block3},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			blocks, meta, err := finder.GetBlocks(ctx, userID, testData.minT, testData.maxT)
			require.NoError(t, err)
			require.ElementsMatch(t, testData.expectedBlocks, blocks)
			require.Equal(t, idx.Metadata(), meta)
		})
	}
}

// TestBucketIndexBlocksFinder_GetBlocks_DeletionMarkAging checks, at several simulated deletion-mark
// ages, which blocks GetBlocks still returns relative to IgnoreDeletionMarksDelay set to the
// production default for IgnoreDeletionMarksWhileQueryingDelay (50m).
func TestBucketIndexBlocksFinder_GetBlocks_DeletionMarkAging(t *testing.T) {
	const userID = "user-1"
	const blockMinT, blockMaxT = 0, 10000

	ctx := context.Background()
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(t)

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

	// A block with no deletion mark, covering the same time range as the aged blocks below, to
	// confirm it's always returned regardless of the others' ages.
	permanentBlock := &bucketindex.Block{ID: ulid.MustNew(1, nil), MinTime: blockMinT, MaxTime: blockMaxT}
	blocks := bucketindex.Blocks{permanentBlock}
	ageBlocks := make([]*bucketindex.Block, len(ages))
	var marks bucketindex.BlockDeletionMarks

	now := time.Now()
	for i, a := range ages {
		b := &bucketindex.Block{ID: ulid.MustNew(uint64(i+2), nil), MinTime: blockMinT, MaxTime: blockMaxT}
		blocks = append(blocks, b)
		ageBlocks[i] = b
		marks = append(marks, &bucketindex.BlockDeletionMark{ID: b.ID, DeletionTime: now.Add(-a.age).Unix()})
	}

	idx := &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             blocks,
		BlockDeletionMarks: marks,
		UpdatedAt:          time.Now().Unix(),
	}
	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))

	cfg := BucketIndexBlocksFinderConfig{
		IndexLoader: bucketindex.LoaderConfig{
			CheckInterval:         time.Minute,
			UpdateOnStaleInterval: time.Minute,
			UpdateOnErrorInterval: time.Minute,
			IdleTimeout:           time.Minute,
		},
		MaxStalePeriod:           time.Hour,
		IgnoreDeletionMarksDelay: 50 * time.Minute, // matches the IgnoreDeletionMarksWhileQueryingDelay production default
	}
	finder := NewBucketIndexBlocksFinder(cfg, bkt, nil, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, finder))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, finder))
	})

	returned, _, err := finder.GetBlocks(ctx, userID, blockMinT, blockMaxT)
	require.NoError(t, err)

	present := make(map[ulid.ULID]bool, len(returned))
	for _, b := range returned {
		present[b.ID] = true
	}

	assert.True(t, present[permanentBlock.ID], "block without a deletion mark should always be returned")

	for i, a := range ages {
		switch a.name {
		case "50m":
			// Deletion mark age equals the delay exactly (GetBlocks excludes a block only once age
			// strictly exceeds the delay). Real elapsed time between writing the mark and GetBlocks
			// evaluating it makes this boundary non-deterministic, so it's reported but not asserted.
			t.Logf("age=50m boundary: block %s returned=%v (informational only)", ageBlocks[i].ID, present[ageBlocks[i].ID])
		case "60m", "70m":
			assert.False(t, present[ageBlocks[i].ID], "block should be excluded at age=%s (past the 50m delay)", a.name)
		default:
			assert.True(t, present[ageBlocks[i].ID], "block should still be returned at age=%s (within the 50m delay)", a.name)
		}
	}
}

func BenchmarkBucketIndexBlocksFinder_GetBlocks(b *testing.B) {
	const (
		numBlocks        = 1000
		numDeletionMarks = 100
		userID           = "user-1"
	)

	ctx := context.Background()
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(b)

	// Mock a bucket index.
	idx := &bucketindex.Index{
		Version:   bucketindex.IndexVersion1,
		UpdatedAt: time.Now().Unix(),
	}

	for i := 1; i <= numBlocks; i++ {
		id := ulid.MustNew(uint64(i), nil)
		minT := int64(i * 10)
		maxT := int64((i + 1) * 10)
		idx.Blocks = append(idx.Blocks, &bucketindex.Block{ID: id, MinTime: minT, MaxTime: maxT})
	}
	for i := 1; i <= numDeletionMarks; i++ {
		id := ulid.MustNew(uint64(i), nil)
		idx.BlockDeletionMarks = append(idx.BlockDeletionMarks, &bucketindex.BlockDeletionMark{ID: id, DeletionTime: time.Now().Unix()})
	}
	require.NoError(b, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))
	finder := prepareBucketIndexBlocksFinder(b, bkt)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		blocks, _, err := finder.GetBlocks(ctx, userID, 100, 200)
		if err != nil || len(blocks) != 11 {
			b.Fail()
		}
	}
}

func TestBucketIndexBlocksFinder_GetBlocks_BucketIndexDoesNotExist(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	blocks, meta, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.NoError(t, err)
	assert.Nil(t, meta)
	assert.Empty(t, blocks)
}

func TestBucketIndexBlocksFinder_GetBlocks_BucketIndexIsCorrupted(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	// Upload a corrupted bucket index.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, bucketindex.IndexCompressedFilename), strings.NewReader("invalid}!")))

	_, _, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.ErrorIs(t, err, bucketindex.ErrIndexCorrupted)
}

func TestBucketIndexBlocksFinder_GetBlocks_BucketIndexIsTooOld(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	idx := &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             bucketindex.Blocks{},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{},
		UpdatedAt:          time.Now().Add(-2 * time.Hour).Unix(),
	}
	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))

	_, _, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.EqualError(t, err, newBucketIndexTooOldError(idx.GetUpdatedAt(), finder.cfg.MaxStalePeriod).Error())
}

func prepareBucketIndexBlocksFinder(t testing.TB, bkt objstore.Bucket) *BucketIndexBlocksFinder {
	ctx := context.Background()
	cfg := BucketIndexBlocksFinderConfig{
		IndexLoader: bucketindex.LoaderConfig{
			CheckInterval:         time.Minute,
			UpdateOnStaleInterval: time.Minute,
			UpdateOnErrorInterval: time.Minute,
			IdleTimeout:           time.Minute,
		},
		MaxStalePeriod:           time.Hour,
		IgnoreDeletionMarksDelay: time.Hour,
	}

	finder := NewBucketIndexBlocksFinder(cfg, bkt, nil, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, finder))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, finder))
	})

	return finder
}

func TestBlocksFinderBucketIndexErrMsgs(t *testing.T) {
	tests := map[string]struct {
		err error
		msg string
	}{
		"newBucketIndexTooOldError": {
			err: newBucketIndexTooOldError(time.Unix(1000000000, 0), time.Hour),
			msg: `the bucket index is too old. It was last updated at 2001-09-09T01:46:40Z, which exceeds the maximum allowed staleness period of 1h0m0s (err-mimir-bucket-index-too-old)`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.msg, tc.err.Error())
		})
	}
}
