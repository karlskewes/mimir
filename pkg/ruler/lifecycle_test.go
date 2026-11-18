// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/lifecycle_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
)

// TestRulerShutdown tests shutting down ruler unregisters correctly
func TestRulerShutdown(t *testing.T) {
	ctx := context.Background()

	config := defaultRulerConfig(t)
	r := prepareRuler(t, config, newMockRuleStore(mockRules))

	kvStore := config.Ring.Common.KVStore.Mock

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

	// Wait until the tokens are registered in the ring
	test.Poll(t, 100*time.Millisecond, config.Ring.NumTokens, func() interface{} {
		return numTokens(kvStore, "localhost", RulerRingKey)
	})

	require.Equal(t, ring.ACTIVE, r.lifecycler.GetState())

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r))

	// Wait until the tokens are unregistered from the ring
	test.Poll(t, 100*time.Millisecond, 0, func() interface{} {
		return numTokens(kvStore, "localhost", RulerRingKey)
	})
}

func TestRulerShutdown_ShouldLeaveRingBeforeStoppingRuleEvaluation(t *testing.T) {
	tests := map[string]struct {
		handoverPeriod     time.Duration
		handoverPeriodAuto bool
		syncDuration       time.Duration
		expectedHandover   time.Duration
	}{
		"without handover period": {},
		"with handover period": {
			handoverPeriod:   30 * time.Second,
			expectedHandover: 30 * time.Second,
		},
		"with estimated handover period": {
			handoverPeriod:     time.Minute,
			handoverPeriodAuto: true,
			syncDuration:       10 * time.Second,
			// 1.2 times the ring check period of 5s, plus 2 syncs.
			expectedHandover: 26 * time.Second,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()

				cfg := defaultRulerConfig(t)
				cfg.ShutdownHandoverPeriod = tc.handoverPeriod
				cfg.ShutdownHandoverPeriodAuto = tc.handoverPeriodAuto
				r := prepareRuler(t, cfg, newMockRuleStore(mockRules))

				kvStore := cfg.Ring.Common.KVStore.Mock

				var (
					managerStoppedAt    time.Time
					tokensAtManagerStop int
				)
				r.manager = &onStopManager{
					MultiTenantManager: r.manager,
					onStop: func() {
						managerStoppedAt = time.Now()
						tokensAtManagerStop = numTokens(kvStore, "localhost", RulerRingKey)
					},
				}

				require.NoError(t, services.StartAndAwaitRunning(ctx, r))
				defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

				// Wait until the tokens are registered in the ring.
				test.Poll(t, 100*time.Millisecond, cfg.Ring.NumTokens, func() interface{} {
					return numTokens(kvStore, "localhost", RulerRingKey)
				})

				// Record the sync just before the stop.
				// Then no background sync can replace it in the last syncs.
				r.fullSyncDurations.add(tc.syncDuration)

				stopStartedAt := time.Now()
				require.NoError(t, services.StopAndAwaitTerminated(ctx, r))

				require.Equal(t, 0, tokensAtManagerStop, "the ruler should leave the ring before it stops evaluating rule groups")

				// The bubble has a fake clock, so the wait is the handover period plus only a little more.
				waited := managerStoppedAt.Sub(stopStartedAt)
				require.GreaterOrEqual(t, waited, tc.expectedHandover, "the ruler should keep evaluating rule groups for the handover period after leaving the ring")
				require.Less(t, waited, tc.expectedHandover+time.Second, "the ruler should stop evaluating rule groups when the handover period ends")
			})
		})
	}
}

func TestRuler_ShutdownHandoverPeriod(t *testing.T) {
	tests := map[string]struct {
		auto          bool
		debounce      time.Duration
		syncDurations []time.Duration
		expected      time.Duration
	}{
		"fixed period": {
			syncDurations: []time.Duration{10 * time.Second},
			expected:      time.Minute,
		},
		"maximum before the first full sync": {
			auto:     true,
			expected: time.Minute,
		},
		"estimate from the longest of the last syncs": {
			auto:          true,
			syncDurations: []time.Duration{4 * time.Second, 10 * time.Second, 7 * time.Second},
			expected:      26 * time.Second,
		},
		"estimate from the last 3 syncs only": {
			auto:          true,
			syncDurations: []time.Duration{40 * time.Second, time.Second, 2 * time.Second, 3 * time.Second},
			expected:      12 * time.Second,
		},
		"estimate with a ring change debounce": {
			auto:          true,
			debounce:      5 * time.Second,
			syncDurations: []time.Duration{10 * time.Second},
			expected:      41 * time.Second,
		},
		"estimate up to the fixed period": {
			auto:          true,
			syncDurations: []time.Duration{40 * time.Second},
			expected:      time.Minute,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := &Ruler{cfg: Config{
				RingCheckPeriod:            5 * time.Second,
				RingChangeDebounce:         tc.debounce,
				RingChangeMaxDebounce:      15 * time.Second,
				ShutdownHandoverPeriod:     time.Minute,
				ShutdownHandoverPeriodAuto: tc.auto,
			}}
			for _, d := range tc.syncDurations {
				r.fullSyncDurations.add(d)
			}

			require.Equal(t, tc.expected, r.shutdownHandoverPeriod())
		})
	}
}

func TestRuler_SyncRules_ShouldRecordOnlyCompletedFullSyncDurations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := defaultRulerConfig(t)
		// No sync runs in the background during the test.
		cfg.PollInterval = time.Hour
		cfg.RingCheckPeriod = time.Hour
		store := &slowRuleStore{RuleStore: newMockRuleStore(mockRules), delay: 40 * time.Second}
		r := prepareRuler(t, cfg, store, withStart())

		// The estimate does not use the initial sync, which ran on start.
		_, recorded := r.fullSyncDurations.longest()
		require.Zero(t, recorded)

		store.delay = 7 * time.Second
		require.NoError(t, r.syncRules(t.Context(), nil, rulerSyncReasonPeriodic, true))

		// A partial sync after a config change does not show how long a full sync takes.
		store.delay = 30 * time.Second
		require.NoError(t, r.syncRules(t.Context(), []string{"user1"}, rulerSyncReasonAPIChange, false))

		// A failed sync stops early.
		store.err = errors.New("store failure")
		require.ErrorIs(t, r.syncRules(t.Context(), nil, rulerSyncReasonRingChange, true), store.err)
		store.err = nil

		// A sync that the shutdown cancels stops early.
		ctx, cancel := context.WithCancel(t.Context())
		store.onLoad = cancel
		require.NoError(t, r.syncRules(ctx, nil, rulerSyncReasonRingChange, true))

		longest, recorded := r.fullSyncDurations.longest()
		require.Equal(t, 7*time.Second, longest)
		require.Equal(t, 1, recorded)
	})
}

// slowRuleStore waits before it loads rule groups, then optionally fails or calls onLoad.
type slowRuleStore struct {
	rulestore.RuleStore
	delay  time.Duration
	err    error
	onLoad func()
}

func (s *slowRuleStore) LoadRuleGroups(ctx context.Context, groups map[string]rulespb.RuleGroupList) (rulespb.RuleGroupList, error) {
	time.Sleep(s.delay)
	if s.err != nil {
		return nil, s.err
	}
	if s.onLoad != nil {
		s.onLoad()
	}
	return s.RuleStore.LoadRuleGroups(ctx, groups)
}

// onStopManager is a MultiTenantManager that calls onStop before stopping the wrapped manager.
type onStopManager struct {
	MultiTenantManager
	onStop func()
}

func (m *onStopManager) Stop() {
	m.onStop()
	m.MultiTenantManager.Stop()
}

func TestRuler_RingLifecyclerShouldAutoForgetUnhealthyInstances(t *testing.T) {
	const unhealthyInstanceID = "unhealthy-id"
	const heartbeatTimeout = time.Minute

	ctx := context.Background()
	cfg := defaultRulerConfig(t)
	cfg.Ring.Common.HeartbeatPeriod = 100 * time.Millisecond
	cfg.Ring.Common.HeartbeatTimeout = heartbeatTimeout
	cfg.Ring.AutoForgetUnhealthyPeriods = 10

	r := prepareRuler(t, cfg, newMockRuleStore(mockRules))

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

	ringClient := cfg.Ring.Common.KVStore.Mock

	// Add an unhealthy instance to the ring.
	require.NoError(t, ringClient.CAS(ctx, RulerRingKey, func(in interface{}) (interface{}, bool, error) {
		ringDesc := ring.GetOrCreateRingDesc(in)

		instance := ringDesc.AddIngester(unhealthyInstanceID, "1.1.1.1", "", generateSortedTokens(cfg.Ring.NumTokens), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		instance.Timestamp = time.Now().Add(-time.Duration(cfg.Ring.AutoForgetUnhealthyPeriods+1) * heartbeatTimeout).Unix()
		ringDesc.Ingesters[unhealthyInstanceID] = instance

		return ringDesc, true, nil
	}))

	// Ensure the unhealthy instance is removed from the ring.
	test.Poll(t, time.Second*5, false, func() interface{} {
		d, err := ringClient.Get(ctx, RulerRingKey)
		if err != nil {
			return err
		}

		_, ok := ring.GetOrCreateRingDesc(d).Ingesters[unhealthyInstanceID]
		return ok
	})
}

func generateSortedTokens(numTokens int) ring.Tokens {
	tokens := ring.NewRandomTokenGenerator().GenerateTokens(numTokens, nil)

	// Ensure generated tokens are sorted.
	slices.SortFunc(tokens, func(a, b uint32) int {
		return cmp.Compare(a, b)
	})

	return tokens
}

// numTokens determines the number of tokens owned by the specified
// address
func numTokens(c kv.Client, name, ringKey string) int {
	ringDesc, err := c.Get(context.Background(), ringKey)

	// The ringDesc may be null if the lifecycler hasn't stored the ring
	// to the KVStore yet.
	if ringDesc == nil || err != nil {
		return 0
	}
	rd := ringDesc.(*ring.Desc)
	return len(rd.Ingesters[name].Tokens)
}
