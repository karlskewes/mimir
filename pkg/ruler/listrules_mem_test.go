// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"hash"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/gziphandler"
)

// listRulesPerNamespace writes the same body as marshalAndSend(rgs.Formatted(), ...).
// It formats and marshals one namespace at a time.
// The sort order matches yaml.v3 map key order only for the fixture namespace names.
func listRulesPerNamespace(rgs rulespb.RuleGroupList, w http.ResponseWriter) error {
	byNamespace := map[string]rulespb.RuleGroupList{}
	for _, rg := range rgs {
		byNamespace[rg.Namespace] = append(byNamespace[rg.Namespace], rg)
	}
	w.Header().Set("Content-Type", "application/yaml")
	for _, ns := range slices.Sorted(maps.Keys(byNamespace)) {
		var output interface{} = byNamespace[ns].Formatted()
		d, err := yaml.Marshal(&output)
		if err != nil {
			return err
		}
		if _, err := w.Write(d); err != nil {
			return err
		}
	}
	return nil
}

func TestListRulesPerNamespaceMatchesMarshal(t *testing.T) {
	rgs := listRulesBenchFixture()

	want := httptest.NewRecorder()
	marshalAndSend(rgs.Formatted(), want, log.NewNopLogger())

	got := httptest.NewRecorder()
	require.NoError(t, listRulesPerNamespace(rgs, got))
	require.True(t, bytes.Equal(want.Body.Bytes(), got.Body.Bytes()), "per-namespace body differs from marshalAndSend body")
}

// countingResponseWriter counts and hashes the bytes written through it.
type countingResponseWriter struct {
	http.ResponseWriter
	n int
	h hash.Hash
}

func (w *countingResponseWriter) Write(b []byte) (int, error) {
	w.n += len(b)
	if w.h != nil {
		w.h.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

// discardResponseWriter drops the body.
type discardResponseWriter struct{ header http.Header }

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}

func heapObjectsBytes() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// procStatusBytes returns a "kB" field of /proc/self/status in bytes.
func procStatusBytes(t *testing.T, field string) uint64 {
	b, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, field+":"); ok {
			kb, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			require.NoError(t, err)
			return kb * 1024
		}
	}
	t.Fatalf("field %s not found in /proc/self/status", field)
	return 0
}

// TestListRulesEncodeMemory measures the heap of one ListRules encode.
// It runs only when LISTRULES_MEM_SHAPE is "<namespaces>,<groups>,<rules>".
// Run one subtest per process.
// Otherwise memory that the runtime keeps from one subtest hides the RSS growth of the next.
func TestListRulesEncodeMemory(t *testing.T) {
	shape := os.Getenv("LISTRULES_MEM_SHAPE")
	if shape == "" {
		t.Skip("set LISTRULES_MEM_SHAPE=<namespaces>,<groups>,<rules>")
	}
	var numNamespaces, numGroups, numRules int
	_, err := fmt.Sscanf(shape, "%d,%d,%d", &numNamespaces, &numGroups, &numRules)
	require.NoError(t, err)

	runtime.GC()
	before := heapObjectsBytes()
	rgs := listRulesFixture(numNamespaces, numGroups, numRules)
	runtime.GC()
	rgsHeap := heapObjectsBytes() - before

	before = heapObjectsBytes()
	formatted := rgs.Formatted()
	runtime.GC()
	formattedHeap := heapObjectsBytes() - before
	runtime.KeepAlive(formatted)
	formatted = nil
	t.Logf("shape namespaces=%d groups=%d rules=%d rgs_heap=%d formatted_heap=%d", numNamespaces, len(rgs), numRules, rgsHeap, formattedHeap)

	logger := log.NewNopLogger()
	variants := []struct {
		name   string
		encode func(w http.ResponseWriter) error
	}{
		{"whole", func(w http.ResponseWriter) error { marshalAndSend(rgs.Formatted(), w, logger); return nil }},
		{"per_namespace", func(w http.ResponseWriter) error { return listRulesPerNamespace(rgs, w) }},
	}
	for _, v := range variants {
		for _, gogc := range []int{100, 10} {
			t.Run(fmt.Sprintf("%s_gogc%d", v.name, gogc), func(t *testing.T) {
				var raw *countingResponseWriter
				var encodeErr error
				handler := gziphandler.GzipHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					raw = &countingResponseWriter{ResponseWriter: w, h: sha256.New()}
					encodeErr = v.encode(raw)
				}), gzip.DefaultCompression)
				req := httptest.NewRequest(http.MethodGet, "/prometheus/config/v1/rules", nil)
				req.Header.Set("Accept-Encoding", "gzip")
				out := &countingResponseWriter{ResponseWriter: &discardResponseWriter{header: http.Header{}}}

				prevGOGC := debug.SetGCPercent(gogc)
				defer debug.SetGCPercent(prevGOGC)
				debug.FreeOSMemory()

				rssTracked := os.WriteFile("/proc/self/clear_refs", []byte("5"), 0) == nil
				var rssBefore uint64
				if rssTracked {
					rssBefore = procStatusBytes(t, "VmRSS")
				}
				var msBefore runtime.MemStats
				runtime.ReadMemStats(&msBefore)
				heapBefore := heapObjectsBytes()

				stop := make(chan struct{})
				peakCh := make(chan uint64)
				go func() {
					s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
					var peak uint64
					tick := time.NewTicker(time.Millisecond)
					defer tick.Stop()
					for {
						metrics.Read(s)
						peak = max(peak, s[0].Value.Uint64())
						select {
						case <-stop:
							metrics.Read(s)
							peakCh <- max(peak, s[0].Value.Uint64())
							return
						case <-tick.C:
						}
					}
				}()

				start := time.Now()
				handler.ServeHTTP(out, req)
				elapsed := time.Since(start)
				close(stop)
				peak := <-peakCh

				var msAfter runtime.MemStats
				runtime.ReadMemStats(&msAfter)
				var rssPeakDelta int64
				if rssTracked {
					rssPeakDelta = int64(procStatusBytes(t, "VmHWM")) - int64(rssBefore)
				}

				require.NoError(t, encodeErr)
				require.Equal(t, "gzip", out.Header().Get("Content-Encoding"))
				t.Logf("result variant=%s gogc=%d groups=%d rules=%d total_alloc=%d heap_before=%d peak_heap=%d peak_heap_delta=%d rss_tracked=%t peak_rss_delta=%d num_gc=%d raw_bytes=%d gzip_bytes=%d raw_sha256=%x elapsed=%s",
					v.name, gogc, len(rgs), numRules,
					msAfter.TotalAlloc-msBefore.TotalAlloc, heapBefore, peak, int64(peak)-int64(heapBefore),
					rssTracked, rssPeakDelta, msAfter.NumGC-msBefore.NumGC,
					raw.n, out.n, raw.h.Sum(nil), elapsed)
			})
		}
	}
	runtime.KeepAlive(rgs)
}
