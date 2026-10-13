package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Scenarios = map[string]Policy{
	"S0":   {Name: "S0", TTLJitterLo: 1, Workers: 10},
	"S0ns": {Name: "S0ns", TTLJitterLo: 1, Workers: 10}, // sync off
	"S1a":  {Name: "S1a", TTLJitterLo: 0.8, Workers: 10},
	"S1b":  {Name: "S1b", TTLJitterLo: 0.5, Workers: 10},
	"S2":   {Name: "S2", TTLJitterLo: 1, Refresh: true, RefreshFrac: 0.9, Workers: 10},
	"S2b":  {Name: "S2b", TTLJitterLo: 1, Refresh: true, RefreshFrac: 0.9, RefreshPool: 10, Workers: 10},
	"S3":   {Name: "S3", TTLJitterLo: 1, Workers: 40},
	"S4":   {Name: "S4", TTLJitterLo: 1, ExtendOnHit: true, Workers: 10},
	"S5":   {Name: "S5", TTLJitterLo: 1, Refresh: true, RefreshFrac: 0.9, RefreshPool: 10, Workers: 40}, // extra: S2b + S3
}

const (
	H       = 3600.0
	ColdEnd = 48 * H
	// Warm mode: 24 h cold warm-up, then a replay of the server events seen on 2026-10-12,
	// then the policy switch and 48 h with two more server events.
	WarmSw  = 48 * H
	WarmEnd = 96 * H
	ValFrom = 24*H + 21*H // 21:00 on the replay day
	ValTo   = 24*H + 23*H
)

func hms(h, m, sec float64) float64 { return h*H + m*60 + sec }

func warmEvents(seed uint64) []Event {
	r := rand.New(rand.NewPCG(seed, 99))
	perm := r.Perm(Shards)
	d := 24 * H
	return []Event{
		{T: d + hms(11, 25, 0), Shards: []int{perm[0]}, Label: "replace"},
		{T: d + hms(11, 50, 0), Shards: []int{perm[1]}, Label: "replace"},
		{T: d + hms(12, 28, 0), Shards: []int{perm[2]}, Label: "replace"},
		{T: d + hms(12, 54, 0), Shards: []int{perm[3]}, Label: "replace"},
		{T: d + hms(15, 27, 0), Shards: []int{perm[4]}, Label: "replace"},
		{T: d + hms(21, 32, 42), Shards: []int{perm[4], perm[5]}, Label: "reorder"},
		// After the policy switch.
		{T: WarmSw + 12*H, Shards: []int{perm[0]}, Label: "replace"},
		{T: WarmSw + 30*H, Shards: []int{perm[1], perm[2]}, Label: "reorder"},
	}
}

func makeConfig(scn, mode, arr string, seed uint64, out string, d1 bool) Config {
	cfg := Config{Scenario: scn, Mode: mode, Arrival: arr, Seed: seed, Policy: Scenarios[scn], Sync: scn != "S0ns", D1: d1}
	if mode == "cold" {
		cfg.End = ColdEnd
	} else {
		cfg.SwitchAt = WarmSw
		cfg.End = WarmEnd
		cfg.Events = warmEvents(seed)
	}
	cfg.Out = filepath.Join(out, fmt.Sprintf("%s_%s_%s_s%d", scn, mode, arr, seed))
	return cfg
}

func runOne(cfg Config) (*Summary, error) {
	s := NewSim(cfg)
	if cfg.SwitchAt <= 0 {
		for _, p := range s.pools {
			p.max = cfg.Policy.RefreshPool
		}
	}
	t0 := time.Now()
	s.Run()
	sum := summarize(s)
	sum.WallSec = time.Since(t0).Seconds()
	if err := writeRun(s, sum); err != nil {
		return nil, err
	}
	return sum, nil
}

func main() {
	out := flag.String("out", "out", "output directory")
	scn := flag.String("scn", "S0", "comma-separated scenarios, or all")
	modes := flag.String("mode", "cold", "comma-separated: cold,warm")
	arrs := flag.String("arr", "poisson", "comma-separated: poisson,fixed")
	seeds := flag.Int("seeds", 1, "number of seeds")
	par := flag.Int("par", runtime.NumCPU()-2, "parallel runs")
	analyzeOnly := flag.Bool("analyze", false, "only run the analysis")
	flag.StringVar(&ProdDir, "prod", "", "directory with the production query_range JSON files, for -analyze")
	flag.Parse()
	if *analyzeOnly {
		if err := analyze(*out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	var scns []string
	if *scn == "all" {
		for k := range Scenarios {
			scns = append(scns, k)
		}
		sort.Strings(scns)
	} else {
		scns = strings.Split(*scn, ",")
	}
	var cfgs []Config
	for _, sc := range scns {
		for _, m := range strings.Split(*modes, ",") {
			for _, a := range strings.Split(*arrs, ",") {
				for sd := 1; sd <= *seeds; sd++ {
					d1 := sd == 1
					cfgs = append(cfgs, makeConfig(sc, m, a, uint64(sd), *out, d1))
				}
			}
		}
	}
	ch := make(chan Config)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < *par; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cfg := range ch {
				sum, err := runOne(cfg)
				mu.Lock()
				if err != nil {
					fmt.Fprintln(os.Stderr, cfg.Out, err)
				} else {
					fmt.Printf("%-28s wall=%5.1fs lr_p50=%6.1f lr_p99=%6.1f miss/h=%7.0f ns=%.3f gapA=%.2f denB=%.2f bursts/615=%.2f acf=%.0f/%.2f\n",
						filepath.Base(cfg.Out), sum.WallSec, sum.DurP50, sum.DurP99, sum.MissPerH, sum.NotStored, sum.GapMean, sum.DenMean, sum.BurstsPer615, sum.ACFLag, sum.ACFr)
				}
				mu.Unlock()
			}
		}()
	}
	for _, c := range cfgs {
		ch <- c
	}
	close(ch)
	wg.Wait()
}

// noNaN replaces NaN floats with -1, because encoding/json rejects NaN.
// -1 marks "no value" in summary.json.
func noNaN(v reflect.Value) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			noNaN(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanSet() || v.Field(i).Kind() != reflect.Float64 {
				noNaN(v.Field(i))
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			noNaN(v.Index(i))
		}
	case reflect.Float64:
		if v.CanSet() && math.IsNaN(v.Float()) {
			v.SetFloat(-1)
		}
	}
}

func writeJSON(path string, v any) error {
	noNaN(reflect.ValueOf(v))
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func writeRun(s *Sim, sum *Summary) error {
	dir := s.cfg.Out
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "summary.json"), sum); err != nil {
		return err
	}
	// 15 s series.
	var sb strings.Builder
	sb.WriteString("t,lookups,hits,miss_list,miss_sync,gets,ref_gets,adds,not_stored,miss_rate1m\n")
	b := s.b
	for t0 := 0; t0+15 <= s.nsec-2; t0 += 15 {
		var l, h, ml, ms, g, rg, a, ns int64
		for i := t0; i < t0+15; i++ {
			l += int64(b.lookups[i])
			h += int64(b.hits[i])
			ml += int64(b.missL[i])
			ms += int64(b.missS[i])
			g += int64(b.gets[i])
			rg += int64(b.refGets[i])
			a += int64(b.adds[i])
			ns += int64(b.ns[i])
		}
		fmt.Fprintf(&sb, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%.2f\n", t0+15, l, h, ml, ms, g, rg, a, ns, rate1m(s, t0+15))
	}
	if err := os.WriteFile(filepath.Join(dir, "series15s.csv"), []byte(sb.String()), 0o644); err != nil {
		return err
	}
	sb.Reset()
	sb.WriteString("id,arrive,start,end,dur,hits,misses\n")
	for _, r := range s.ReqLog {
		fmt.Fprintf(&sb, "%d,%.3f,%.3f,%.3f,%.3f,%d,%d\n", r.ID, r.Arrive, r.Start, r.End, r.End-r.Start, r.Hits, r.Misses)
	}
	if err := os.WriteFile(filepath.Join(dir, "requests.csv"), []byte(sb.String()), 0o644); err != nil {
		return err
	}
	sb.Reset()
	sb.WriteString("t,live,gap_count,gap_top1,gap_top2,gap_top3,den_count,peak30,comp1,comp2,comp3,runs\n")
	for _, c := range s.Cohorts {
		fmt.Fprintf(&sb, "%.0f,%d,%d,%d,%d,%d,%d,%.4f", c.T, c.Live, c.GapCount, c.GapTop[0], c.GapTop[1], c.GapTop[2], c.DenCount, c.Peak30)
		for r := 0; r < 3; r++ {
			var parts []string
			for _, v := range c.Comp[r] {
				parts = append(parts, fmt.Sprint(v))
			}
			sb.WriteString("," + strings.Join(parts, " "))
		}
		var rs []string
		for _, v := range c.Runs {
			rs = append(rs, strconv.FormatFloat(v, 'f', 1, 64))
		}
		sb.WriteString("," + strings.Join(rs, " ") + "\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "cohorts.csv"), []byte(sb.String()), 0o644); err != nil {
		return err
	}
	if s.cfg.D1 {
		rows := map[string]any{"t": s.RowT, "phase": s.RowPh, "writer": s.RowWr, "facet": s.RowFacet, "facetBins": FacetBins, "events": s.cfg.Events, "switchAt": s.cfg.SwitchAt}
		b, err := json.Marshal(rows)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "d1rows.json"), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// rate1m mimics Prometheus rate(...[1m]) of the miss counter at time t (s).
func rate1m(s *Sim, t int) float64 {
	sum := 0
	for i := t - 60; i < t; i++ {
		if i >= 0 && i < s.nsec {
			sum += int(s.b.missL[i]) + int(s.b.missS[i])
		}
	}
	return float64(sum) / 60
}

func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	i := int(math.Ceil(p*float64(len(c)))) - 1
	if i < 0 {
		i = 0
	}
	return c[i]
}
