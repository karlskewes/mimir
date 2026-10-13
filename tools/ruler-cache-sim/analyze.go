package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ProdDir holds the production query_range JSON files that -analyze reads.
var ProdDir string

type runData struct {
	sum  Summary
	dir  string
	durs []float64
	miss []float64
}

func loadRuns(out string) ([]*runData, error) {
	dirs, err := filepath.Glob(filepath.Join(out, "*", "summary.json"))
	if err != nil {
		return nil, err
	}
	var runs []*runData
	for _, f := range dirs {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		rd := &runData{dir: filepath.Dir(f)}
		if err := json.Unmarshal(b, &rd.sum); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		reqs, err := readRequests(rd.dir)
		if err != nil {
			return nil, err
		}
		for _, r := range reqs {
			if r.Start >= rd.sum.From && r.End < rd.sum.To {
				rd.durs = append(rd.durs, r.End-r.Start)
				rd.miss = append(rd.miss, float64(r.Misses))
			}
		}
		runs = append(runs, rd)
	}
	return runs, nil
}

func readRequests(dir string) ([]ReqRec, error) {
	f, err := os.Open(filepath.Join(dir, "requests.csv"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []ReqRec
	sc := bufio.NewScanner(f)
	sc.Scan()
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		st, _ := strconv.ParseFloat(p[2], 64)
		en, _ := strconv.ParseFloat(p[3], 64)
		mi, _ := strconv.Atoi(p[6])
		out = append(out, ReqRec{Start: st, End: en, Misses: int32(mi)})
	}
	return out, sc.Err()
}

func readCSV(path string) ([]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	sc.Scan()
	hdr := strings.Split(sc.Text(), ",")
	var rows []map[string]string
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		m := map[string]string{}
		for i, h := range hdr {
			if i < len(p) {
				m[h] = p[i]
			}
		}
		rows = append(rows, m)
	}
	return rows, sc.Err()
}

func atof(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }

var scnOrder = []string{"S0", "S0ns", "S1a", "S1b", "S2", "S2b", "S3", "S5", "S4"}

var scnDesc = map[string]string{
	"S0":   "baseline, TTL 600 s",
	"S0ns": "baseline, rule sync off",
	"S1a":  "TTL x U(0.8, 1.0)",
	"S1b":  "TTL x U(0.5, 1.0)",
	"S2":   "refresh-ahead at 0.9 TTL, unbounded",
	"S2b":  "refresh-ahead at 0.9 TTL, 10 workers per pod",
	"S3":   "40 workers",
	"S4":   "extend TTL on hit (reference)",
	"S5":   "S2b plus 40 workers (extra)",
}

func group(runs []*runData, scn, mode, arr string) []*runData {
	var g []*runData
	for _, r := range runs {
		if r.sum.Scenario == scn && r.sum.Mode == mode && r.sum.Arrival == arr {
			g = append(g, r)
		}
	}
	return g
}

func f0(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return strconv.FormatFloat(v, 'f', 0, 64)
}
func f1(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}
func pct(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return strconv.FormatFloat(100*v, 'f', 1, 64) + "%"
}
func kfmt(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return strconv.FormatFloat(v/1000, 'f', 0, 64) + "k"
}

func scenarioTable(runs []*runData, mode, arr string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "| Scenario | LRG p50 s | LRG p99 s | LRG max s | miss/req p50 | miss/req p99 | miss/req max | GETs/h | of which refresh | not-stored | cohorts A mean (range) | cohorts B mean (range) | bursts/615 s | peak 30 s share | staleness bound (observed max) |\n")
	fmt.Fprintf(&sb, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, scn := range scnOrder {
		g := group(runs, scn, mode, arr)
		if len(g) == 0 {
			continue
		}
		var durs, miss []float64
		var gets, ref, ns, ga, gb, bu, pk, age []float64
		gmin, gmax, bmin, bmax := math.MaxInt, 0, math.MaxInt, 0
		for _, r := range g {
			durs = append(durs, r.durs...)
			miss = append(miss, r.miss...)
			s := r.sum
			gets = append(gets, s.GetsPerH)
			ref = append(ref, s.RefGetsPerH)
			ns = append(ns, s.NotStored)
			ga = append(ga, s.GapMean)
			gb = append(gb, s.DenMean)
			bu = append(bu, s.BurstsPer615)
			pk = append(pk, s.Peak30Mean)
			age = append(age, s.MaxAge)
			gmin, gmax = min(gmin, s.GapMin), max(gmax, s.GapMax)
			bmin, bmax = min(bmin, s.DenMin), max(bmax, s.DenMax)
		}
		bound := "600 s"
		if scn == "S4" {
			bound = "none"
		}
		fmt.Fprintf(&sb, "| %s %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s (%d-%d) | %s (%d-%d) | %s | %s | %s (%s s) |\n",
			scn, scnDesc[scn], f1(percentile(durs, .5)), f1(percentile(durs, .99)), f1(percentile(durs, 1)),
			f0(percentile(miss, .5)), f0(percentile(miss, .99)), f0(percentile(miss, 1)),
			kfmt(mean(gets)), kfmt(mean(ref)), pct(mean(ns)), f1(mean(ga)), gmin, gmax, f1(mean(gb)), bmin, bmax,
			strconv.FormatFloat(mean(bu), 'f', 2, 64), pct(mean(pk)), bound, f0(percentile(age, 1)))
	}
	return sb.String()
}

func eventTable(runs []*runData, arr string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "| Scenario | event | misses/h 1 h before | misses/h 1 h after | cohorts A 6 h before | cohorts A 1-7 h after | cohorts B 6 h before | cohorts B 1-7 h after |\n|---|---|---|---|---|---|---|---|\n")
	for _, scn := range scnOrder {
		g := group(runs, scn, "warm", arr)
		if len(g) == 0 {
			continue
		}
		for ei := range g[0].sum.EventStats {
			var a, b, c, d, e, f []float64
			for _, r := range g {
				st := r.sum.EventStats[ei]
				a = append(a, st.MissPre1h)
				b = append(b, st.MissPost1h)
				c = append(c, st.GapPre)
				d = append(d, st.GapPost)
				e = append(e, st.DenPre)
				f = append(f, st.DenPost)
			}
			st := g[0].sum.EventStats[ei]
			fmt.Fprintf(&sb, "| %s | %s %d index(es) at +%.0f h | %s | %s | %s | %s | %s | %s |\n", scn, st.Label, len(st.Shards), (st.T-WarmSw)/H,
				kfmt(mean(a)), kfmt(mean(b)), f1(mean(c)), f1(mean(d)), f1(mean(e)), f1(mean(f)))
		}
	}
	return sb.String()
}

// Production data.

type promResult struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// promSum sums the series that match the filter, by timestamp.
func promSum(file string, filter func(map[string]string) bool) (map[int64]float64, error) {
	b, err := os.ReadFile(filepath.Join(ProdDir, file))
	if err != nil {
		return nil, err
	}
	var pr promResult
	if err := json.Unmarshal(b, &pr); err != nil {
		return nil, err
	}
	out := map[int64]float64{}
	for _, r := range pr.Data.Result {
		if filter != nil && !filter(r.Metric) {
			continue
		}
		for _, v := range r.Values {
			t := int64(v[0].(float64))
			x, _ := strconv.ParseFloat(v[1].(string), 64)
			out[t] += x
		}
	}
	return out, nil
}

type prodData struct {
	ts                      []int64
	miss, req, hit, add, ns []float64
}

func loadProd() (*prodData, error) {
	rg := func(m map[string]string) bool { return m["config"] == "rule-group" }
	req, err := promSum("A_req_pod.json", rg)
	if err != nil {
		return nil, err
	}
	hit, err := promSum("A_hit_pod.json", rg)
	if err != nil {
		return nil, err
	}
	add, err := promSum("A_add_pod.json", nil)
	if err != nil {
		return nil, err
	}
	ns, err := promSum("A_addns_pod.json", nil)
	if err != nil {
		return nil, err
	}
	p := &prodData{}
	for t := range req {
		p.ts = append(p.ts, t)
	}
	sort.Slice(p.ts, func(i, j int) bool { return p.ts[i] < p.ts[j] })
	for _, t := range p.ts {
		p.req = append(p.req, req[t])
		p.hit = append(p.hit, hit[t])
		p.miss = append(p.miss, req[t]-hit[t])
		p.add = append(p.add, add[t])
		p.ns = append(p.ns, ns[t])
	}
	return p, nil
}

// vstat holds the validation metrics for one window.
type vstat map[string]float64

func burstStats(rate, add, ns []float64, binsBack int) (n int, nsMin, nsMax, peak float64) {
	bs := findBursts(rate, BurstLo, BurstHi, BurstMerge)
	nsMin, nsMax = math.NaN(), math.NaN()
	for _, b := range bs {
		var a, s float64
		for i := max(b[0]-binsBack, 0); i <= b[1]; i++ {
			a += add[i]
			s += ns[i]
		}
		for i := b[0]; i <= b[1]; i++ {
			peak = math.Max(peak, rate[i])
		}
		if a == 0 {
			continue
		}
		r := s / a
		if math.IsNaN(nsMin) || r < nsMin {
			nsMin = r
		}
		if math.IsNaN(nsMax) || r > nsMax {
			nsMax = r
		}
	}
	return len(bs), nsMin, nsMax, peak
}

// prodWindow computes the metrics over [from, to) in seconds after 21:00 UTC.
func prodWindow(p *prodData, from, to float64) vstat {
	const t21 = 1791838800
	var R, Hh, A, NS float64
	var rate, add, ns []float64
	n := 0
	for i, t := range p.ts {
		rel := float64(t - t21)
		if rel <= from || rel > to {
			continue
		}
		rate = append(rate, p.miss[i])
		add = append(add, p.add[i])
		ns = append(ns, p.ns[i])
		if t%60 == 0 {
			R += p.req[i] * 60
			Hh += p.hit[i] * 60
			A += p.add[i] * 60
			NS += p.ns[i] * 60
			n++
		}
	}
	m := vstat{"hit": Hh / R, "missh": (R - Hh) / (float64(n) / 60), "ns": NS / A}
	bn, lo, hi, pk := burstStats(rate, add, ns, 0)
	m["bursts615"] = float64(bn) / ((to - from) / CycleP)
	m["burstNSmin"], m["burstNSmax"], m["peak"] = lo, hi, pk
	lag, r := acfPeak(rate, 20, 60)
	m["acflag"], m["acfr"] = float64(lag*15), r
	return m
}

// simWindow computes the same metrics from a run's saved files, over [from, to) in sim seconds.
func simWindow(dir string, from, to float64) (vstat, error) {
	rows, err := readCSV(filepath.Join(dir, "series15s.csv"))
	if err != nil {
		return nil, err
	}
	var L, Hh, A, NS float64
	var rate, add, ns []float64
	for _, r := range rows {
		t := atof(r["t"])
		if t <= from || t > to {
			continue
		}
		L += atof(r["lookups"])
		Hh += atof(r["hits"])
		A += atof(r["adds"])
		NS += atof(r["not_stored"])
		rate = append(rate, atof(r["miss_rate1m"]))
		add = append(add, atof(r["adds"]))
		ns = append(ns, atof(r["not_stored"]))
	}
	m := vstat{"hit": Hh / L, "missh": (L - Hh) / ((to - from) / H), "ns": NS / A}
	bn, lo, hi, pk := burstStats(rate, add, ns, 3)
	m["bursts615"] = float64(bn) / ((to - from) / CycleP)
	m["burstNSmin"], m["burstNSmax"], m["peak"] = lo, hi, pk
	lag, r := acfPeak(rate, 20, 60)
	m["acflag"], m["acfr"] = float64(lag*15), r
	reqs, err := readRequests(dir)
	if err != nil {
		return nil, err
	}
	m["slowMin"], m["slowMax"], m["slowMissMin"], m["slowMissMax"] = math.NaN(), math.NaN(), math.NaN(), math.NaN()
	var nreq, nslow float64
	for _, q := range reqs {
		if q.Start < from || q.End >= to {
			continue
		}
		nreq++
		d := q.End - q.Start
		if d <= 20 {
			continue
		}
		nslow++
		mi := float64(q.Misses)
		if math.IsNaN(m["slowMin"]) || d < m["slowMin"] {
			m["slowMin"] = d
		}
		if math.IsNaN(m["slowMax"]) || d > m["slowMax"] {
			m["slowMax"] = d
		}
		if math.IsNaN(m["slowMissMin"]) || mi < m["slowMissMin"] {
			m["slowMissMin"] = mi
		}
		if math.IsNaN(m["slowMissMax"]) || mi > m["slowMissMax"] {
			m["slowMissMax"] = mi
		}
	}
	m["slowShare"] = nslow / nreq
	return m, nil
}

type vrow struct {
	key, name string
	f         func(float64) string
	prodFix   string
}

var vrows = []vrow{
	{"hit", "hit ratio", func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }, ""},
	{"missh", "misses per hour", kfmt, ""},
	{"ns", "not-stored share of adds", pct, ""},
	{"burstNSmin", "not-stored share in the burst with the lowest share", pct, ""},
	{"burstNSmax", "not-stored share in the burst with the highest share", pct, ""},
	{"bursts615", "miss bursts per 615 s", func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }, ""},
	{"peak", "peak miss rate, per s (rate over 1m)", f0, ""},
	{"acflag", "autocorrelation peak lag, s", f0, ""},
	{"acfr", "autocorrelation peak r", func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }, ""},
	{"slowMin", "slow requests (LRG > 20 s): shortest, s", f1, "27.7 (sampled traces)"},
	{"slowMax", "slow requests: longest, s", f1, "100.2 (sampled traces)"},
	{"slowMissMin", "slow requests: fewest misses", f0, "7849 (sampled traces)"},
	{"slowMissMax", "slow requests: most misses", f0, "35315 (sampled traces)"},
	{"slowShare", "share of requests with LRG > 20 s", pct, "unknown"},
}

func validationTable(runs []*runData, p *prodData) (string, error) {
	type col struct {
		name     string
		prod     bool
		arr      string
		from, to float64 // seconds after 21:00
	}
	cols := []col{
		{"production 21:00-23:00", true, "", 0, 2 * H},
		{"S0 fixed +-2 s, 21:00-23:00", false, "fixed", 0, 2 * H},
		{"production 21:00-21:28", true, "", 0, 28 * 60},
		{"S0 fixed +-2 s, 21:00-21:28", false, "fixed", 0, 28 * 60},
		{"production 21:28-23:00", true, "", 28 * 60, 2 * H},
		{"S0 fixed +-2 s, 21:28-23:00", false, "fixed", 28 * 60, 2 * H},
		{"S0 fixed +-0.1 s, 21:00-23:00", false, "fixedtight", 0, 2 * H},
		{"S0 Poisson, 21:00-23:00", false, "poisson", 0, 2 * H},
	}
	vals := make([][]vstat, len(cols))
	for ci, c := range cols {
		if c.prod {
			vals[ci] = []vstat{prodWindow(p, c.from, c.to)}
			continue
		}
		for _, r := range group(runs, "S0", "warm", c.arr) {
			v, err := simWindow(r.dir, ValFrom+c.from, ValFrom+c.to)
			if err != nil {
				return "", err
			}
			vals[ci] = append(vals[ci], v)
		}
	}
	var sb strings.Builder
	sb.WriteString("Simulated columns: median over 5 seeds, with the seed range in brackets.\n\n| Metric |")
	for _, c := range cols {
		sb.WriteString(" " + c.name + " |")
	}
	sb.WriteString("\n|---|")
	for range cols {
		sb.WriteString("---|")
	}
	sb.WriteString("\n")
	for _, r := range vrows {
		fmt.Fprintf(&sb, "| %s |", r.name)
		for ci, c := range cols {
			var v []float64
			for _, s := range vals[ci] {
				if x, ok := s[r.key]; ok && !math.IsNaN(x) {
					v = append(v, x)
				}
			}
			switch {
			case c.prod && r.prodFix != "":
				if c.from == 0 && c.to == 2*H {
					sb.WriteString(" " + r.prodFix + " |")
				} else {
					sb.WriteString(" |")
				}
			case len(v) == 0:
				sb.WriteString(" n/a |")
			case c.prod:
				sb.WriteString(" " + r.f(v[0]) + " |")
			default:
				fmt.Fprintf(&sb, " %s [%s, %s] |", r.f(percentile(v, .5)), r.f(percentile(v, 0)), r.f(percentile(v, 1)))
			}
		}
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

func fmtAuto(v float64) string {
	switch {
	case math.IsNaN(v):
		return "n/a"
	case math.Abs(v) >= 1000:
		return kfmt(v)
	case math.Abs(v) >= 10:
		return f0(v)
	case math.Abs(v) >= 1:
		return f1(v)
	default:
		return strconv.FormatFloat(v, 'f', 3, 64)
	}
}

func compTable(runs []*runData, arr string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Composition of the 3 largest gap clusters at 22:00 on the replay day (S0, %s arrivals). Each cell is the number of live keys of that server index in the cluster.\n\n", arr)
	for _, r := range group(runs, "S0", "warm", arr) {
		fmt.Fprintf(&sb, "Seed %d. Events on the replay day:", r.sum.Seed)
		cfgEv := warmEvents(r.sum.Seed)
		for _, e := range cfgEv[:6] {
			fmt.Fprintf(&sb, " %02d:%02d %s %v;", int(math.Mod(e.T, 24*H)/H), int(math.Mod(e.T, H)/60), e.Label, e.Shards)
		}
		sb.WriteString("\n\n| cluster | idx0 | idx1 | idx2 | idx3 | idx4 | idx5 |\n|---|---|---|---|---|---|---|\n")
		for ci := 0; ci < 3; ci++ {
			fmt.Fprintf(&sb, "| %d |", ci+1)
			for sh := 0; sh < Shards; sh++ {
				fmt.Fprintf(&sb, " %d |", r.sum.ValComp[ci][sh])
			}
			sb.WriteString("\n")
		}
		sizes := shardSizes()
		sb.WriteString("| not in the 3 largest, or not cached |")
		for sh := 0; sh < Shards; sh++ {
			t := 0
			for ci := 0; ci < 3; ci++ {
				t += r.sum.ValComp[ci][sh]
			}
			fmt.Fprintf(&sb, " %d |", sizes[sh]-t)
		}
		sb.WriteString("\n| share in dominant cluster, mean 21:00-23:00 |")
		for sh := 0; sh < Shards; sh++ {
			fmt.Fprintf(&sb, " %.2f |", r.sum.ValAlign[sh])
		}
		sb.WriteString("\n| mean run length along list order at 22:00 |")
		for sh := 0; sh < Shards; sh++ {
			fmt.Fprintf(&sb, " %.1f |", r.sum.ValRuns[sh])
		}
		sb.WriteString("\n| run length if labels were random |")
		for sh := 0; sh < Shards; sh++ {
			// Expected run length of an i.i.d. label sequence: 1 / (1 - sum p_i^2).
			t, q := 0.0, 0.0
			for ci := 0; ci < 3; ci++ {
				t += float64(r.sum.ValComp[ci][sh])
			}
			for ci := 0; ci < 3; ci++ {
				p := float64(r.sum.ValComp[ci][sh]) / t
				q += p * p
			}
			if q >= 1 {
				sb.WriteString(" one label |")
			} else {
				fmt.Fprintf(&sb, " %.1f |", 1/(1-q))
			}
		}
		sb.WriteString("\n\n")
	}
	return sb.String()
}

// shardSizes repeats the fixed key placement of NewSim.
func shardSizes() [Shards]int {
	rk := rand.New(rand.NewPCG(12345, 7))
	var out [Shards]int
	for k := 0; k < N; k++ {
		out[rk.IntN(Shards)]++
		rk.IntN(SyncPods)
	}
	return out
}

// Mermaid.

func mermaidLines(title, xlab string, xs []string, ylab string, ymax float64, series [][]float64) string {
	var sb strings.Builder
	sb.WriteString("```mermaid\nxychart-beta\n")
	fmt.Fprintf(&sb, "    title \"%s\"\n", title)
	fmt.Fprintf(&sb, "    x-axis \"%s\" [%s]\n", xlab, strings.Join(xs, ", "))
	fmt.Fprintf(&sb, "    y-axis \"%s\" 0 --> %s\n", ylab, strconv.FormatFloat(ymax, 'f', -1, 64))
	for _, s := range series {
		var p []string
		for _, v := range s {
			if math.IsNaN(v) {
				v = 0
			}
			p = append(p, strconv.FormatFloat(v, 'f', 1, 64))
		}
		fmt.Fprintf(&sb, "    line [%s]\n", strings.Join(p, ", "))
	}
	sb.WriteString("```\n")
	return sb.String()
}

func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	e := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10} {
		if m*e >= v {
			return m * e
		}
	}
	return 10 * e
}

// cohortSeries averages a cohort column over seeds in buckets.
func cohortSeries(runs []*runData, scn, mode, arr, col string, from, to float64, n int) []float64 {
	out := make([]float64, n)
	cnt := make([]float64, n)
	w := (to - from) / float64(n)
	for _, r := range group(runs, scn, mode, arr) {
		rows, err := readCSV(filepath.Join(r.dir, "cohorts.csv"))
		if err != nil {
			continue
		}
		for _, row := range rows {
			t := atof(row["t"])
			if t < from || t >= to {
				continue
			}
			b := int((t - from) / w)
			out[b] += atof(row[col])
			cnt[b]++
		}
	}
	for i := range out {
		out[i] /= math.Max(cnt[i], 1)
	}
	return out
}

func durSlice(r *runData, from float64) []float64 {
	reqs, _ := readRequests(r.dir)
	out := make([]float64, 60)
	for _, q := range reqs {
		if q.Start >= from && q.Start < from+H {
			b := int((q.Start - from) / 60)
			out[b] = math.Max(out[b], q.End-q.Start)
		}
	}
	return out
}

func analyze(out string) error {
	runs, err := loadRuns(out)
	if err != nil {
		return err
	}
	rep := filepath.Join(filepath.Dir(out), "report")
	if err := os.MkdirAll(rep, 0o755); err != nil {
		return err
	}
	var sb strings.Builder
	for _, mode := range []string{"warm", "cold"} {
		for _, arr := range []string{"fixed", "poisson", "fixedtight"} {
			if len(group(runs, "S0", mode, arr)) == 0 {
				continue
			}
			fmt.Fprintf(&sb, "## %s start, %s arrivals\n\n", mode, arr)
			sb.WriteString(scenarioTable(runs, mode, arr))
			sb.WriteString("\n")
		}
	}
	for _, arr := range []string{"fixed", "poisson"} {
		fmt.Fprintf(&sb, "## server events after the switch, %s arrivals\n\n", arr)
		sb.WriteString(eventTable(runs, arr))
		sb.WriteString("\n")
	}
	if err := os.WriteFile(filepath.Join(rep, "tables.md"), []byte(sb.String()), 0o644); err != nil {
		return err
	}
	prod, err := loadProd()
	if err != nil {
		return err
	}
	vt, err := validationTable(runs, prod)
	if err != nil {
		return err
	}
	vs := vt + "\n" + compTable(runs, "fixed")
	if err := os.WriteFile(filepath.Join(rep, "validation.md"), []byte(vs), 0o644); err != nil {
		return err
	}
	// D2: cohort count over time from cold start, S0 against S1a, 48 min buckets.
	for _, col := range []string{"gap_count", "den_count"} {
		a := cohortSeries(runs, "S0", "cold", "fixed", col, 0, ColdEnd, 60)
		b := cohortSeries(runs, "S1a", "cold", "fixed", col, 0, ColdEnd, 60)
		var xs []string
		for i := 0; i < 60; i++ {
			xs = append(xs, strconv.FormatFloat(float64(i)*0.8, 'f', 1, 64))
		}
		ym := 0.0
		for i := range a {
			ym = math.Max(ym, math.Max(a[i], b[i]))
		}
		name := map[string]string{"gap_count": "gap method", "den_count": "density method"}[col]
		mm := mermaidLines(fmt.Sprintf("Macro cohorts from cold start (%s): line 1 S0, line 2 S1a", name), "hours", xs, "cohorts", niceMax(ym), [][]float64{a, b})
		fn := map[string]string{"gap_count": "d2_gap.mmd", "den_count": "d2_density.mmd"}[col]
		if err := os.WriteFile(filepath.Join(rep, fn), []byte(mm), 0o644); err != nil {
			return err
		}
	}
	// D3: LoadRuleGroups duration per request, 1 h slice after the switch, warm, fixed, seed 1.
	best := os.Getenv("BEST")
	if best == "" {
		best = "S2b"
	}
	slice := WarmSw + 4*H
	var d3 [][]float64
	ym := 0.0
	for _, scn := range []string{"S0", best} {
		for _, r := range group(runs, scn, "warm", "fixed") {
			if r.sum.Seed == 1 {
				s := durSlice(r, slice)
				for _, v := range s {
					ym = math.Max(ym, v)
				}
				d3 = append(d3, s)
			}
		}
	}
	var xs []string
	for i := 0; i < 60; i++ {
		xs = append(xs, strconv.Itoa(i))
	}
	mm := mermaidLines(fmt.Sprintf("LoadRuleGroups s, slowest per minute: line 1 S0, line 2 %s", best), "minute", xs, "seconds", niceMax(ym), d3)
	if err := os.WriteFile(filepath.Join(rep, "d3.mmd"), []byte(mm), 0o644); err != nil {
		return err
	}
	// D4: production miss rate at a 1 m step, two blocks of 60 points.
	for blk := 0; blk < 2; blk++ {
		var vals []float64
		var xs []string
		for i, t := range prod.ts {
			if t%60 != 0 {
				continue
			}
			mi := (t - 1791838800) / 60
			if mi < int64(blk*60) || mi >= int64(blk*60+60) {
				continue
			}
			vals = append(vals, prod.miss[i])
			xs = append(xs, fmt.Sprintf("\"%02d:%02d\"", 21+blk, mi%60))
		}
		ym := 0.0
		for _, v := range vals {
			ym = math.Max(ym, v)
		}
		mm := mermaidLines(fmt.Sprintf("Production rule-group cache misses per s, %02d:00-%02d:59 UTC", 21+blk, 21+blk), "time", xs, "misses per s", niceMax(ym), [][]float64{vals})
		if err := os.WriteFile(filepath.Join(rep, fmt.Sprintf("d4_%d.mmd", blk+1)), []byte(mm), 0o644); err != nil {
			return err
		}
	}
	// D1 cohort maps.
	for _, spec := range []struct{ scn, mode string }{{"S0", "cold"}, {"S1a", "cold"}, {"S0", "warm"}, {"S1a", "warm"}, {"S2b", "warm"}, {"S3", "warm"}} {
		for _, r := range group(runs, spec.scn, spec.mode, "fixed") {
			if r.sum.Seed != 1 {
				continue
			}
			if err := renderD1(r, rep); err != nil {
				return err
			}
		}
	}
	fmt.Println("wrote", rep)
	return nil
}
