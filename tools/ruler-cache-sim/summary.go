package main

import (
	"math"
)

type Window struct {
	From, To     float64
	NReq         int
	DurP50       float64
	DurP99       float64
	DurMax       float64
	MissReqP50   float64
	MissReqP99   float64
	MissReqMax   float64
	SlowN        int // LoadRuleGroups > 20 s
	SlowDurMin   float64
	SlowDurMax   float64
	SlowMissMin  float64
	SlowMissMax  float64
	GetsPerH     float64
	RefGetsPerH  float64
	MissPerH     float64
	HitRatio     float64
	NotStored    float64
	BurstN       int
	BurstsPer615 float64
	BurstNSMin   float64
	BurstNSMax   float64
	BurstPeakMax float64
	ACFLag       float64
	ACFr         float64
}

type EvStat struct {
	T          float64
	Label      string
	Shards     []int
	MissPre1h  float64
	MissPost1h float64
	GapPre     float64
	GapPost    float64
	DenPre     float64
	DenPost    float64
}

type Summary struct {
	Scenario, Mode, Arrival string
	Seed                    uint64
	Window
	ACF2hLag     float64 // median over 2 h windows
	ACF2hR       float64
	GapMean      float64
	GapMin       int
	GapMax       int
	GapP10       float64
	GapP90       float64
	DenMean      float64
	DenMin       int
	DenMax       int
	Peak30Mean   float64
	Top1Mean     float64
	Top2Mean     float64
	MaxAge       float64
	AgeOver600   int64
	SyncMissPerH float64
	// Warm mode only.
	EventStats []EvStat
	Val        *Window // 21:00-23:00 of the replay day, like the production window
	ValComp    [3][Shards]int
	ValAlign   [Shards]float64 // share of each server's live keys in its dominant top-3 cluster
	ValRuns    [Shards]float64 // mean run length along list order at 22:00 on the replay day
	WallSec    float64
}

func (s *Sim) rateSeries(from, to float64) []float64 {
	var r []float64
	for t := int(from) + 15; float64(t) <= to; t += 15 {
		r = append(r, rate1m(s, t))
	}
	return r
}

// Bursts: runs of points >= lo, merged across gaps shorter than mergeGap points, kept when the peak >= hi.
func findBursts(rate []float64, lo, hi float64, mergeGap int) [][2]int {
	var runs [][2]int
	in := false
	for i, v := range rate {
		if v >= lo {
			if !in {
				runs = append(runs, [2]int{i, i})
				in = true
			}
			runs[len(runs)-1][1] = i
		} else {
			in = false
		}
	}
	var merged [][2]int
	for _, r := range runs {
		if len(merged) > 0 && r[0]-merged[len(merged)-1][1] <= mergeGap {
			merged[len(merged)-1][1] = r[1]
			continue
		}
		merged = append(merged, r)
	}
	var out [][2]int
	for _, r := range merged {
		pk := 0.0
		for i := r[0]; i <= r[1]; i++ {
			pk = math.Max(pk, rate[i])
		}
		if pk >= hi {
			out = append(out, r)
		}
	}
	return out
}

const (
	BurstLo    = 100.0
	BurstHi    = 300.0
	BurstMerge = 4
)

func acfPeak(x []float64, minLag, maxLag int) (int, float64) {
	n := len(x)
	if n < maxLag+2 {
		return 0, math.NaN()
	}
	m := 0.0
	for _, v := range x {
		m += v
	}
	m /= float64(n)
	den := 0.0
	for _, v := range x {
		den += (v - m) * (v - m)
	}
	if den == 0 {
		return 0, math.NaN()
	}
	best, bl := math.Inf(-1), 0
	for lag := minLag; lag <= maxLag; lag++ {
		num := 0.0
		for i := 0; i+lag < n; i++ {
			num += (x[i] - m) * (x[i+lag] - m)
		}
		r := num / den
		if r > best {
			best, bl = r, lag
		}
	}
	return bl, best
}

func (s *Sim) window(from, to float64) Window {
	w := Window{From: from, To: to}
	var dur, mis []float64
	for _, r := range s.ReqLog {
		if r.Start < from || r.End >= to {
			continue
		}
		d := r.End - r.Start
		dur = append(dur, d)
		mis = append(mis, float64(r.Misses))
		if d > 20 {
			if w.SlowN == 0 || d < w.SlowDurMin {
				w.SlowDurMin = d
			}
			w.SlowDurMax = math.Max(w.SlowDurMax, d)
			if w.SlowN == 0 || float64(r.Misses) < w.SlowMissMin {
				w.SlowMissMin = float64(r.Misses)
			}
			w.SlowMissMax = math.Max(w.SlowMissMax, float64(r.Misses))
			w.SlowN++
		}
	}
	w.NReq = len(dur)
	w.DurP50, w.DurP99, w.DurMax = percentile(dur, 0.5), percentile(dur, 0.99), percentile(dur, 1)
	w.MissReqP50, w.MissReqP99, w.MissReqMax = percentile(mis, 0.5), percentile(mis, 0.99), percentile(mis, 1)
	var l, h, g, rg, a, ns int64
	for i := int(from); i < int(to) && i < s.nsec; i++ {
		l += int64(s.b.lookups[i])
		h += int64(s.b.hits[i])
		g += int64(s.b.gets[i])
		rg += int64(s.b.refGets[i])
		a += int64(s.b.adds[i])
		ns += int64(s.b.ns[i])
	}
	hrs := (to - from) / H
	w.GetsPerH = float64(g) / hrs
	w.RefGetsPerH = float64(rg) / hrs
	w.MissPerH = float64(l-h) / hrs
	if l > 0 {
		w.HitRatio = float64(h) / float64(l)
	}
	if a > 0 {
		w.NotStored = float64(ns) / float64(a)
	}
	rate := s.rateSeries(from, to)
	bs := findBursts(rate, BurstLo, BurstHi, BurstMerge)
	w.BurstN = len(bs)
	w.BurstsPer615 = float64(len(bs)) / ((to - from) / CycleP)
	w.BurstNSMin = math.NaN()
	w.BurstNSMax = math.NaN()
	for _, b := range bs {
		// rate point i covers the 60 s ending at from+15*(i+1).
		t0 := int(from) + 15*(b[0]+1) - 60
		t1 := int(from) + 15*(b[1]+1)
		var ba, bn int64
		pk := 0.0
		for i := b[0]; i <= b[1]; i++ {
			pk = math.Max(pk, rate[i])
		}
		w.BurstPeakMax = math.Max(w.BurstPeakMax, pk)
		for i := max(t0, 0); i < t1 && i < s.nsec; i++ {
			ba += int64(s.b.adds[i])
			bn += int64(s.b.ns[i])
		}
		if ba == 0 {
			continue
		}
		r := float64(bn) / float64(ba)
		if math.IsNaN(w.BurstNSMin) || r < w.BurstNSMin {
			w.BurstNSMin = r
		}
		if math.IsNaN(w.BurstNSMax) || r > w.BurstNSMax {
			w.BurstNSMax = r
		}
	}
	lag, r := acfPeak(rate, 20, 60)
	w.ACFLag, w.ACFr = float64(lag*15), r
	return w
}

func summarize(s *Sim) *Summary {
	cfg := s.cfg
	from := H
	if cfg.Mode == "warm" {
		from = cfg.SwitchAt
	}
	sum := &Summary{Scenario: cfg.Scenario, Mode: cfg.Mode, Arrival: cfg.Arrival, Seed: cfg.Seed}
	sum.Window = s.window(from, cfg.End)
	var lags, rs []float64
	for t := from; t+2*H <= cfg.End; t += 2 * H {
		l, r := acfPeak(s.rateSeries(t, t+2*H), 20, 60)
		lags = append(lags, float64(l*15))
		rs = append(rs, r)
	}
	sum.ACF2hLag, sum.ACF2hR = percentile(lags, 0.5), percentile(rs, 0.5)
	var gaps, dens []float64
	var pk, t1, t2 float64
	for _, c := range s.Cohorts {
		if c.T < from {
			continue
		}
		gaps = append(gaps, float64(c.GapCount))
		dens = append(dens, float64(c.DenCount))
		pk += c.Peak30
		t1 += float64(c.GapTop[0])
		t2 += float64(c.GapTop[1])
		if len(gaps) == 1 || c.GapCount < sum.GapMin {
			sum.GapMin = c.GapCount
		}
		sum.GapMax = max(sum.GapMax, c.GapCount)
		if len(dens) == 1 || c.DenCount < sum.DenMin {
			sum.DenMin = c.DenCount
		}
		sum.DenMax = max(sum.DenMax, c.DenCount)
	}
	n := float64(len(gaps))
	sum.GapMean = mean(gaps)
	sum.DenMean = mean(dens)
	sum.GapP10, sum.GapP90 = percentile(gaps, 0.1), percentile(gaps, 0.9)
	sum.Peak30Mean, sum.Top1Mean, sum.Top2Mean = pk/n, t1/n, t2/n
	sum.MaxAge = s.MaxAge
	sum.AgeOver600 = s.AgeOver600
	var ms int64
	for i := int(from); i < int(cfg.End) && i < s.nsec; i++ {
		ms += int64(s.b.missS[i])
	}
	sum.SyncMissPerH = float64(ms) / ((cfg.End - from) / H)
	for _, e := range cfg.Events {
		if e.T < from {
			continue
		}
		st := EvStat{T: e.T, Label: e.Label, Shards: e.Shards}
		st.MissPre1h = s.window(e.T-H, e.T).MissPerH
		st.MissPost1h = s.window(e.T, e.T+H).MissPerH
		st.GapPre, st.DenPre = s.cohortMean(e.T-6*H, e.T)
		st.GapPost, st.DenPost = s.cohortMean(e.T+H, e.T+7*H)
		sum.EventStats = append(sum.EventStats, st)
	}
	if cfg.Mode == "warm" {
		v := s.window(ValFrom, ValTo)
		sum.Val = &v
		var n float64
		for _, c := range s.Cohorts {
			if c.T < ValFrom || c.T >= ValTo {
				continue
			}
			if c.T == ValFrom+H {
				sum.ValComp = c.Comp
				sum.ValRuns = c.Runs
			}
			for sh := 0; sh < Shards; sh++ {
				tot, best := 0, 0
				for r := 0; r < 3; r++ {
					tot += c.Comp[r][sh]
					best = max(best, c.Comp[r][sh])
				}
				if tot > 0 {
					sum.ValAlign[sh] += float64(best) / float64(tot)
				}
			}
			n++
		}
		for sh := range sum.ValAlign {
			sum.ValAlign[sh] /= n
		}
	}
	return sum
}

func (s *Sim) cohortMean(from, to float64) (float64, float64) {
	var g, d []float64
	for _, c := range s.Cohorts {
		if c.T >= from && c.T < to {
			g = append(g, float64(c.GapCount))
			d = append(d, float64(c.DenCount))
		}
	}
	return mean(g), mean(d)
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	t := 0.0
	for _, x := range v {
		t += x
	}
	return t / float64(len(v))
}
