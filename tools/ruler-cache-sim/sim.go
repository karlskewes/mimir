package main

// Discrete-event model of the ruler rule-group cache under ListRules polling.
// Time unit is seconds.
// Cache semantics follow CachingBucket.Get and getReader.Read:
// one lookup per key, hit returns with no write,
// miss does an object-store GET, then Add(exists lock) and Add(content lock),
// and sets the content only when the content-lock Add succeeds.

import (
	"math"
	"math/rand/v2"
)

const (
	N          = 73087
	TTL        = 600.0
	LockTTL    = 15.0
	HitCost    = 0.00106 // seconds per lookup per worker
	ListMin    = 6.7     // list phase U(6.7, 7.1) s, from 4 traces
	ListMax    = 7.1
	MeanGap    = 30.0
	FixJitter  = 2.0 // fixed arrivals at the ruler: k*30 + U(-2, 2)
	SyncPods   = 14
	SyncPeriod = 600.0
	Shards     = 6
	CycleP     = 615.0 // period used for the cohort phase metric
	D1Bins     = 1000
	FacetBins  = 200
)

// GET latency: piecewise-linear inverse CDF.
// p50, p90, p99 and the mean match the production numbers.
// p0, p10, p99.9 and p100 are assumptions.
var getQx = []float64{0, 0.1, 0.5, 0.9, 0.99, 0.999, 1}
var getQy = []float64{18, 22, 25.6, 29.2, 42.5, 80, 150}

func sampleGet(r *rand.Rand) float64 {
	u := r.Float64()
	for i := 1; i < len(getQx); i++ {
		if u <= getQx[i] {
			f := (u - getQx[i-1]) / (getQx[i] - getQx[i-1])
			return (getQy[i-1] + f*(getQy[i]-getQy[i-1])) / 1000
		}
	}
	return getQy[len(getQy)-1] / 1000
}

type Policy struct {
	Name        string
	TTLJitterLo float64 // content TTL = TTL * U(lo, 1); 1 means fixed
	Refresh     bool    // refresh-ahead
	RefreshFrac float64
	RefreshPool int // background workers per pod, 0 = unbounded
	ExtendOnHit bool
	Workers     int
}

type Config struct {
	Scenario string
	Mode     string // cold | warm
	Arrival  string // poisson | fixed
	Seed     uint64
	Policy   Policy
	SwitchAt float64 // policy switch time; baseline before it
	End      float64
	Events   []Event
	Sync     bool
	D1       bool
	Out      string
}

// Event loses all keys of the given memcached server indexes at time T.
// One index: a server replaced.
// Two indexes: an address-order change, keys look up a server that does not hold them.
type Event struct {
	T      float64
	Shards []int
	Label  string
}

var Baseline = Policy{Name: "S0", TTLJitterLo: 1, Workers: 10}

type Cache struct {
	exp    []float64 // content expiry, 0 = absent
	wtime  []float64 // GET start of the stored copy
	writer []int32
	clock  []float64
	elock  []float64
	shard  []uint8
	owner  []uint8
	inflt  []uint16 // refresh single-flight, one bit per pod
	phase  [Shards]float64
}

// memExp converts a TTL to an absolute expiry.
// dskit truncates the TTL to whole seconds.
// memcached keeps a one-second clock and expires an item when exptime <= current_time.
func (c *Cache) memExp(now, ttl float64, k int32) float64 {
	secs := math.Floor(ttl)
	if secs <= 0 {
		return -1
	}
	ph := c.phase[c.shard[k]]
	return math.Floor(now-ph) + ph + secs
}

type worker struct {
	t   float64
	key int32
	gs  float64
}

const (
	kindList = iota
	kindSync
	kindRefresh
)

type Req struct {
	id     int32
	kind   int
	pod    int
	keys   []int32 // nil = all keys in list order
	n      int
	cursor int
	ws     []worker
	active int
	arrive float64
	start  float64
	end    float64
	hits   int32
	misses int32
	slot   int32
}

func (r *Req) siftDown(i int) {
	ws := r.ws[:r.active]
	for {
		l := 2*i + 1
		if l >= len(ws) {
			return
		}
		m := l
		if l+1 < len(ws) && ws[l+1].t < ws[l].t {
			m = l + 1
		}
		if ws[i].t <= ws[m].t {
			return
		}
		ws[i], ws[m] = ws[m], ws[i]
		i = m
	}
}

// Pool runs background refreshes for one pod.
type Pool struct {
	pod   int
	max   int
	queue []refJob
	qh    int
	busy  []worker // heap
	ver   int32
	sched float64 // time of the live heap entry, +Inf if none
}

type refJob struct {
	key int32
	t   float64
}

func (p *Pool) push(w worker) {
	p.busy = append(p.busy, w)
	i := len(p.busy) - 1
	for i > 0 {
		par := (i - 1) / 2
		if p.busy[par].t <= p.busy[i].t {
			break
		}
		p.busy[par], p.busy[i] = p.busy[i], p.busy[par]
		i = par
	}
}

func (p *Pool) pop() worker {
	w := p.busy[0]
	last := len(p.busy) - 1
	p.busy[0] = p.busy[last]
	p.busy = p.busy[:last]
	i := 0
	for {
		l := 2*i + 1
		if l >= len(p.busy) {
			break
		}
		m := l
		if l+1 < len(p.busy) && p.busy[l+1].t < p.busy[l].t {
			m = l + 1
		}
		if p.busy[i].t <= p.busy[m].t {
			break
		}
		p.busy[i], p.busy[m] = p.busy[m], p.busy[i]
		i = m
	}
	return w
}

// Event heap.
const (
	evReq = iota
	evPool
	evArrive
	evStart
	evSync
	evDrop
	evSample
	evRow
	evSwitch
)

type item struct {
	t    float64
	kind uint8
	idx  int32
	ver  int32
	aux  float64
}

type evHeap []item

func (h *evHeap) push(it item) {
	*h = append(*h, it)
	s := *h
	i := len(s) - 1
	for i > 0 {
		p := (i - 1) / 2
		if s[p].t <= s[i].t {
			break
		}
		s[p], s[i] = s[i], s[p]
		i = p
	}
}

func (h *evHeap) pop() item {
	s := *h
	it := s[0]
	last := len(s) - 1
	s[0] = s[last]
	s = s[:last]
	i := 0
	for {
		l := 2*i + 1
		if l >= len(s) {
			break
		}
		m := l
		if l+1 < len(s) && s[l+1].t < s[l].t {
			m = l + 1
		}
		if s[i].t <= s[m].t {
			break
		}
		s[i], s[m] = s[m], s[i]
		i = m
	}
	*h = s
	return it
}

type Bins struct {
	lookups, hits, missL, missS, gets, refGets, adds, ns []int32
}

func newBins(n int) Bins {
	mk := func() []int32 { return make([]int32, n) }
	return Bins{mk(), mk(), mk(), mk(), mk(), mk(), mk(), mk()}
}

type ReqRec struct {
	ID     int32
	Kind   int
	Arrive float64
	Start  float64
	End    float64
	Hits   int32
	Misses int32
}

type CohortRec struct {
	T        float64
	Live     int
	GapCount int             // method A
	GapTop   [3]int          // sizes of the largest gap clusters
	DenCount int             // method B
	Peak30   float64         // share of live keys in the busiest 30 s phase window
	Comp     [3][Shards]int  // server-index composition of the 3 largest gap clusters
	Runs     [Shards]float64 // mean run length of macro-cluster labels along list order, per server index
}

type Sim struct {
	cfg          Config
	pol          Policy
	c            Cache
	rArr         *rand.Rand
	rList        *rand.Rand
	rGet         *rand.Rand
	rSync        *rand.Rand
	rPod         *rand.Rand
	rTTL         *rand.Rand
	h            evHeap
	reqs         []*Req
	free         []int32
	pools        []*Pool
	nid          int32
	sync         [][]int32
	b            Bins
	nsec         int
	isSyncWriter map[int32]bool

	ReqLog     []ReqRec
	SyncLog    []ReqRec
	Cohorts    []CohortRec
	RowPh      [][]float32
	RowWr      [][]int32
	RowT       []float64
	MaxAge     float64
	AgeOver600 int64
	arrK       int
	arr0       float64

	hist     []int32
	hist1    []int32
	lab      []int16
	byShard  [][]int32
	RowFacet [][]float32
}

func NewSim(cfg Config) *Sim {
	s := &Sim{cfg: cfg, pol: Baseline}
	if cfg.SwitchAt <= 0 {
		s.pol = cfg.Policy
	}
	seed := cfg.Seed
	s.rArr = rand.New(rand.NewPCG(seed, 1))
	s.rList = rand.New(rand.NewPCG(seed, 2))
	s.rGet = rand.New(rand.NewPCG(seed, 3))
	s.rSync = rand.New(rand.NewPCG(seed, 4))
	s.rPod = rand.New(rand.NewPCG(seed, 5))
	s.rTTL = rand.New(rand.NewPCG(seed, 6))
	// Key placement (owner pod, memcached shard) is a property of the tenant: fixed seed.
	rk := rand.New(rand.NewPCG(12345, 7))
	c := &s.c
	c.exp = make([]float64, N)
	c.wtime = make([]float64, N)
	c.writer = make([]int32, N)
	c.clock = make([]float64, N)
	c.elock = make([]float64, N)
	c.shard = make([]uint8, N)
	c.owner = make([]uint8, N)
	c.inflt = make([]uint16, N)
	s.sync = make([][]int32, SyncPods)
	for k := 0; k < N; k++ {
		c.shard[k] = uint8(rk.IntN(Shards))
		c.owner[k] = uint8(rk.IntN(SyncPods))
		c.writer[k] = -1
		s.sync[c.owner[k]] = append(s.sync[c.owner[k]], int32(k))
	}
	for i := range c.phase {
		c.phase[i] = rk.Float64()
	}
	s.nsec = int(cfg.End) + 2
	s.b = newBins(s.nsec)
	s.isSyncWriter = map[int32]bool{}
	s.hist = make([]int32, int(CycleP*10))
	s.hist1 = make([]int32, int(CycleP))
	s.lab = make([]int16, int(CycleP*10))
	s.byShard = make([][]int32, Shards)
	for k := 0; k < N; k++ {
		s.byShard[c.shard[k]] = append(s.byShard[c.shard[k]], int32(k))
	}
	for p := 0; p < SyncPods; p++ {
		s.pools = append(s.pools, &Pool{pod: p, sched: math.Inf(1)})
	}
	return s
}

func (s *Sim) ttlDraw() float64 {
	if s.pol.TTLJitterLo >= 1 {
		return TTL
	}
	lo := s.pol.TTLJitterLo
	return TTL * (lo + (1-lo)*s.rTTL.Float64())
}

func (s *Sim) newReq(kind, pod int, keys []int32, t, arrive float64) {
	w := s.pol.Workers
	r := &Req{id: s.nid, kind: kind, pod: pod, keys: keys, arrive: arrive, start: t}
	s.nid++
	if kind == kindSync {
		s.isSyncWriter[r.id] = true
	}
	if keys == nil {
		r.n = N
	} else {
		r.n = len(keys)
	}
	r.ws = make([]worker, w)
	for i := range r.ws {
		r.ws[i] = worker{t: t, key: -1}
	}
	r.active = w
	var slot int32
	if len(s.free) > 0 {
		slot = s.free[len(s.free)-1]
		s.free = s.free[:len(s.free)-1]
		s.reqs[slot] = r
	} else {
		slot = int32(len(s.reqs))
		s.reqs = append(s.reqs, r)
	}
	r.slot = slot
	s.h.push(item{t: t, kind: evReq, idx: slot})
}

func (s *Sim) sec(t float64) int {
	i := int(t)
	if i >= s.nsec {
		i = s.nsec - 1
	}
	return i
}

// lookup returns true on a cache hit at time t.
func (s *Sim) lookup(k int32, t float64, r *Req) bool {
	c := &s.c
	i := s.sec(t)
	s.b.lookups[i]++
	if c.exp[k] > t {
		s.b.hits[i]++
		age := t - c.wtime[k]
		if age > s.MaxAge {
			s.MaxAge = age
		}
		if age > TTL {
			s.AgeOver600++
		}
		if s.pol.ExtendOnHit {
			if e := c.memExp(t, TTL, k); e > c.exp[k] {
				c.exp[k] = e
			}
		}
		if s.pol.Refresh && age > s.pol.RefreshFrac*TTL {
			bit := uint16(1) << r.pod
			if c.inflt[k]&bit == 0 {
				c.inflt[k] |= bit
				s.enqueueRefresh(r.pod, k, t)
			}
		}
		return true
	}
	if r.kind == kindSync {
		s.b.missS[i]++
	} else {
		s.b.missL[i]++
	}
	return false
}

// store applies the post-GET cache writes at time t for a GET that started at gs.
func (s *Sim) store(k int32, gs, t float64, writer int32, refresh bool) {
	c := &s.c
	i := s.sec(t)
	if refresh {
		s.b.refGets[i]++
	}
	s.b.gets[i]++
	el := t - gs
	if TTL-el > 0 {
		s.b.adds[i]++
		if c.elock[k] > t {
			s.b.ns[i]++
		} else {
			c.elock[k] = c.memExp(t, LockTTL, k)
		}
	}
	ttl := s.ttlDraw() - el
	if ttl > 0 {
		s.b.adds[i]++
		if c.clock[k] > t {
			s.b.ns[i]++
			return
		}
		c.clock[k] = c.memExp(t, LockTTL, k)
		if e := c.memExp(t, ttl, k); e > 0 {
			c.exp[k] = e
			c.wtime[k] = gs
			c.writer[k] = writer
		}
	}
}

// step runs the earliest worker of r.
func (s *Sim) step(r *Req) {
	w := &r.ws[0]
	t := w.t
	if w.key >= 0 {
		s.store(w.key, w.gs, t, r.id, false)
		w.key = -1
	}
	if r.cursor < r.n {
		var k int32
		if r.keys == nil {
			k = int32(r.cursor)
		} else {
			k = r.keys[r.cursor]
		}
		r.cursor++
		if s.lookup(k, t, r) {
			r.hits++
			w.t = t + HitCost
		} else {
			r.misses++
			gs := t + HitCost
			w.t = gs + sampleGet(s.rGet)
			w.key = k
			w.gs = gs
		}
		r.siftDown(0)
		return
	}
	if t > r.end {
		r.end = t
	}
	r.active--
	r.ws[0] = r.ws[r.active]
	r.siftDown(0)
}

func (s *Sim) enqueueRefresh(pod int, k int32, t float64) {
	p := s.pools[pod]
	if p.max == 0 || len(p.busy) < p.max {
		p.push(worker{t: t + sampleGet(s.rGet), key: k, gs: t})
		s.schedPool(p)
		return
	}
	p.queue = append(p.queue, refJob{key: k, t: t})
}

func (s *Sim) schedPool(p *Pool) {
	if len(p.busy) == 0 {
		p.sched = math.Inf(1)
		return
	}
	if nt := p.busy[0].t; nt < p.sched {
		p.ver++
		p.sched = nt
		s.h.push(item{t: nt, kind: evPool, idx: int32(p.pod), ver: p.ver})
	}
}

func (s *Sim) runPool(p *Pool, t float64) {
	for len(p.busy) > 0 && p.busy[0].t <= t {
		w := p.pop()
		s.store(w.key, w.gs, w.t, -2, true)
		s.c.inflt[w.key] &^= uint16(1) << p.pod
		if p.qh < len(p.queue) {
			j := p.queue[p.qh]
			p.qh++
			if p.qh > 4096 && p.qh*2 > len(p.queue) {
				p.queue = append(p.queue[:0], p.queue[p.qh:]...)
				p.qh = 0
			}
			p.push(worker{t: w.t + sampleGet(s.rGet), key: j.key, gs: w.t})
		}
	}
	p.sched = math.Inf(1)
	s.schedPool(p)
}

func (s *Sim) nextArrival(t float64) float64 {
	switch s.cfg.Arrival {
	case "fixed":
		s.arrK++
		return s.arr0 + float64(s.arrK)*MeanGap + FixJitter*(2*s.rArr.Float64()-1)
	case "fixedtight":
		s.arrK++
		return s.arr0 + float64(s.arrK)*MeanGap + 0.1*(2*s.rArr.Float64()-1)
	}
	return t + s.rArr.ExpFloat64()*MeanGap
}

func (s *Sim) drop(shards []int) {
	c := &s.c
	var lost [Shards]bool
	for _, sh := range shards {
		lost[sh] = true
	}
	for k := 0; k < N; k++ {
		if lost[c.shard[k]] {
			c.exp[k] = 0
			c.clock[k] = 0
			c.elock[k] = 0
		}
	}
}

func (s *Sim) Run() {
	cfg := s.cfg
	if cfg.Arrival != "poisson" {
		s.arr0 = s.rArr.Float64() * MeanGap
		s.h.push(item{t: s.arr0, kind: evArrive})
	} else {
		s.h.push(item{t: s.rArr.ExpFloat64() * MeanGap, kind: evArrive})
	}
	if cfg.Sync {
		for p := 0; p < SyncPods; p++ {
			s.h.push(item{t: s.rSync.Float64() * SyncPeriod, kind: evSync, idx: int32(p)})
		}
	}
	for i, e := range cfg.Events {
		s.h.push(item{t: e.T, kind: evDrop, idx: int32(i)})
	}
	s.h.push(item{t: 60, kind: evSample})
	if cfg.D1 {
		s.h.push(item{t: 0, kind: evRow})
	}
	if cfg.SwitchAt > 0 {
		s.h.push(item{t: cfg.SwitchAt, kind: evSwitch})
	}
	for len(s.h) > 0 {
		it := s.h.pop()
		if it.t >= cfg.End {
			break
		}
		switch it.kind {
		case evReq:
			r := s.reqs[it.idx]
			for {
				s.step(r)
				if r.active == 0 {
					s.finish(r)
					break
				}
				nt := r.ws[0].t
				if len(s.h) == 0 || nt <= s.h[0].t {
					continue
				}
				s.h.push(item{t: nt, kind: evReq, idx: r.slot})
				break
			}
		case evPool:
			p := s.pools[it.idx]
			if it.ver != p.ver {
				continue
			}
			s.runPool(p, it.t)
		case evArrive:
			lt := ListMin + (ListMax-ListMin)*s.rList.Float64()
			s.h.push(item{t: it.t + lt, kind: evStart, aux: it.t})
			s.h.push(item{t: s.nextArrival(it.t), kind: evArrive})
		case evStart:
			s.newReq(kindList, s.rPod.IntN(SyncPods), nil, it.t, it.aux)
		case evSync:
			p := int(it.idx)
			s.newReq(kindSync, p, s.sync[p], it.t, it.t)
			s.h.push(item{t: it.t + SyncPeriod*(0.9+0.2*s.rSync.Float64()), kind: evSync, idx: it.idx})
		case evDrop:
			s.drop(cfg.Events[it.idx].Shards)
		case evSample:
			s.Cohorts = append(s.Cohorts, s.sampleCohorts(it.t))
			s.h.push(item{t: it.t + 60, kind: evSample})
		case evRow:
			s.sampleRow(it.t)
			s.h.push(item{t: it.t + CycleP, kind: evRow})
		case evSwitch:
			s.pol = cfg.Policy
			for _, p := range s.pools {
				p.max = s.pol.RefreshPool
			}
		}
	}
}

func (s *Sim) finish(r *Req) {
	rec := ReqRec{ID: r.id, Kind: r.kind, Arrive: r.arrive, Start: r.start, End: r.end, Hits: r.hits, Misses: r.misses}
	if r.kind == kindList {
		s.ReqLog = append(s.ReqLog, rec)
	} else {
		s.SyncLog = append(s.SyncLog, rec)
	}
	s.reqs[r.slot] = nil
	s.free = append(s.free, r.slot)
}

// sampleCohorts measures expiry-phase clusters of the live keys at time t.
// Phase = expiry mod CycleP.
// Method A (gap): phases on a circle, split on gaps > 20 s, count clusters > 1000 keys.
// Method B (density): 1 s phase bins, 5 s moving sum, a bin is dense above 2x the uniform level,
// count runs of dense bins that hold > 1000 keys.
func (s *Sim) sampleCohorts(t float64) CohortRec {
	c := &s.c
	h := s.hist
	h1 := s.hist1
	for i := range h {
		h[i] = 0
	}
	for i := range h1 {
		h1[i] = 0
	}
	live := 0
	for k := 0; k < N; k++ {
		if c.exp[k] > t {
			b := phaseBin(c.exp[k])
			h[b]++
			h1[b/10]++
			live++
		}
	}
	rec := CohortRec{T: t, Live: live}
	if live == 0 {
		return rec
	}
	// Method A: label every 0.1 s bin with a cluster id.
	nb := len(h)
	const gapBins = 200
	var nz []int
	for i, v := range h {
		if v > 0 {
			nz = append(nz, i)
		}
	}
	gapBefore := func(j int) bool {
		if len(nz) == 1 {
			return true
		}
		d := nz[j] - nz[(j-1+len(nz))%len(nz)]
		if d <= 0 {
			d += nb
		}
		return d > gapBins
	}
	split := -1
	for j := range nz {
		if gapBefore(j) {
			split = j
			break
		}
	}
	lab := s.lab
	for i := range lab {
		lab[i] = -1
	}
	var sizes []int
	if split < 0 {
		split = 0 // no gap: one cluster
	}
	cur := -1
	for jj := 0; jj < len(nz); jj++ {
		j := (split + jj) % len(nz)
		if jj == 0 || gapBefore(j) {
			sizes = append(sizes, 0)
			cur++
		}
		lab[nz[j]] = int16(cur)
		sizes[cur] += int(h[nz[j]])
	}
	order := make([]int, len(sizes))
	for i := range order {
		order[i] = i
	}
	sortIdxDesc(order, sizes)
	for _, ci := range order {
		if sizes[ci] > 1000 {
			rec.GapCount++
		}
	}
	rank := map[int16]int{}
	for r := 0; r < 3 && r < len(order); r++ {
		rec.GapTop[r] = sizes[order[r]]
		rank[int16(order[r])] = r
	}
	for k := 0; k < N; k++ {
		if c.exp[k] > t {
			if r, ok := rank[lab[phaseBin(c.exp[k])]]; ok {
				rec.Comp[r][c.shard[k]]++
			}
		}
	}
	for sh, keys := range s.byShard {
		prev, n, runs := int16(-2), 0, 0
		for _, k := range keys {
			if c.exp[k] <= t {
				continue
			}
			l := lab[phaseBin(c.exp[k])]
			if l < 0 || sizes[l] <= 1000 {
				continue
			}
			n++
			if l != prev {
				runs++
				prev = l
			}
		}
		if runs > 0 {
			rec.Runs[sh] = float64(n) / float64(runs)
		}
	}
	// Method B.
	m := len(h1)
	thr := 2 * 5 * float64(live) / float64(m)
	dense := make([]bool, m)
	any := false
	allDense := true
	for i := 0; i < m; i++ {
		sum := 0
		for d := -2; d <= 2; d++ {
			sum += int(h1[(i+d+m)%m])
		}
		dense[i] = float64(sum) > thr
		any = any || dense[i]
		allDense = allDense && dense[i]
	}
	if any && !allDense {
		st := 0
		for st < m && !(dense[st] && !dense[(st-1+m)%m]) {
			st++
		}
		cur := 0
		for ii := 0; ii < m; ii++ {
			i := (st + ii) % m
			if dense[i] {
				cur += int(h1[i])
			} else if cur > 0 {
				if cur > 1000 {
					rec.DenCount++
				}
				cur = 0
			}
		}
		if cur > 1000 {
			rec.DenCount++
		}
	}
	// Peak 30 s share.
	win := 0
	for i := 0; i < 30; i++ {
		win += int(h1[i])
	}
	best := win
	for i := 30; i < m+30; i++ {
		win += int(h1[i%m]) - int(h1[(i-30)%m])
		if win > best {
			best = win
		}
	}
	rec.Peak30 = float64(best) / float64(live)
	return rec
}

func phaseBin(exp float64) int {
	b := int(math.Mod(exp, CycleP) * 10)
	if b >= int(CycleP*10) {
		b = int(CycleP*10) - 1
	}
	return b
}

func sortIdxDesc(idx []int, v []int) {
	for i := 1; i < len(idx); i++ {
		for j := i; j > 0 && v[idx[j]] > v[idx[j-1]]; j-- {
			idx[j], idx[j-1] = idx[j-1], idx[j]
		}
	}
}

// sampleRow stores one cohort-map row: per position bin, the modal 5 s expiry phase and the modal writer.
// It also stores a faceted row: per server index, per position bin of that server's keys.
func (s *Sim) sampleRow(t float64) {
	c := &s.c
	cnt := make([]int, int(CycleP/5)+1)
	modal := func(keys []int32) (float32, int32) {
		for i := range cnt {
			cnt[i] = 0
		}
		wc := map[int32]int{}
		live := 0
		for _, k := range keys {
			if c.exp[k] > t {
				cnt[int(math.Mod(c.exp[k], CycleP)/5)]++
				wc[c.writer[k]]++
				live++
			}
		}
		if live*2 < len(keys) {
			return -1, -1
		}
		bq, bv := 0, -1
		for i, v := range cnt {
			if v > bv {
				bq, bv = i, v
			}
		}
		bw, bc := int32(-1), -1
		for w, v := range wc {
			if v > bc || (v == bc && w < bw) {
				bw, bc = w, v
			}
		}
		if s.isSyncWriter[bw] {
			bw = -3
		}
		return float32(bq*5) + 2.5, bw
	}
	ph := make([]float32, D1Bins)
	wr := make([]int32, D1Bins)
	keys := make([]int32, 0, N/D1Bins+2)
	for b := 0; b < D1Bins; b++ {
		keys = keys[:0]
		for k := b * N / D1Bins; k < (b+1)*N/D1Bins; k++ {
			keys = append(keys, int32(k))
		}
		ph[b], wr[b] = modal(keys)
	}
	fph := make([]float32, Shards*FacetBins)
	for sh := 0; sh < Shards; sh++ {
		ks := s.byShard[sh]
		for b := 0; b < FacetBins; b++ {
			fph[sh*FacetBins+b], _ = modal(ks[b*len(ks)/FacetBins : (b+1)*len(ks)/FacetBins])
		}
	}
	s.RowPh = append(s.RowPh, ph)
	s.RowWr = append(s.RowWr, wr)
	s.RowFacet = append(s.RowFacet, fph)
	s.RowT = append(s.RowT, t)
}
