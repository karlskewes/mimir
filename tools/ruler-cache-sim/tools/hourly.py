import csv, sys, collections
d = sys.argv[1]
step = float(sys.argv[2]) if len(sys.argv) > 2 else 3600
ser = list(csv.DictReader(open(d + '/series15s.csv')))
coh = list(csv.DictReader(open(d + '/cohorts.csv')))
agg = collections.defaultdict(lambda: [0, 0, 0, 0, 0])
for r in ser:
    b = int(float(r['t']) // step)
    a = agg[b]
    a[0] += int(r['lookups']); a[1] += int(r['hits']); a[2] += int(r['gets']); a[3] += int(r['adds']); a[4] += int(r['not_stored'])
cg = collections.defaultdict(list)
for r in coh:
    cg[int(float(r['t']) // step)].append((int(r['gap_count']), int(r['den_count']), int(r['gap_top1']), int(r['gap_top2']), int(r['gap_top3'])))
for b in sorted(agg):
    a = agg[b]; c = cg.get(b, [])
    gm = sum(x[0] for x in c) / max(len(c), 1); dm = sum(x[1] for x in c) / max(len(c), 1)
    tops = c[len(c) // 2][2:] if c else ()
    print(f"h{b*step/3600:5.1f} miss/h={(a[0]-a[1])*3600/step:8.0f} gets/h={a[2]*3600/step:8.0f} ns={a[4]/max(a[3],1):.3f} gapA={gm:.1f} denB={dm:.1f} tops={tops}")
