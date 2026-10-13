import json, sys, glob
for f in sorted(glob.glob(sys.argv[1] + '/*/summary.json')):
    s = json.load(open(f))
    print('==', f.split('/')[-2], 'scenario: p50=%.1f p99=%.1f miss/h=%.0f ns=%.3f gapA=%.2f denB=%.2f' % (s['DurP50'], s['DurP99'], s['MissPerH'], s['NotStored'], s['GapMean'], s['DenMean']))
    v = s.get('Val')
    if v:
        print('  VAL: hit=%.4f miss/h=%.0f ns=%.3f bursts/615=%.2f burstNS=%.2f-%.2f peak=%.0f acf=%.0f/%.2f slow n=%d/%d dur %.1f-%.1f miss %.0f-%.0f p50dur=%.1f' % (
            v['HitRatio'], v['MissPerH'], v['NotStored'], v['BurstsPer615'], v['BurstNSMin'], v['BurstNSMax'], v['BurstPeakMax'], v['ACFLag'], v['ACFr'], v['SlowN'], v['NReq'], v['SlowDurMin'], v['SlowDurMax'], v['SlowMissMin'], v['SlowMissMax'], v['DurP50']))
        print('  comp@22h', s['ValComp'])
        print('  align', [round(x, 2) for x in s['ValAlign']])
