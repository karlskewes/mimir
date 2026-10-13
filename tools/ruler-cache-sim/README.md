# ruler-cache-sim

Discrete-event model of the ruler `rule-group` cache under ListRules polling.
The comments at the top of `sim.go` give the cache semantics and the constants.

## Run

- `go build -o rgsim .`
- `./rgsim -out out -scn S0,S3,S2b,S5 -mode warm -arr fixed,poisson -seeds 5`
- `./rgsim -out out -analyze -prod <dir>` (writes `report` next to the `-out` directory)
- `python3 tools/val.py out` (one line per run)
- `python3 tools/hourly.py out/<run> [step]` (miss rate and cohort counts per step)

## Scenarios

| Scenario | Change from S0 |
|---|---|
| S0 | none: TTL 600 s, 10 workers |
| S0ns | rule sync off |
| S1a | TTL × U(0.8, 1.0) |
| S1b | TTL × U(0.5, 1.0) |
| S2 | refresh-ahead at 0.9 × TTL, no worker limit |
| S2b | refresh-ahead at 0.9 × TTL, 10 workers per pod |
| S3 | 40 workers |
| S4 | a hit extends the TTL |
| S5 | S2b and S3 |

## Modes

- `-mode cold`: 48 h from an empty cache
- `-mode warm`: 24 h warm-up, then 24 h of server events, then the scenario for 48 h with 2 more server events
- `-arr fixed`: one request every 30 s ± 2 s
- `-arr fixedtight`: one request every 30 s ± 0.1 s
- `-arr poisson`: mean gap 30 s

## Production input

`-analyze` needs 4 Prometheus `query_range` JSON files in the `-prod` directory.
Replace `<sel>` with the cluster, namespace and ruler pod matchers.

| File | Query |
|---|---|
| `A_req_pod.json` | `sum by (pod, config) (rate(thanos_store_bucket_cache_operation_requests_total{<sel>}[1m]))` |
| `A_hit_pod.json` | `sum by (pod, config) (rate(thanos_store_bucket_cache_operation_hits_total{<sel>}[1m]))` |
| `A_add_pod.json` | `sum by (pod) (rate(thanos_cache_operations_total{<sel>, name="ruler-storage-cache", operation="add"}[1m]))` |
| `A_addns_pod.json` | `sum by (pod) (rate(thanos_cache_operation_failures_total{<sel>, name="ruler-storage-cache", operation="add", reason="not-stored"}[1m]))` |
