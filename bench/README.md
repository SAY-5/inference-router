# bench/

Two benchmark binaries and their committed result artifacts.

## `load_bench` (built from `bench/load.cpp`)

Drives a fixed number of client threads against the router, measuring throughput and
percentile latency. By default it spawns an in-process router + echo backend so the
binary is self-contained.

```sh
./build/load_bench --concurrency 8 --duration-s 5
./build/load_bench --concurrency 8 --requests 1000 --out bench/bench-result.json
```

Output JSON shape:

```json
{
  "schema": "inference-router.bench.v1",
  "ok": 12345,
  "err": 0,
  "total_ms": 5001,
  "throughput_rps": 2468.9,
  "p50_us": 320,
  "p95_us": 600,
  "p99_us": 900,
  "p999_us": 1500,
  "concurrency": 8,
  "payload_bytes": 64
}
```

## `chaos` (built from `tests/chaos/chaos.cpp`)

The load-bearing test. Runs N clients × M requests through the full router and triggers
the drain protocol mid-run. Asserts `dropped_total == 0`.

```sh
./build/chaos --clients 50 --requests 100 --sigterm-after-ms 5000
```

Result JSON:

```json
{
  "schema": "inference-router.chaos.v1",
  "verdict": "pass",
  "dropped_total": 0,
  "accepted_total": ...,
  "completed_total": ...,
  "errored_total": ...,
  "client_sent": ...,
  "client_got_response": ...,
  "client_connect_failed": ...,
  "client_io_failed": ...,
  ...
}
```

The `bench/chaos-result.json` committed to this repo is the most recent local run.
