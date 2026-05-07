# inference-router

A Linux TCP request router in C++20. Accepts client requests on the front door, dispatches
each through a fixed thread pool, and forwards them over a thread-safe connection pool to
backend inference workers. Handles `SIGTERM` with a drain protocol that — under the
documented invariant and verified by a chaos test — drops zero in-flight requests.

The wire is binary: a 4-byte big-endian length prefix followed by an opaque payload.
The router never parses payload bytes — it transports them.

## Latest chaos result

Most recent committed run from `bench/chaos-result.json`
(50 clients × 100 requests, drain triggered at t=5s):

| metric | value |
| --- | --- |
| accepted | 994 |
| completed | 994 |
| errored | 0 |
| **dropped** | **0** |
| client connect-failed (post-drain attempts) | 4006 |
| sigterm trigger | t = 5004 ms |
| total run | 5284 ms |

`accepted` is the count of connections the router handed off to a worker after `accept4()`
returned. `client_connect_failed` is post-drain dial attempts that hit a closed listening
socket — those connections were never accepted and don't count toward the no-drop
invariant.

The CI job `chaos-smoke` enforces `dropped_total == 0` on every push.

## What this studies

- **TCP request routing under shutdown stress.** The acceptor closes its listening socket
  on `SIGTERM`; in-flight requests run to completion; only then do worker threads stop and
  backend connections close. The grace deadline (default 30s, `--shutdown-grace`) is the
  *only* documented path to a non-zero `dropped`.
- **Backend connection-pool design.** `idle_timeout`, `max_lifetime`, and a periodic
  health check together control connection churn (see [docs/pool.md](docs/pool.md)).
- **Hybrid reactor.** A single epoll-based acceptor thread + N blocking worker threads.
  Why this and not full async? See [docs/reactor.md](docs/reactor.md).

## Modules

| Path | What |
| --- | --- |
| `src/acceptor.{h,cpp}` | Single-threaded epoll acceptor. Hands new conns to the pool. |
| `src/thread_pool.{h,cpp}` | Bounded MPMC thread pool (mutex + condvar). |
| `src/handler.{h,cpp}` | Per-request: read, borrow backend, forward, write. |
| `src/backend_pool.{h,cpp}` | Connection pool with idle timeout, max lifetime, health check. |
| `src/connection.{h,cpp}` | Length-prefixed wire protocol + dial/listen helpers. |
| `src/shutdown.{h,cpp}` | `SIGTERM`/`SIGINT` coordinator + drain. |
| `src/metrics.{h,cpp}` | Atomic counters (accepted/completed/errored/dropped/in_flight). |
| `tools/backend_echo` | Tiny echo backend for tests + docker-compose. |
| `tests/unit` | GoogleTest suites per module. |
| `tests/integration` | E2E in-process router + backend stubs. |
| `tests/chaos/chaos.cpp` | The load-bearing test: drives traffic, triggers drain, asserts `dropped == 0`. |
| `bench/load.cpp` | Throughput + percentile-latency micro-benchmark. |

## Quickstart

```sh
make build
make test
make chaos             # full-size chaos run, writes bench/chaos-result.json
make bench             # 5-second throughput + p50/p95/p99/p999 run
```

ASan + UBSan:

```sh
make test-asan
```

Docker:

```sh
make docker
docker compose up
```

## Architecture

```
              ┌─────────────────────────────────────────────────────────────┐
              │ inference-router process                                    │
              │                                                             │
  TCP client ─┼─▶ acceptor (epoll on listen_fd + eventfd)                   │
              │       │                                                     │
              │       ▼                                                     │
              │   bounded MPMC queue ──▶ N worker threads                   │
              │                              │                              │
              │                              ▼                              │
              │                   handler.handle_one(client_fd):            │
              │                     read req → borrow backend conn          │
              │                     write req → read resp → write back     │
              │                     return conn to pool                     │
              │                              │                              │
              │                              ▼                              │
              │                       BackendPool ──▶ TCP backend workers   │
              │                                                             │
              │   shutdown coordinator (SIGTERM):                           │
              │     1. acceptor.stop()                                      │
              │     2. wait for in_flight==0 (up to --shutdown-grace)       │
              │     3. pool.stop()                                          │
              │     4. backend_pool.shutdown()                              │
              └─────────────────────────────────────────────────────────────┘
```

## What this is *not*

- **Not HTTP.** The wire is binary length-prefixed only. If you want HTTP routing, see the
  many existing reverse proxies (envoy, nginx, traefik, haproxy).
- **Not a model server.** The "backend workers" are TCP echo stubs. In production you
  would point `--backend` at a real model server (Triton, vLLM, TGI, etc).
- **Not TLS.** Plaintext sockets only. mTLS is a documented swap, not in this repo.
- **Not Kubernetes-aware.** No service-discovery, no service mesh, no sidecar. The
  backend list is static at startup.
- **Not weighted/least-conn load balancing.** Round-robin via the pool is the only
  strategy. Weighted routing is future work.
- **Not a job/task lifecycle manager.** That belongs to a different repo
  (`SAY-5/job-controller`). This is hot-path *transport*.
- **Not lock-free.** The thread pool and backend pool both use `std::mutex` + condvar.
  At the queue depths and request rates this study cares about, mutex contention is below
  the I/O noise floor — see [docs/reactor.md](docs/reactor.md) for the sizing argument.

## Documentation

- [docs/shutdown.md](docs/shutdown.md) — drain protocol, the no-drop invariant, what the
  grace deadline does
- [docs/pool.md](docs/pool.md) — connection pool design and the `idle_timeout` vs
  `max_lifetime` distinction
- [docs/reactor.md](docs/reactor.md) — why epoll + blocking workers, not full async
- [ARCHITECTURE.md](ARCHITECTURE.md) — full design notes

## License

[MIT](LICENSE).
