# Architecture

This document covers the design decisions that aren't obvious from skimming the headers.

## The shutdown protocol and the no-drop invariant

The defining claim of this project is: **a request that the acceptor handed to a worker
thread will receive a response, even when `SIGTERM` arrives mid-flight, as long as the
in-flight set drains within the grace deadline.**

State machine:

```
        accept loop running
       ┌───────────────────┐
       │                   │
       ▼                   │
  RUNNING ──SIGTERM──▶ DRAINING ──in_flight==0──▶ STOPPING ──pool joined──▶ EXIT
                          │
                          └──grace deadline──▶ FORCED_DROP ──▶ STOPPING ──▶ EXIT
                                (in_flight counted as dropped)
```

What each phase actually does:

1. **RUNNING.** The acceptor thread is blocked in `epoll_wait`. Workers are pulling tasks
   off the queue and executing them.
2. **`SIGTERM` lands.** The signal handler (`src/shutdown.cpp`) is async-signal-safe — it
   sets `requested_` (an `std::atomic<bool>`) and returns. It does **not** call
   `notify_all` from the handler, because `condition_variable::notify_all` is not
   async-signal-safe; the main thread polls the flag with a 1s timeout instead.
3. **DRAINING.** The main thread calls `Acceptor::stop()`, which writes one byte to the
   eventfd, waking `epoll_wait`, then closes the listening socket. New `accept()` calls
   stop immediately. Already-queued connections are still processed — the worker pool is
   *not* stopped here.
4. The main thread polls `metrics.in_flight()` and sleeps 20ms between samples.
5. When `in_flight == 0`, the pool is stopped (`pool.stop()` joins workers — by this
   point the queue is empty so there's nothing to drain).
6. **`backend_pool.shutdown()`** closes all idle connections and refuses new borrows.
   In-use conns released by stragglers are closed on return.
7. The process exits.

The grace deadline is the override: if `in_flight > 0` after `--shutdown-grace`
milliseconds, the main thread increments `dropped` for each remaining in-flight request
and exits. This is a deliberate, flag-explicit override, not a bug, and it is the only
documented path to `dropped > 0`.

The chaos test (`tests/chaos/chaos.cpp`) exercises the DRAINING path with a heavy load
and asserts `dropped == 0` against a real run.

## Connection pool math

`BackendPool` (in `src/backend_pool.h`) holds two timeouts at once:

| timeout | what it controls | why both? |
| --- | --- | --- |
| `idle_timeout` (default 60s) | Close idle conns that haven't been used recently. | Frees backend resources when traffic is low. |
| `max_lifetime` (default 5min) | Recycle conns even if active. | Stale TCP, half-closed peers, NAT/firewall idle expiry, or rotated TLS keys (if you swap in TLS). |

The reaper thread runs at `min(idle_timeout, health_check_interval)`. Each tick it:

1. Closes any idle conn whose `last_used` is older than `idle_timeout` OR whose `created`
   is older than `max_lifetime`.
2. Pings every remaining idle conn (zero-length write + zero-length read). Survivors are
   kept; unresponsive conns are closed.

The reaper does I/O outside the pool mutex — it pops the idle set, pings, and re-pushes
the survivors. Borrows during a reaper sweep go through `dial_one_()` and create a new
conn. This trades a brief uptick in connection creates for a guarantee that the pool
isn't holding stale fds.

## Why epoll for accept, blocking for workers

A pure async design (everything driven off epoll) would let one thread service many
concurrent requests. We chose against it for three reasons:

1. **The hot path is I/O-bound on the backend round-trip.** A blocking worker spends 95%+
   of its time in `recv` from the backend, not in user-space scheduling. Adding async
   state machines on top of that doesn't help latency much.
2. **The handler is linear.** `read req → borrow → write → read → write → release` is six
   sequential steps. Expressing it as state-machine callbacks would balloon the line count
   for no clarity gain.
3. **Reasoning about cancellation is harder in async.** The drain protocol's "let
   in-flight finish" is trivial to express when each request owns one worker thread; in
   async it's a state-table edit.

The acceptor *is* event-driven (epoll on `listen_fd` + an eventfd for the wake signal)
because `accept()` is the only place we genuinely need to wait on multiple fds at once.

## Wire protocol decision

Length-prefixed binary (`uint32 len` big-endian, then `len` bytes payload). Why not HTTP?

- **The router doesn't parse the payload.** HTTP framing buys nothing here.
- **HTTP is multi-message-per-conn by default.** The handler-per-conn model (one
  request, one response, then close) is the simplest mental model for the no-drop
  invariant. Pipelining/keep-alive would require tracking which response the client is
  waiting on across multiple round-trips.
- **Length-prefixed is smaller and more predictable.** No header parsing, no chunked
  encoding edge cases, no CRLF interpretation.

A real production deployment would likely have HTTP at the front and length-prefixed
internally, or gRPC throughout. That's a layering concern outside this repo.

## ASan + UBSan as test discipline

The CI job `build-asan` configures the build with `-fsanitize=address,undefined` and runs
the full unit + integration suite under it. ASan catches use-after-free on the connection
fds (which is the most likely class of bug in code that owns lots of fds across threads).
UBSan catches the integer narrowing and signed-overflow paths in the size_t / int32
length math.

Tests run with `ASAN_OPTIONS=detect_leaks=0` because GoogleTest's process-fork test mode
plays poorly with leak detection; GoogleTest itself isn't compiled with ASan in our CI.

## What's deliberately not here

- **No HTTP layer.** Documented in the README's "What this is *not*" section.
- **No TLS.** Plaintext only. Adding TLS means swapping the `connection.cpp` read/write
  helpers for an OpenSSL/BoringSSL-backed pair.
- **No service discovery.** Backend list is `--backend host:port` at startup, single
  destination per pool. Real deployments would feed in a discovery client (Consul,
  k8s endpoints API, etc.) and rotate destinations per borrow.
- **No load-balancing strategies.** Round-robin via pool turnover is the only strategy.
  Weighted routing, least-connections, p2c, etc. are future work.
- **No rate limiting / admission control.** A bounded queue rejects when full
  (`--reject-on-full`), but that's not the same as token-bucket limiting or per-tenant
  quotas.
- **No tracing/OpenTelemetry.** Metrics are in-memory counters. Wiring them to OTLP or
  Prometheus is a code change at one site (`src/metrics.cpp`).
- **No structured config file.** All config is CLI flags. This keeps the binary's startup
  surface small and the documentation easy to keep in sync.

## Why this is split off from `job-controller`

The other side of the workload — *which* model server to schedule a request to, when to
spin up workers, capacity planning — lives in a separate repo
(`SAY-5/job-controller`). Inference-router is the *transport hot path*: a request comes
in, a request goes to a backend, a response comes back. The control plane does not run
on this thread.
