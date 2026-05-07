# Reactor: epoll acceptor + blocking workers

## The hybrid choice

Two design extremes:

- **Fully blocking, thread-per-connection.** Easy to read, easy to reason about. Falls
  apart at hundreds of concurrent conns because each thread costs ~1 MB stack + scheduler
  pressure.
- **Fully async, single thread + state machine.** Scales to many conns per thread, but
  the request path becomes a callback chain or a coroutine state machine, and the drain
  protocol becomes "track which state every conn is in" instead of "wait for the
  in_flight counter to hit zero."

This repo lands in the middle:

| component | model |
| --- | --- |
| acceptor | event-driven (epoll), one thread |
| worker pool | bounded thread pool, blocking I/O per worker |
| backend pool | thread-safe, blocking I/O |

The acceptor is event-driven because it has two fds to watch (the listening socket and
the wake eventfd) and the natural way to wait on both is `epoll_wait`. Everything past
the queue is blocking because the per-request work is short and serial.

## Sizing argument

A worker thread spends most of its time in:

- `recv` from the client (reading the request)
- `send` to the backend
- `recv` from the backend (waiting for the model's response — this dominates)
- `send` to the client

For an inference workload where backend round-trip is anywhere from milliseconds to
seconds, the user-space cost of mutex/condvar contention on the dispatch queue (a few
hundred nanoseconds per push/pop) is well below the noise floor. The thread-pool
abstraction does NOT need to be lock-free.

The hard cap is **fd count and memory**, not throughput. With 16 workers and a 1 MB
default stack, the router fits in 16 MB of thread overhead before any I/O buffers. Linux
default `RLIMIT_NOFILE` is 1024; that bounds (clients + backend conns + bookkeeping)
and is the practical reason `pool_size + threads` is small.

## Why not Boost.Asio / liburing / seastar?

- **Boost.Asio.** Pulls in Boost. The wire and reactor logic here are 200 LOC of
  syscalls; the Boost dependency tree is weight we don't need.
- **liburing.** A natural fit for accept + read + write batched together, but it raises
  the kernel-version floor (5.1+) and the complexity ceiling. The chaos test would still
  need to exercise drain. Future work.
- **seastar.** Fantastic for shared-nothing, single-thread-per-core throughput. Wrong
  abstraction for what this study is about.

## EINTR retry policy

`accept4`, `epoll_wait`, `recv`, `send`, `poll`, `close` all retry on EINTR. The signal
handler (`src/shutdown.cpp`) deliberately does NOT set `SA_RESTART`, because we *want*
syscalls to surface EINTR — that's how the acceptor wakes up faster than the 1s
`epoll_wait` timeout when `SIGTERM` arrives. The eventfd wake is the primary mechanism;
EINTR is the fallback.
