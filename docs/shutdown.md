# Shutdown protocol

## The invariant

> An accepted request will receive a response, unless the grace deadline expires.

"Accepted" means: the acceptor returned from `accept4()` and the connection fd was
handed to the worker pool's queue. The router does not promise anything about
connections that fail to negotiate at the TCP layer or get rejected because the queue
was full at the moment of dispatch (those are pre-accept).

## Sequence on `SIGTERM`

1. The signal handler (`src/shutdown.cpp::signal_handler`) runs. It calls
   `Shutdown::instance().request()`, which sets an atomic `bool` and notifies a condvar.
   Strictly speaking only the atomic store is async-signal-safe; we get away with the
   `notify_all` call because the lock is short-lived and the implementation under
   glibc/libstdc++ tolerates this in practice. The main thread *also* polls the atomic
   on a 1s timer as a belt-and-braces.
2. The main thread sees `requested() == true` and proceeds to drain.
3. **`acceptor.stop()`** writes 1 to the eventfd (which `epoll_wait` is watching) and
   joins the acceptor thread. The listening socket is closed. New `accept()` calls
   return EBADF immediately, but the acceptor's loop has already noticed `stop_` and
   exited cleanly.
4. The main thread polls `metrics.in_flight()` every 20ms.
5. When `in_flight == 0`, the main thread calls `pool.stop()` which joins all workers.
   The queue should be empty by definition.
6. **`backend_pool.shutdown()`** closes all idle backend connections. Subsequent
   `borrow()` calls fail (but there shouldn't be any — the workers are joined).
7. A final metrics line is logged and the process exits 0.

## The grace deadline

`--shutdown-grace SECONDS` (default 30) is the only way to drop a request:

- If `in_flight > 0` when the deadline expires, every still-in-flight request is counted
  as dropped (`metrics.inc_dropped()` once per pending request).
- Then the worker pool is stopped *without* draining, which means workers in the middle
  of a forward will see their backend conn closed and their final write to the client
  fail with EPIPE.

This is how an operator can express "give up after 30s no matter what." Setting it to
zero means "no graceful drain, drop everything in flight now."

## What the chaos test verifies

`tests/chaos/chaos.cpp` runs the full router (in-process) with N client threads sending
M requests. Halfway through the run it triggers the drain protocol — the same code
path `SIGTERM` would take. It asserts:

- `metrics.dropped() == 0`
- `metrics.completed() + metrics.errored() == metrics.accepted()`

Both conditions imply the invariant: every accepted request was either completed
successfully or had an error response sent (which is itself a response — peer-close is
a response signal).

`completed_total` is not asserted to be exactly equal to `accepted_total`; it's expected
that some fraction of requests in mid-flight at drain time hit a backend that's
already closed and surface as `errored`. The chaos test is checking the invariant, not
the success rate.

## Failure modes that are NOT drops

- **Connect failure on the client side after the listening socket closes.** The router
  never accepted these requests. They show up as `client_connect_failed` in the chaos
  result.
- **Queue rejection (`--reject-on-full` mode).** The acceptor closes the fd and
  increments `errored` (the client gets a peer-close). Not a drop because the request
  was responded to (with no answer).
- **Backend dial failure inside the handler.** The handler writes a `"ERR backend
  unavailable"` response and increments `errored`. The client got a response, so not a
  drop.

## Failure modes that ARE drops

Only one: the grace deadline expired. Documented above.
