# Backend connection pool

## Goal

Keep a small set of warm TCP connections to a backend `(host, port)` so the request hot
path is `borrow → write → read → release`, not `dial → write → read → close`. Bound the
total connection count so a backend doesn't get fd-exhausted during a traffic spike.

## Knobs

| flag / option | default | what |
| --- | --- | --- |
| `max_size` | 16 | hard upper bound on (idle + in-use) conns |
| `min_idle` | 2 | optional; the reaper will not kill below this if traffic is high — current implementation enforces it as a soft floor only when reaping |
| `idle_timeout` | 60s | close conns whose `last_used` is older than this |
| `max_lifetime` | 5min | close conns whose `created` is older than this, even if active |
| `health_check_interval` | 10s | reaper tick rate |
| `connect_timeout` | 2s | dial deadline |
| `borrow_timeout` | 2s | how long `borrow()` waits when pool is full |

## Borrow / release

```
borrow():
  for as long as borrow_timeout has not elapsed:
    if shutting_down: return -1
    if idle is non-empty: pop, mark in-use, return fd
    if (idle + in_use) < max_size:
      drop the lock, dial, return fd (in-use accounted for before dial)
    wait on condvar with the borrow deadline

release(fd, ok):
  if !ok or shutting_down: close fd, decrement in_use
  else: push fd to idle, decrement in_use, notify one waiter
```

Important: `dial_one_()` runs without the pool lock held. We pre-increment `in_use_` and
decrement on failure. Without this, a slow dial would block every other concurrent
borrower behind the mutex.

## The reaper

Runs in its own thread. Each tick:

1. Lock the pool. Walk the idle list. For each entry:
   - if `now - last_used > idle_timeout` → mark for close
   - if `now - created > max_lifetime` → mark for close
2. Move all surviving idle conns to a local `to_check` queue (still under lock), reset
   `idle` to empty.
3. Drop the lock. Close the dead conns. For each survivor, send a zero-length
   length-prefixed message and read a zero-length reply. Survivors stay; unresponsive
   conns are closed.
4. Re-acquire the lock and push survivors back to `idle`.

The reaper does I/O off-lock, but during the few milliseconds it holds the empty `idle`
list, concurrent `borrow()` calls fall through to dialing new conns. That's a deliberate
trade for never doing socket I/O under the mutex.

## Health check

The health check uses the same wire protocol the router uses for real traffic: a
length-prefixed message. The backend stub (`tools/backend_echo/main.cpp`) echoes whatever
length it receives, including zero. So the ping is `len=0` out → `len=0` back, with no
payload bytes on either side. A real backend would need to grow a similar idle-ping
contract.

## What this pool does not do

- **No per-host weighted routing.** One pool per `(host, port)`. Multi-destination
  routing would build a thin wrapper that selects a pool per request.
- **No circuit breaking.** A backend that returns errors fast will still get hammered.
  Adding a half-open / open-circuit state is straightforward but isn't here.
- **No prepared connections.** Conns aren't pre-warmed to `min_idle` at startup; they
  get created on first borrow. Pre-warming is a cheap addition.
- **No connection-level concurrency.** Each conn handles exactly one in-flight request
  at a time. Multiplexing (HTTP/2-style stream IDs over a single TCP) would require a
  protocol extension.
