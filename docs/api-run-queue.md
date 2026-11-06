# Bounded API run admission

`POST /v1/runs` accepts excess work into a FIFO when the configured concurrent
run limit is occupied. The existing default remains **10**. At most **100 waiting
runs per API adapter/process** are accepted; a full queue returns HTTP 429 with
`run_queue_full` and does not reserve the idempotency key. This does not increase
provider concurrency or queue `/v1/chat/completions` and `/v1/responses`.
Those synchronous surfaces retain backpressure and cannot bypass a waiting FIFO.

## Contract

- HTTP 202 with `status: queued` means the process owns the request, **not** that
  an agent is executing. Poll `GET /v1/runs/{id}` or subscribe to its events.
  Saturated runs publish `run.queued`; existing running/terminal states follow.
- Queue admission is bounded and serialized across requests, with FIFO ordering
  at admission, and an atomic event-loop check/claim of available slots. Waiting
  tasks stay in shutdown/drain inventory but do not count as executing workers.
- No agent or canonical session-authority admission is created while waiting.
  This matters: observing an already admitted session turn is too late to limit
  its execution. Unsaturated authority requests keep the existing synchronous
  durable admission and refusal behavior.
- Queued authority requests may fail at dispatch if their target or permissions
  changed while waiting. Their failed record remains owned and replayable.
- `/stop` of a queued run never constructs an agent or submits a canonical turn.
  Cancellation is observed within the scheduler's 50ms check interval. Disconnecting
  an SSE viewer or navigating away does **not** cancel a queued run.
- Idempotency keys retain existing scope/fingerprint validation. A retry of an
  accepted queued run reuses its run ID, including when the queue is full. A
  conflicting payload returns 409. Clients should supply an Idempotency-Key and
  keep it unchanged on ambiguous network retries.
- Completion, failure and queued cancellation release capacity. Running stops
  retain the existing cooperative executor lifecycle rather than freeing a slot
  merely because the client requested cancellation.

## Durability and boundaries

**The waiting payload/FIFO is process-local, not a durable restart-resuming job
queue.** With the existing durable idempotency store enabled, a dead owner's
nonterminal run is surfaced as `interrupted` on status/replay; it is not silently
re-executed. Without that store, a restarted process can forget run IDs. Clients
must retain the submitted text and treat missing runs as interrupted, not successful.
The stream buffer's existing TTL and idempotency retention still apply.

The cap and queue are per adapter/process, not a distributed cluster budget.
Independent adapters, external CLI/Desktop/gateway turns, and other processes are
not a new shared global semaphore. No new same-session serialization is introduced.
A hung running worker can occupy its slot indefinitely; there is no queue timeout.

## Verification

`tests/gateway/test_api_run_queue.py` uses real HTTP start/status/stop/SSE endpoints
with controlled workers: ten occupied slots, FIFO handoff, cancellation, bounded
rejection, exact replay, conflicting retry and viewer disconnect. It also covers
shutdown interruption and dispatch failure cleanup.
`test_api_run_queue_authority.py` uses a real SessionDB/authority to prove queued
requests do not enter the canonical execution FIFO until capacity is available,
and a cancelled queued request never gets admitted.

No live configuration or service restart is required to review these changes.
Deployment is a separate operator action and must retain the existing cap of 10.
