# Re-architect concurrency and thread ownership across app, consumer, and publishers

## Summary
The current concurrency model mixes process-global shutdown state, class-shared publisher runtime state, pooled connection ownership for long-lived replication threads, and overlapping lifecycle control between `PGWAL`, `WALConsumer`, and publisher backends.

This redesign preserves the intentional current behavior of advancing `flush_lsn` after successful local publisher handoff rather than after downstream broker confirmation. The work focuses on instance isolation, deterministic lifecycle management, bounded buffering, visible degraded-state handling, and removal of redundant thread state.

## Goals
- Isolate concurrency state per `PGWAL` instance and per publisher instance.
- Preserve early `flush_lsn` advancement after publisher handoff.
- Remove class-shared publisher queues and related shared worker state.
- Define one clear lifecycle for app, consumer, and publishers.
- Keep destination outages from blocking PostgreSQL WAL progress.
- Make unhealthy publisher state observable to the application.
- Keep ordering best-effort and simple for a single consumer thread.

## Non-Goals
- No change to `WALReplicationOpts` or wal2json option modeling.
- No guaranteed end-to-end durability before `flush_lsn`.
- No parallel fan-out across publishers in this pass.
- No new async/await API.
- No persistence or disk-backed spool for failed publishers.
- No automatic app shutdown on prolonged publisher outage by default.

## Current State
- `pgwal.events.EXIT` is a module-global `threading.Event`.
- `PGWAL.consume()` registers a raw background thread.
- `PGWAL.run()` sets global exit state and joins task threads.
- `WALConsumer.consume_async()` calls `start_replication()` and loops until global exit clears.
- `KafkaPublisher` and `RabbitPublisher` use class-level `_MSG_QUEUE`.
- Publisher worker lifecycle is partially tracked, but mutable runtime state still leaks across instances.
- Consumer feedback is advanced after `publisher.publish(msg)` returns, which today means local handoff, not broker confirmation.
- Tests already compensate for class-shared queue leakage by draining publisher queues in fixtures.

## Required Changes
### 1. Replace global lifecycle state with app-owned supervision
- Delete the use of global `EXIT` from runtime control flow.
- `PGWAL` owns a per-instance `stop_event`.
- `PGWAL` owns app state: `idle`, `starting`, `running`, `stopping`, `stopped`, `failed`.
- `PGWAL` owns registered consumer handles and publisher instances.
- `PGWAL.consume()` returns `ConsumerHandle` instead of `threading.Thread`.
- `ConsumerHandle` must include:
  - `consumer`
  - `thread`
  - `stop_event`
  - `state`
  - `last_error`
  - `stop()`
  - `join(timeout=None) -> bool`

### 2. Remove pooled connections from the replication path
- `PGWAL` must stop using `ThreadedConnectionPool` for logical replication consumers.
- Each consumer thread opens exactly one `LogicalReplicationConnection`.
- That connection is created in the consumer runner and closed in the same runner.
- Each consumer owns its replication cursor for the full run.
- Connection lifecycle must be per-thread and non-shared.

### 3. Simplify consumer control flow
- Merge `PGWAL._consume()` and `WALConsumer.consume_async()` into one clear runner path.
- `start_replication()` is called exactly once per consumer run.
- Remove `_consuming`.
- Remove the class-level consumer lock.
- Add per-consumer state:
  - `idle`
  - `starting`
  - `running`
  - `stopping`
  - `stopped`
  - `failed`
- Keep the existing `select()` based wait model unless a concrete implementation issue requires a narrower change.
- `WALConsumer.stop()` must only affect that consumer instance.

### 4. Preserve early WAL advancement, but formalize handoff semantics
- The library must continue sending feedback after successful publisher handoff, not after broker delivery confirmation.
- Define successful handoff as:
  - synchronous publisher: `publish()` returns without error
  - threaded publisher: `publish()` admits the message into that publisher instance queue
- If all publishers report successful handoff, send `flush_lsn=msg.data_start`.
- If any publisher rejects handoff because it is stopping or cannot admit the message, do not advance feedback for that message and transition the consumer to failed state.
- Keep publisher fan-out sequential in configured order.

### 5. Refactor `BasePublisher` to per-instance runtime state
- Move all mutable runtime fields from class scope to instance scope.
- This includes:
  - queue
  - running/stopping flags
  - thread handle
  - thread lock
  - metrics
  - last error
- Remove class-level `_MSG_QUEUE`.
- Remove class-level `_running`.
- Keep worker-thread ownership per publisher instance.
- Define publisher state:
  - `idle`
  - `starting`
  - `running`
  - `degraded`
  - `stopping`
  - `stopped`
  - `failed`
- Standardize the publisher API around:
  - `start()`
  - `publish(msg) -> PublishResult`
  - `stop(drain: bool = False)`
  - `wait_stopped(timeout: float | None = None) -> bool`
  - `state`
  - `metrics`

### 6. Define `PublishResult`
Use a small result object with:
- `accepted: bool`
- `dropped: bool`
- `reason: str | None`

Semantics:
- `accepted=True` means the consumer may count this publisher as a successful handoff.
- `dropped=True` is only used when queue policy intentionally evicts another buffered message to admit the new message.
- `accepted=False` means the publisher could not accept the message and consumer feedback must not advance.

### 7. Use bounded instance queues for threaded publishers
- Threaded publishers must use bounded per-instance queues.
- Default queue policy is `drop_oldest`.
- On full queue:
  - evict one oldest queued item
  - admit the new item
  - increment dropped counter
  - set publisher state to `degraded`
  - return `PublishResult(accepted=True, dropped=True, reason="queue_full_drop_oldest")`
- Do not block the consumer thread waiting for queue capacity.
- Do not use unbounded `SimpleQueue` in threaded publishers after the redesign.

### 8. Backend-specific implementation rules
#### `ShellPublisher`
- Synchronous only.
- No worker thread.
- No internal queue.
- No-op `start()`.
- `publish()` logs and returns `accepted=True` unless stopping/closed.

#### `KafkaPublisher`
- Keep one worker thread per instance.
- That worker owns the `KafkaProducer`.
- The consumer thread only performs queue admission.
- Worker drains the instance queue and calls `producer.send(...)`.
- Broker acks are not waited on by the consumer path.
- Send exceptions, flush exceptions, and producer close exceptions must update state/metrics and `last_error`.
- Publisher remains running unless stop is requested; prolonged outage degrades state rather than stopping the app.

#### `RabbitPublisher`
- Keep one dedicated Pika I/O thread per instance.
- Replace class-shared queue with instance-owned bounded queue.
- The worker thread owns the Pika connection, channel, and ioloop.
- The worker drains the queue and publishes on the broker thread.
- Confirms remain for metrics and health only, not WAL feedback gating.
- Disconnects trigger reconnect flow while the app keeps running.
- Repeated queue churn while disconnected increases dropped counters and keeps publisher in `degraded`.

## Public API / Interface Changes
- `PGWAL.consume()` return type changes from `threading.Thread` to `ConsumerHandle`.
- `PGWAL` gains `stop()` and `close()`.
- `pgwal.events.EXIT` is removed from runtime API usage.
- `BasePublisher.publish()` returns `PublishResult`.
- Threaded publishers accept optional queue sizing config in constructors.
- Stubs must be updated to reflect all changed public signatures.

## Failure Visibility
- Prolonged publisher disconnection does not auto-stop the app.
- The application must be able to inspect publisher health through code, not only logs.
- Each publisher exposes metrics with at least:
  - `queued_count`
  - `queue_capacity`
  - `dropped_count`
  - `publish_error_count`
  - `reconnect_count`
  - `last_error`
  - `last_success_at`
- `PGWAL` should expose a helper to collect current consumer and publisher states for diagnostics.

## Acceptance Criteria
- No class-level mutable queue state remains in Kafka or Rabbit publishers.
- Stopping one `PGWAL` instance does not affect another instance in the same process.
- Each consumer uses a dedicated replication connection.
- `start_replication()` is called once per consumer run.
- Consumer feedback still advances after publisher handoff, not broker confirmation.
- Threaded publisher queue saturation follows `drop_oldest`.
- Publisher degradation is visible through state and metrics.
- Shutdown is deterministic for app, consumers, and publishers.
- Checked-in `.pyi` files are synchronized with the new API.
- Architecture docs are updated after implementation.

## Validation
- `python -m pytest tests/test_consumers.py -vv`
- `python -m pytest tests/test_kafka_publisher.py -vv`
- `python -m pytest tests/test_rabbitmq_publisher.py -vv`
- `python -m pytest tests/test_sync_stubs.py -vv`
- `python -m mypy pgwal`
- `python scripts/sync_stubs.py --check`
- `make run_tests`
- `python scripts/update_project_structure.py`

Also add or update tests for:
- publisher instance isolation
- queue saturation with `drop_oldest`
- app instance isolation
- consumer lifecycle transitions
- failure-state reporting without app shutdown

## Risks
- Changing `PGWAL.consume()` return type is a real API break.
- Removing pooled replication connections may affect tests or callers that relied on pool internals.
- Queue saturation semantics intentionally allow loss, so tests must assert visibility of drops rather than absence of loss.
- RabbitMQ reconnect logic must not race with stop logic.
- Kafka worker shutdown must not leave producer resources open.

## Default Decisions
- Spec filename: `THREAD1.md`
- Spec directory: `specs/`
- Default queue policy: `drop_oldest`
- Failure model: `state + metrics`
- WAL feedback policy: advance after successful local publisher handoff
- Consumer fan-out mode: sequential
- App shutdown policy: explicit stop only, not auto-stop on unhealthy destination

## Notes For The Implementer
- Keep the first implementation pass surgical around concurrency ownership and runtime state.
- Do not add durability features that conflict with early `flush_lsn`.
- Prefer removing redundant state over preserving compatibility with current internal implementation details.
- Update docs only after the code and stubs reflect the new runtime model.

## Assumptions
- Beta status permits breaking low-level APIs where needed to make lifecycle and concurrency correct.
- Current early `flush_lsn` behavior is an intentional product constraint, not a bug.
- A single new spec file is preferred over splitting this redesign across multiple spec files because the app/consumer/publisher thread model is one connected design.
