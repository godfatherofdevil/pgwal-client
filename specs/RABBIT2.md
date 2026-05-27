# Make publisher teardown deterministic for background broker publishers

## Summary
`RabbitPublisher` currently starts its broker loop on an untracked daemon thread through `ensure_running(...)`. Shutdown is only partially synchronous: `stop()` requests channel and connection closure, but the library does not own the worker thread lifecycle and callers cannot wait until the pika `SelectConnection` loop has fully exited.

This creates an intermittent teardown race in tests where sockets can still be owned by pika when fixture teardown completes, surfacing as `ResourceWarning: unclosed socket`. The fix for this task must live in the library lifecycle rather than only in test code.

The end state is:
- publisher background threads are tracked per instance
- callers can wait for publisher shutdown completion through a library API
- `RabbitPublisher.stop()` triggers deterministic shutdown on the connection thread
- application and test teardown use the library shutdown contract instead of polling private pika state

## Goals
- Make background publisher teardown deterministic at the library level.
- Fix the RabbitMQ teardown race that intermittently leaves sockets unclosed at test teardown.
- Preserve the existing `publish(...)` auto-start behavior.
- Avoid changing delivery behavior outside startup and shutdown coordination.
- Keep the fix surgical and centered on publisher lifecycle ownership.

## Non-Goals
- No redesign of the `RabbitPublisher` publish path or reconnect behavior beyond what is needed for reliable shutdown.
- No warning suppression, forced garbage collection, or test-only workaround as the primary fix.
- No new requirement for callers to start publisher threads manually.
- No repo-wide concurrency refactor outside publisher lifecycle management.

## Current State
- `pgwal/publishers/base.py` starts publisher threads with `run_publisher_daemon(...)`, but does not keep the thread handle.
- `ensure_running(...)` may auto-start a background publisher thread on first `publish(...)`.
- `RabbitPublisher.stop()` closes channel and connection, but there is no library method to wait until the worker thread and pika ioloop have actually stopped.
- `tests/conftest.py` currently polls `publisher._connection.is_open` during RabbitMQ fixture teardown, which is weaker than waiting for the publisher loop to fully exit.
- `PGWAL.stop_publishers()` calls `publisher.stop()` but does not wait for completion.

## Required Changes
### 1. Track publisher worker threads in the library
Update `pgwal/publishers/base.py` so publisher background threads are owned per publisher instance.

Required behavior:
- keep a per-instance worker thread reference
- prevent concurrent duplicate worker-thread startup for the same publisher instance
- preserve `publish(...)` auto-start semantics through `ensure_running(...)`
- expose a blocking `wait_stopped(timeout: float | None = None) -> bool` method on `BasePublisher`

`wait_stopped(...)` must:
- return `True` when no thread is active or when the thread exits within the timeout
- return `False` if the tracked thread is still alive after the timeout
- clear the stored thread reference after successful exit

### 2. Make RabbitPublisher shutdown deterministic
Update `pgwal/publishers/rabbitmq.py` so shutdown is coordinated through the connection/ioloop lifecycle instead of best-effort direct closes from another thread.

Required behavior:
- maintain a shutdown-complete event for the RabbitMQ worker
- clear the event when `run()` starts and set it only when the worker is fully exiting
- wrap the main loop in a final cleanup path that resets connection/channel references and marks the publisher stopped
- treat `_stopping` as terminal shutdown, not reconnect flow

The shutdown path must:
- request channel/connection closure from the publisher/ioloop thread when a connection exists
- stop the pika ioloop when shutdown reaches terminal connection closure
- allow `run()` to return promptly once shutdown is requested

### 3. Use the library shutdown contract from application code
Update `pgwal/app.py` so publisher shutdown waits for completion.

Required behavior:
- `PGWAL.stop_publishers()` must call `publisher.stop()`
- then wait for each publisher to stop with a bounded timeout
- if a publisher fails to stop within the timeout, log a warning and continue shutdown

This change must apply generically across publishers, even though the current race is observed on RabbitMQ.

### 4. Update tests to validate lifecycle completion
Update RabbitMQ integration teardown and tests to use the library-level completion signal.

Required changes:
- `tests/conftest.py` must stop Rabbit publishers through `publisher.stop()` plus `publisher.wait_stopped(...)`
- remove teardown logic that relies only on `_connection.is_open`
- add or update tests so shutdown verifies thread/lifecycle completion rather than only connection state

Tests must validate:
- a Rabbit publisher started via `publish(...)` can be stopped and waited on successfully
- the tracked worker thread exits after shutdown
- repeated publish calls do not require direct thread management by the caller

### 5. Regenerate and ship updated stubs
After the library changes are in place:
- regenerate stubs with `python scripts/sync_stubs.py`
- keep the checked-in `.pyi` files in sync with the updated publisher lifecycle API

## Public API / Interface Changes
- Add `BasePublisher.wait_stopped(timeout: float | None = None) -> bool`.

No other public API should change.

## Acceptance Criteria
- Background publisher threads are tracked per publisher instance.
- `RabbitPublisher` shutdown completes deterministically through the library lifecycle.
- `PGWAL.stop_publishers()` waits for publishers to stop.
- RabbitMQ test teardown uses `wait_stopped(...)`.
- Checked-in stubs reflect the new lifecycle API.
- Architecture and project-structure docs are updated after implementation.
- `python -m mypy pgwal` passes.

## Validation
Validate with the smallest accurate checks first.

Required validation:
- `python -m pytest tests/test_rabbitmq_publisher.py -vv`
- `python -m pytest tests/test_sync_stubs.py -vv`
- `python -m mypy pgwal`
- `python scripts/sync_stubs.py --check`
- `python scripts/update_project_structure.py`

Run the broader integration path if local infrastructure is available:
- `make run_tests`

## Risks
- `pika.SelectConnection` shutdown behavior is adapter-specific, so cross-thread close handling must stay conservative and use the connection loop correctly.
- If the worker thread reference is not cleared only after true exit, callers may see false-positive shutdown completion.
- A blocking `wait_stopped(...)` API must not accidentally deadlock when called after partial startup or repeated stop requests.

## Default Decisions
- Spec filename: `RABBIT2.md`
- Shutdown API: `wait_stopped(timeout: float | None = None) -> bool`
- Background thread model: tracked per publisher instance
- RabbitMQ shutdown strategy: request shutdown on the connection thread, then wait for worker exit
- Application integration: `PGWAL.stop_publishers()` waits for completion

## Notes For The Implementer
- Keep the lifecycle changes focused and avoid unrelated publisher refactors.
- Prefer explicit shutdown completion signals over timing-based sleeps.
- Do not make the fix test-only; tests should consume the library contract that production code can also use.
