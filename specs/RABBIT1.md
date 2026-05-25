# Add real-RabbitMQ integration coverage for RabbitPublisher

## Summary
`RabbitPublisher` currently has no automated coverage in this repository. The existing test suite already uses real infrastructure for PostgreSQL WAL replication, and the main test path is intended to validate behavior against live services rather than mocks.

This change adds RabbitMQ-backed tests for `RabbitPublisher` using a real RabbitMQ container locally and in CI. The tests verify externally observable publish behavior only: payloads queued through `RabbitPublisher.publish(...)` are delivered to a bound RabbitMQ queue with the expected content.

RabbitMQ runs from an official Alpine-based image. Use `rabbitmq:4.3-alpine` as the default test image.

## Goals
- Add automated coverage for `RabbitPublisher`.
- Run the RabbitPublisher tests against a real RabbitMQ broker.
- Keep the RabbitMQ-backed tests in the default integration path locally and in CI.
- Reuse the repo's current Docker-based test orchestration style.
- Keep the tests focused on observable delivery behavior instead of mocked callback internals.

## Non-Goals
- No rewrite of `RabbitPublisher` behavior or threading model unless tests prove one is required.
- No mocked unit-test suite for Pika callback methods.
- No RabbitMQ management UI dependency.
- No PostgreSQL-to-RabbitMQ end-to-end replication test as part of this task.
- No new public Python API.

## Current State
- `pgwal/publishers/rabbitmq.py` contains an asynchronous `RabbitPublisher` built on `pika.SelectConnection`.
- `RabbitPublisher.publish(...)` enqueues `msg.payload` into an internal queue and starts the publisher thread if needed.
- The repository already uses real-service integration testing for PostgreSQL.
- The `Makefile` has an ad hoc `run_rabbitmq` target using `rabbitmq:3.13-management-alpine`, but RabbitMQ is not part of the default `run_tests` path.
- The CI workflow starts PostgreSQL for tests but does not start RabbitMQ.
- There is currently no RabbitMQ-specific test module under `tests/`.

## Required Changes
### 1. Add a RabbitPublisher integration test module
Create a new test module at `tests/test_rabbitmq_publisher.py`.

The test module must:
- create isolated RabbitMQ resources per test run
- connect to a real broker
- instantiate `RabbitPublisher`
- publish one or more messages through the existing `publish(...)` API
- consume from RabbitMQ using a separate test connection
- assert that the delivered body matches the original payload

### 2. Add RabbitMQ test fixtures
Extend `tests/conftest.py` with RabbitMQ fixtures that provide the minimum shared setup for broker-backed tests.

The fixture layer must provide:
- broker connection settings from environment variables
- a test AMQP connection/channel for setup and assertions
- unique exchange, queue, and routing key names per test or per test run
- teardown that deletes test-created broker resources when practical
- teardown that stops any started `RabbitPublisher` instance cleanly

The fixtures should avoid coupling RabbitMQ tests to the PostgreSQL replication fixtures. RabbitPublisher tests should use a lightweight fake message object with a `payload` attribute rather than requiring a real `ReplicationMessage`.

### 3. Cover publisher behavior through broker-visible outcomes
The RabbitMQ tests must cover these behaviors:
- publisher bootstraps exchange, queue, and binding successfully against a live broker
- a string payload published through `publish(...)` is delivered to the bound queue
- a JSON-like payload is serialized and delivered as JSON text
- multiple published payloads are delivered in order for a single publisher instance
- `stop()` shuts the publisher down cleanly after use

Tests should assert broker-observable behavior rather than private implementation counters unless that becomes necessary to diagnose a real failure.

### 4. Wire RabbitMQ into local test orchestration
Replace the current standalone RabbitMQ helper target with a test-oriented flow consistent with PostgreSQL.

The Makefile must define RabbitMQ test configuration and orchestration with:
- `TEST_RABBITMQ_IMAGE=rabbitmq:4.3-alpine`
- a container name variable
- an AMQP port variable
- a `run_rabbitmq_test` target
- a `wait_rabbitmq_test` target
- cleanup for the RabbitMQ container in `clean`

The readiness check must not use a fixed sleep. Use `rabbitmq-diagnostics -q ping` executed inside the container with a timeout loop. On timeout, print container logs and fail.

### 5. Include RabbitMQ in the default integration path
Update `make run_tests` so the default full test path starts both PostgreSQL and RabbitMQ before running pytest.

The full path must:
- start PostgreSQL
- wait for PostgreSQL
- bootstrap PostgreSQL replication privileges
- start RabbitMQ
- wait for RabbitMQ readiness
- run pytest
- stop both containers during cleanup even after failures

### 6. Start RabbitMQ in CI
Update `.github/workflows/ci.yml` so the main `test` job provisions RabbitMQ in addition to PostgreSQL.

The CI workflow must:
- start a RabbitMQ container from `rabbitmq:4.3-alpine`
- expose the AMQP port needed by the tests
- wait for readiness with `rabbitmq-diagnostics -q ping`
- run the full pytest suite after both services are ready
- always dump RabbitMQ logs on failure or job completion, matching the existing PostgreSQL diagnostics pattern

RabbitMQ should remain in the existing `test` job rather than a separate job or optional workflow.

## Implementation Details
Keep the RabbitMQ test contract explicit and consistent across fixtures, Makefile targets, and CI.

The shared RabbitMQ contract is:
- image: `rabbitmq:4.3-alpine`
- host: `localhost`
- port: `5672`
- container name: `pgwal-rabbit-tests`

The tests should use direct AMQP operations to verify delivery:
- a publisher instance declares the exchange and queue as part of its normal bootstrap path
- the assertion path reads messages from the queue with a separate blocking AMQP connection
- tests should poll with a bounded timeout rather than assume immediate delivery

Use unique names for exchange, queue, and routing key to prevent collisions across repeated local runs and CI retries.

The fake message object used by tests should be minimal and include only:
- `payload`

Do not create broader fake replication abstractions.

## Acceptance Criteria
- A new RabbitMQ publisher test module exists under `tests/`.
- The RabbitMQ tests run against a real RabbitMQ broker, not mocks.
- The default local integration path starts RabbitMQ automatically.
- The CI `test` job starts RabbitMQ automatically.
- The RabbitPublisher tests validate delivery of published payloads through the broker.
- The RabbitMQ image used for tests is `rabbitmq:4.3-alpine`.
- No new public library API is introduced for this work.

## Validation
Validate with the smallest accurate checks first.

Required validation:
- `python -m pytest tests/test_rabbitmq_publisher.py -vv`
- `make run_tests`
- confirm RabbitMQ readiness checks fail with logs if the broker does not become healthy
- confirm test cleanup does not leave the RabbitMQ container running after failure
- confirm repeated local test runs do not fail due to stale RabbitMQ queue or exchange names

## Risks
- `RabbitPublisher` uses asynchronous Pika behavior, so tests may be flaky if they rely on unbounded timing assumptions.
- Broker resource collisions may occur if test naming is not isolated.
- Shutdown may leave background activity if teardown does not stop the publisher cleanly.
- CI failures may be hard to diagnose without mandatory RabbitMQ log capture.

## Default Decisions
- Spec filename: `RABBIT1.md`
- Spec directory: `specs/`
- Test classification: integration tests for `RabbitPublisher`
- Broker image: `rabbitmq:4.3-alpine`
- Readiness method: `rabbitmq-diagnostics -q ping`
- CI placement: existing main `test` job
- Verification strategy: broker-observable delivery only

## Notes For The Implementer
- Keep the change surgical and aligned with existing test orchestration patterns.
- Prefer a lightweight fake message object over tying RabbitPublisher coverage to PostgreSQL WAL fixtures.
- Do not add mocked callback tests unless live-broker testing exposes a gap that cannot be covered otherwise.
- Do not use the management image unless implementation validation proves it is required.
