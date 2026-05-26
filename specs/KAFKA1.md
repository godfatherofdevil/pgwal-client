# Add real-Kafka integration coverage for `KafkaPublisher`

## Summary
`KafkaPublisher` currently has no automated coverage in this repository. Like the existing RabbitMQ coverage, Kafka should be validated against a real broker and through externally observable delivery behavior rather than mocks.

This change adds Kafka-backed integration tests for `KafkaPublisher`, shared Kafka test fixtures, and Kafka service orchestration in the local `Makefile` path and the CI workflow. The tests verify that payloads published through `KafkaPublisher.publish(...)` are delivered to a Kafka topic with the expected content.

Use a pinned Apache Kafka image for test execution. Default to `apache/kafka-native:3.9.1`.

## Goals
- Add automated integration coverage for `KafkaPublisher`.
- Run the KafkaPublisher tests against a real Kafka broker.
- Keep Kafka-backed tests in the default integration path locally and in CI.
- Reuse the repo's current Docker-based test orchestration style.
- Keep tests focused on observable topic delivery behavior instead of producer internals.

## Non-Goals
- No rewrite of `KafkaPublisher` behavior or threading model unless tests prove one is required.
- No mocked unit-test suite for producer send or flush calls.
- No PostgreSQL-to-Kafka end-to-end replication test as part of this task.
- No new public Python API.

## Current State
- `pgwal/publishers/kafka.py` contains a `KafkaPublisher` built on `kafka.KafkaProducer`.
- `KafkaPublisher.publish(...)` enqueues `msg.payload` into a class-level internal queue and starts the publisher thread if needed.
- `KafkaPublisher.run(...)` drains that queue only while `pgwal.events.EXIT` is set.
- The repository already uses real-service integration testing for PostgreSQL and RabbitMQ.
- The `Makefile` has an ad hoc `run_kafka` target using `apache/kafka:latest`, but Kafka is not part of the default `run_tests` path.
- The CI workflow starts PostgreSQL and RabbitMQ for tests but does not start Kafka.
- There is currently no Kafka-specific test module under `tests/`.

## Required Changes
### 1. Add a KafkaPublisher integration test module
Create `tests/test_kafka_publisher.py`.

The test module must:
- create isolated Kafka topics per test run
- connect to a real broker
- instantiate `KafkaPublisher`
- publish one or more messages through the existing `publish(...)` API
- consume from Kafka using a separate test consumer
- assert that the delivered value matches the original payload

### 2. Add Kafka test fixtures
Extend `tests/conftest.py` with Kafka fixtures that provide the minimum shared setup for broker-backed tests.

The fixture layer must provide:
- broker connection settings from environment variables
- a Kafka admin client for topic creation
- a Kafka consumer path for assertions
- unique topic names per test or per test run
- teardown that stops any started `KafkaPublisher` instance cleanly
- explicit draining of the shared class-level publisher queue before and after each test

The fixtures should avoid coupling Kafka tests to the PostgreSQL replication fixtures. KafkaPublisher tests should use a lightweight fake message object with a `payload` attribute rather than requiring a real `ReplicationMessage`.

### 3. Cover publisher behavior through broker-visible outcomes
The Kafka tests must cover these behaviors:
- a string payload published through `publish(...)` is delivered to the topic unchanged
- a JSON-like payload is serialized and delivered as JSON text
- multiple published payloads are delivered in order for a single publisher instance
- `stop()` shuts the publisher down cleanly after use

Tests should assert topic-observable behavior rather than private producer counters unless that becomes necessary to diagnose a real failure.

### 4. Wire Kafka into local test orchestration
Replace the current standalone Kafka helper target with a test-oriented flow consistent with PostgreSQL and RabbitMQ.

The `Makefile` must define Kafka test configuration and orchestration with:
- `TEST_KAFKA_IMAGE=apache/kafka-native:3.9.1`
- a container name variable
- a broker host variable
- a broker port variable
- a `run_kafka_test` target
- a `wait_kafka_test` target
- cleanup for the Kafka container in `clean`

The readiness check must not use a fixed sleep. It must verify that the Kafka broker is reachable before pytest starts. On timeout, print container logs and fail.

### 5. Include Kafka in the default integration path
Update `make run_tests` so the default full test path starts Kafka in addition to PostgreSQL and RabbitMQ before running pytest.

The full path must:
- start PostgreSQL
- wait for PostgreSQL
- bootstrap PostgreSQL replication privileges
- start RabbitMQ
- wait for RabbitMQ readiness
- start Kafka
- wait for Kafka readiness
- run pytest
- stop all test containers during cleanup even after failures

### 6. Start Kafka in CI
Update `.github/workflows/ci.yml` so the main `test` job provisions Kafka in addition to PostgreSQL and RabbitMQ.

The CI workflow must:
- start a Kafka container from `apache/kafka-native:3.9.1`
- expose the broker port needed by the tests
- wait for broker readiness before starting pytest
- run the full pytest suite after all services are ready
- always dump Kafka logs on failure or job completion, matching the existing PostgreSQL and RabbitMQ diagnostics pattern

Kafka should remain in the existing `test` job rather than a separate job or optional workflow.

## Implementation Details
Keep the Kafka test contract explicit and consistent across fixtures, `Makefile`, and CI.

The shared Kafka contract is:
- image: `apache/kafka-native:3.9.1`
- host: `localhost`
- port: `9092`
- container name: `pgwal-kafka-tests`

The tests should use direct Kafka operations to verify delivery:
- a publisher instance sends to a dedicated topic as part of normal publish flow
- the assertion path reads messages from Kafka with a separate consumer
- tests should poll with a bounded timeout rather than assume immediate delivery

Use unique topic names to prevent collisions across repeated local runs and CI retries.

The fake message object used by tests should be minimal and include only:
- `payload`

Do not create broader fake replication abstractions.

## Acceptance Criteria
- A new Kafka publisher test module exists under `tests/`.
- The Kafka tests run against a real Kafka broker, not mocks.
- The default local integration path starts Kafka automatically.
- The CI `test` job starts Kafka automatically.
- The KafkaPublisher tests validate delivery of published payloads through the broker.
- The Kafka image used for tests is `apache/kafka-native:3.9.1`.
- No new public library API is introduced for this work.

## Validation
Validate with the smallest accurate checks first.

Required validation:
- `python -m pytest tests/test_kafka_publisher.py -vv`
- `make run_tests`
- confirm Kafka readiness checks fail with logs if the broker does not become healthy
- confirm test cleanup does not leave the Kafka container running after failure
- confirm repeated local test runs do not fail due to stale topic names or leftover publisher queue state

## Risks
- `KafkaPublisher` uses a shared class-level queue, so tests may leak state if fixtures do not drain it explicitly.
- Broker startup may be slower or less deterministic than RabbitMQ without an explicit readiness check.
- Kafka topic creation may fail if tests rely on broker defaults instead of creating topics explicitly.
- CI failures may be hard to diagnose without mandatory Kafka log capture.

## Default Decisions
- Spec filename: `KAFKA1.md`
- Spec directory: `specs/`
- Test classification: integration tests for `KafkaPublisher`
- Broker image: `apache/kafka-native:3.9.1`
- Readiness method: broker reachability check using Kafka client metadata
- CI placement: existing main `test` job
- Verification strategy: broker-observable delivery only

## Notes For The Implementer
- Keep the change surgical and aligned with existing test orchestration patterns.
- Prefer a lightweight fake message object over tying KafkaPublisher coverage to PostgreSQL WAL fixtures.
- Do not add mocked producer tests unless live-broker testing exposes a gap that cannot be covered otherwise.
