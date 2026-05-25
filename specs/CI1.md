# Enable real-Postgres test execution in CI without building a custom test image

## Summary
The CI pipeline currently skips the integration test job entirely. Those tests are intentionally coupled to a real PostgreSQL instance because this library consumes PostgreSQL WAL via logical replication.

This change enables the CI test job by running tests against `ghcr.io/godfatherofdevil/postgres-18-alpine-wal2json:latest` directly at runtime. The existing custom-image path based on `docker/psql.test.Dockerfile` and `docker/init.sql` is removed from the active test flow. Database bootstrap comes from runtime container configuration rather than a built image layer.

## Goals
- Run the existing pytest suite in CI against a real PostgreSQL instance.
- Use the published `wal2json` image directly.
- Eliminate the requirement to build a dedicated test image before running tests.
- Convert the logic previously expressed in `docker/init.sql` into runtime database configuration.
- Keep local test orchestration aligned with the CI path to avoid drift.

## Non-Goals
- No changes to the library's public Python API.
- No refactor of the test suite to use a different connection contract.
- No introduction of mocks or fake replication backends in CI.
- No expansion of the suite beyond enabling the existing PostgreSQL-backed tests.

## Current State
- The CI workflow contains a `test` job gated by `if: false`.
- The current local flow expects a prebuilt `psql-18-test` image.
- The previous SQL bootstrap created:
  - database `tests`
  - user `tests`
  - superuser privileges for `tests`
- The pytest fixtures expect PostgreSQL at:
  - host `localhost`
  - port `5432`
  - db `tests`
  - user `tests`
  - password `secret`
- The tests create a logical replication slot named `repl_test` with output plugin `wal2json`.

## Required Changes
### 1. CI workflow
Update the CI workflow so the `test` job runs on the existing Python version matrix and provisions PostgreSQL at runtime.

The workflow must:
- remove the skip gate from the `test` job
- check out the repository
- set up the requested Python version
- install the project and test dependencies needed to run pytest
- start PostgreSQL from `ghcr.io/godfatherofdevil/postgres-18-alpine-wal2json:latest`
- expose port `5432`
- wait until the database is ready before starting tests
- run pytest
- always emit useful diagnostics and cleanup the container on failure or success

### 2. Runtime database bootstrap
Replace the SQL bootstrap behavior with runtime configuration.

The container runtime configuration must provide:
- `POSTGRES_DB=tests`
- `POSTGRES_USER=tests`
- `POSTGRES_PASSWORD=secret`

If the image does not grant the created user the privileges required for replication slot creation, the runtime startup flow must include a post-start SQL bootstrap step that elevates `tests` to the minimum privileges needed. Prefer a targeted runtime SQL step over preserving a separate init SQL file.

### 3. Container readiness
The CI job must not rely on a fixed sleep.

Use an explicit readiness check:
- preferred: Docker health check using `pg_isready`
- required fallback: loop until `pg_isready` succeeds or a timeout is reached

On timeout, dump container logs and fail the job.

### 4. Logical replication compatibility
The runtime PostgreSQL process must remain compatible with the current tests.

The final runtime setup must support:
- logical replication connections
- creation of replication slots
- the `wal2json` output plugin

Do not add speculative PostgreSQL flags. Only pass explicit `postgres -c ...` runtime options if validation shows the image defaults are insufficient.

### 5. Local test flow alignment
Update local test orchestration to use the same runtime image path as CI.

The Makefile test path should:
- stop requiring a build step as part of normal test execution
- run the published image directly for `run_psql_test`
- use the same database name, username, password, port, and container name assumptions as CI where practical
- preserve cleanup via `clean`

The goal is one operational model for CI and local integration tests.

### 6. Remove dead custom-image infrastructure
After the runtime path is wired in:
- remove active references to `docker/psql.test.Dockerfile`
- remove active references to `docker/init.sql`
- delete those files if they are no longer used anywhere

Also update any developer-facing instructions that still tell contributors to build a PostgreSQL test image first.

## Implementation Details
Keep the implementation DRY by defining the PostgreSQL test contract once per execution environment and reusing it consistently.

The shared contract is:
- image: `ghcr.io/godfatherofdevil/postgres-18-alpine-wal2json:latest`
- host: `localhost`
- port: `5432`
- database: `tests`
- user: `tests`
- password: `secret`
- container name: `pgwal-tests`

Apply that contract consistently in:
- CI container startup
- local `run_psql_test`
- any readiness or cleanup steps
- any documentation updated as part of this work

Avoid duplicating bootstrap logic across multiple places. If the workflow and Makefile both need the same runtime parameters, express them with the smallest practical duplication and identical values.

## Acceptance Criteria
- The CI `test` job is no longer skipped.
- CI runs the existing test suite against a real PostgreSQL instance.
- CI does not build a custom PostgreSQL test image.
- The database and user setup are satisfied at runtime.
- The WAL logical replication tests pass against the runtime container setup.
- The local Makefile-based integration path no longer depends on `psql-18-test`.
- No library API or test fixture contract is changed unless required to keep the existing tests operational.

## Validation
Validate with the smallest accurate checks first.

Required validation:
- confirm workflow syntax remains valid
- confirm the PostgreSQL container starts and becomes ready
- confirm pytest can connect using the existing fixture connection string
- confirm the replication slot creation path still works
- confirm inserts into `demo` produce consumable replication messages
- confirm cleanup stops the PostgreSQL container even after test failure

## Risks
- The published image may not expose the exact runtime defaults needed for logical replication without extra `postgres -c` options.
- The created `tests` user may need explicit privilege elevation for replication slot creation.
- CI failures may be opaque without mandatory container log capture.

## Default Decisions
- Spec filename: `CI1.md`
- Spec directory: `specs/`
- Primary classification: CI infrastructure
- Bootstrap method: runtime container configuration, not image build
- Readiness method: `pg_isready` with timeout and log capture
- Local and CI parity: required unless a concrete platform constraint prevents it

## Notes For The Implementer
- Do not redesign the tests unless runtime verification proves the existing fixture assumptions are invalid.
- Do not keep both the custom-image path and the runtime-image path active.
- Prefer the smallest change set that makes CI authoritative for PostgreSQL-backed integration tests.
