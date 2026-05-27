# Documentation

Repository documentation lives here unless a file is intentionally kept at the root.

## Contents
- `architecture.md`: end-to-end runtime architecture and flow diagrams for Postgres WAL consumption and publisher fan-out.
- `project-structure.md`: generated repository and module layout reference.

## Maintenance
- Keep `README.md` and `AGENTS.md` at the repository root.
- Regenerate structure docs with `python scripts/update_project_structure.py` after layout changes.

## Standalone E2E Flow
- `scripts/run_e2e.py` is the standalone end-to-end runner. It is separate from the `tests/` suite.
- By default the runner reads local overrides from `.local/.env`. Override that path with `--env-file` or `make run_e2e E2E_ENV_FILE=...`.
- The runner does not start or stop Docker containers itself.
- `make run_e2e` links PostgreSQL startup/bootstrap through Make dependencies.
- Start destination infrastructure separately with the existing broker targets before running e2e:
  - `make run_rabbitmq_test wait_rabbitmq_test`
  - `make run_kafka_test wait_kafka_test`
- Run the e2e flow with explicit publisher selection:
  - `make run_e2e E2E_PUBLISHERS=rabbitmq`
  - `make run_e2e E2E_PUBLISHERS=kafka`
  - `make run_e2e E2E_PUBLISHERS=rabbitmq,kafka E2E_CONSUMER_WORKERS=2`
