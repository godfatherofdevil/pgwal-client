# Repository Guidelines

## Project Structure & Module Organization
Use the generated tree in `docs/project-structure.md` as the canonical layout reference. The summary below is intentionally short:

```text
.
|-- AGENTS.md
|-- README.md
|-- docs/
|-- scripts/
|-- pgwal/
|-- tests/
|-- docker/
`-- .github/
```

`pgwal/` holds the library code, with publisher backends in `pgwal/publishers/`. `tests/` contains pytest coverage for interfaces and replication consumers. Keep project docs under `docs/`, except `README.md` and `AGENTS.md` at the repository root. Put one-off maintenance utilities in `scripts/`.

## Build, Test, and Development Commands
Use Python 3.10+.

- `pip install -r requirements_dev.txt -r requirements_test.txt` installs runtime, packaging, and test tools.
- `pip install -e .` installs the package in editable mode; use `pip install -e .[all]` if you need RabbitMQ and Kafka publishers.
- `python scripts/update_project_structure.py` regenerates `docs/project-structure.md` after layout changes.
- `make update_docs_structure` runs the same structure-doc refresh through the Makefile.
- `make build_psql_test_local` builds the PostgreSQL test image from `docker/psql.test.Dockerfile`.
- `make run_psql_test` starts the local database used by integration tests on `localhost:5432`.
- `make run_tests` starts the DB container, runs `pytest` through `coverage`, prints a coverage report, and stops the container.
- `pre-commit run --all-files` runs the configured formatting and lint hooks before a commit.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, module-level docstrings, `snake_case` for functions and variables, `PascalCase` for classes, and uppercase constants such as `POOL_MIN`. Format with `black -S`; lint with `flake8` and `pylint`. Keep new modules focused and place publisher implementations in `pgwal/publishers/<backend>.py`.

## Testing Guidelines
Tests use `pytest`; the default suite is integration-heavy and expects the Dockerized PostgreSQL instance. Name files `test_<area>.py` and test functions `test_<behavior>`. Reuse fixtures from `tests/conftest.py` instead of opening ad hoc database connections. Run `make run_tests` for the full path, or `python -m pytest tests/test_interface.py -vv` for a targeted check.

## Commit & Pull Request Guidelines
Recent history uses short, imperative commit subjects such as `add tests for cursor timeout` and `only use psycopg2.OperationalError...`. Keep subjects specific and lowercase unless a proper noun requires otherwise. PRs should explain the behavioral change, note any required Docker or broker setup, link the relevant issue, and include test evidence for changes that affect replication, publishers, or connection handling.

## Configuration Tips
`Makefile` includes `.local/.env`; keep local secrets such as `GITHUB_CR_TOKEN` and `GITHUB_CR_USERNAME` there and out of version control. Do not hardcode credentials or broker endpoints in source or tests.
