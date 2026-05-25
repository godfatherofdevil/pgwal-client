# Repository Guidelines

## Project Structure & Module Organization
Use this checklist:

```text
.
|-- AGENTS.md
|-- README.md
|-- docs/
|-- specs/
|-- scripts/
|-- pgwal/
|-- tests/
`-- .github/
```

- Use `docs/project-structure.md` as the canonical layout reference.
- Keep library code in `pgwal/` and publishers in `pgwal/publishers/`.
- Keep tests in `tests/` and shared fixtures in `tests/conftest.py`.
- Keep docs in `docs/`, except `README.md` and `AGENTS.md` at the root.
- Put one-off maintenance utilities in `scripts/`.

## Think Before Coding
- State assumptions explicitly before making non-trivial changes.
- Ask when behavior, scope, or interfaces are unclear.
- Surface meaningful tradeoffs instead of choosing silently.
- Prefer the simpler approach when it solves the request.
- Stop and clarify if the request conflicts with the current code or repo rules.

## Simplicity First
- Implement only what the request requires.
- Avoid new abstractions for single-use code.
- Avoid speculative configurability or future-proofing.
- Keep error handling realistic and tied to actual failure modes in this repo.
- Rewrite overcomplicated changes before submitting them.

## Surgical Changes
- Change only the files and lines needed for the request.
- Match the surrounding style in `pgwal/`, `tests/`, and repo config files.
- Do not refactor unrelated code while working on a focused task.
- Remove imports, variables, or helpers made unused by your change.
- Mention unrelated dead code or issues instead of deleting them unasked.

## Goal-Driven Execution
- Define a concrete success check before implementing multi-step work.
- Break larger tasks into short steps with a verification point for each.
- Prefer test-backed changes for bug fixes and behavioral changes.
- Verify with the smallest accurate command first, then run broader checks if needed.
- Treat `make run_tests` as the full integration path when PostgreSQL-backed behavior changes.

## Build, Test, and Development Commands
- Use Python 3.10+.
- Run `pip install -r requirements_dev.txt -r requirements_test.txt` for dev and test dependencies.
- Run `pip install -e .` for editable installs; use `pip install -e .[all]` for all publishers.
- Run `python scripts/update_project_structure.py` or `make update_docs_structure` after layout changes.
- Run `make run_psql_test` to start the local test database.
- Run `make run_tests` for the full test path with coverage.
- Run `pre-commit run --all-files` before opening a pull request.

## Coding Style & Naming Conventions
- Use 4-space indentation.
- Use module docstrings where the file pattern already exists.
- Use `snake_case` for functions and variables.
- Use `PascalCase` for classes.
- Use uppercase names for constants such as `POOL_MIN`.
- Run `black -S`, `flake8`, and `pylint` through pre-commit.
- Keep new modules focused.
- Place publisher implementations in `pgwal/publishers/<backend>.py`.

## Testing Guidelines
- Use `pytest`.
- Name files `test_<area>.py` and functions `test_<behavior>`.
- Reuse fixtures from `tests/conftest.py`.
- Expect the default suite to depend on the Dockerized PostgreSQL instance.
- Run `make run_tests` for the full integration path.
- Run `python -m pytest tests/test_interface.py -vv` for focused checks.

## Commit & Pull Request Guidelines
- Write short, imperative commit subjects.
- Keep subjects specific and lowercase unless a proper noun requires otherwise.
- Follow the existing style used by `add tests for cursor timeout`.
- Explain the behavioral change in each pull request.
- Note required Docker or broker setup.
- Link the relevant issue when one exists.
- Include test evidence for replication, publisher, or connection-handling changes.

## Configuration Tips
- Keep local overrides in `.local/.env`.
- Keep `.local/.env` out of version control.
- Do not hardcode credentials or broker endpoints in source or tests.

## Agent Examples
- Read `EXAMPLES.md` before making non-trivial changes. It is the canonical example set for how agents are expected to apply the repo principles in practice.
- Treat `EXAMPLES.md` as normative guidance for handling ambiguity, avoiding overengineering, making surgical edits, and verifying work.
