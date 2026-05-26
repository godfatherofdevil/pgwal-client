# Integrate strict mypy and ship generated stubs for `pgwal`

## Summary
This change adds `mypy` as a required development tool, makes the `pgwal/` package pass strict type checking, and introduces a reproducible path for generating `.pyi` stubs with `stubgen`.

The end state is:
- `python -m mypy pgwal` passes cleanly
- `mypy` is wired into the repo's normal development workflow
- generated stubs for `pgwal` are committed and shipped with the package

The first enforcement phase is package-only. `tests/` remain outside the blocking `mypy` target for now, but optional RabbitMQ and Kafka publisher modules are in scope immediately because they are part of the exported package surface.

## Goals
- Add strict `mypy` checking for `pgwal/`.
- Keep the current runtime API and behavior intact while tightening type hints.
- Add the minimum development dependencies required for strict typing.
- Add a repeatable `stubgen` workflow for `pgwal`.
- Commit and ship `.pyi` files and `py.typed` with the package.

## Non-Goals
- No repo-wide strict enforcement for `tests/` or `scripts/` in this first pass.
- No redesign of the library API.
- No speculative runtime refactors just to make types prettier.
- No broad `ignore_missing_imports = true` policy across the whole repo.
- No attempt to model all of `pika` or `kafka-python` with comprehensive local stubs unless implementation proves that is necessary.

## Current State
- The repo currently uses `black`, `flake8`, `pylint`, and `pytest`, but not `mypy`.
- Packaging is configured in `pyproject.toml`.
- The package already contains partial type hints, but they are inconsistent.
- The codebase currently mixes:
  - unannotated public methods
  - legacy `typing.List` / `typing.Union`
  - `None` defaults that conflict with declared types
  - runtime imports used only for typing
  - optional third-party integrations with no guaranteed type support
- The package exports these public symbols from `pgwal.__init__`:
  - `PGWAL`
  - `WALConsumer`
  - `WALReplicationValues`
  - `WALReplicationOpts`
  - `ShellPublisher`
  - `RabbitPublisher`
  - `KafkaPublisher`

## Required Changes
### 1. Add typing dependencies
Update `requirements_dev.txt` to include:
- `mypy`
- `types-psycopg2`

Do not add runtime dependencies for typing. These are development-only additions.

### 2. Add strict mypy configuration
Update `pyproject.toml` with a `[tool.mypy]` section.

The initial configuration must:
- target Python 3.10
- enable strict mode
- enable the Pydantic mypy plugin
- scope enforcement to `pgwal/`
- exclude `tests/` from the first blocking pass

If untyped third-party modules require special handling, use the narrowest module-level override possible. Do not set a global import ignore.

### 3. Add developer workflow hooks
Update the repo tooling so `mypy` is part of normal verification.

Required workflow additions:
- a `Makefile` target for `python -m mypy pgwal`
- a `Makefile` target for `stubgen -p pgwal -o <output-dir>`
- a pre-commit hook for `mypy`

Keep the existing test workflow intact. `make run_tests` remains the integration path for behavior verification.

### 4. Make `pgwal/` pass strict mypy
Update code under `pgwal/` only, keeping the changes surgical.

The implementation must:
- annotate all public constructors, methods, and properties
- replace implicit `Any` where practical
- replace `typing.List` / `typing.Union` with built-in generics
- use `TYPE_CHECKING` imports for symbols needed only by annotations
- fix default argument annotations where `None` is a valid value
- type internal attributes that are initialized lazily
- preserve runtime behavior

### 5. Package-specific typing requirements
#### `pgwal/__init__.py`
- Add an explicit return type for `int_or_str`.
- Preserve the public exports exactly as they are today.

#### `pgwal/app.py`
- Type the `dsn` input with a concrete mapping shape suitable for the current implementation.
- Type `_pool`, `tasks`, and `publishers`.
- Add `-> None` return annotations where missing.

#### `pgwal/consumers.py`
- Replace the current `Callable` subclassing approach if it creates unnecessary typing friction.
- Type the `publishers` constructor parameter as optional.
- Type all lifecycle methods explicitly.

#### `pgwal/interface.py`
- Keep the current model API and serialization behavior unchanged.
- Type `_WALReplicationActions`.
- Type validators in a way compatible with strict `mypy` and the Pydantic plugin.

#### `pgwal/publishers/base.py`
- Type the queue-handling and decorator utilities explicitly.
- Keep the current publish/run/stop contract unchanged.
- Prefer a small local payload alias over repeated broad unions.

#### `pgwal/publishers/shell.py`
- Add missing return annotations only.

#### `pgwal/publishers/kafka.py`
- Type the producer lifecycle, config storage, and publish path.
- If `KafkaProducer` typing remains weak, prefer a narrow local workaround over a repo-wide ignore.

#### `pgwal/publishers/rabbitmq.py`
- Type the connection lifecycle, callback methods, internal counters, and publish path.
- Preserve the current reconnect and scheduling behavior.
- If `pika` typing is incomplete, use the smallest possible workaround.

### 6. Third-party typing strategy
Use this default strategy:
- `psycopg2`: satisfy via `types-psycopg2`
- `pydantic`: satisfy via `pydantic.mypy`
- `pika`: use direct imports first; if that is insufficient, use a narrow override or minimal local typing shim
- `kafka-python`: same as `pika`

If local shims or stubs are needed, they must be limited to the symbols actually used by `pgwal/`. Do not create a broad replacement type surface for those libraries.

### 7. Generate and ship stubs
After `pgwal/` passes strict `mypy`, generate stubs with `stubgen`.

Required workflow:
- run `stubgen -p pgwal -o <temporary-output-dir>`
- review the generated stubs
- copy accepted `.pyi` files into the package
- add `pgwal/py.typed`
- ensure build configuration includes `.pyi` files and `py.typed` in the built package

Do not generate or ship stubs for `tests/`.

## Implementation Details
Keep the first rollout intentionally narrow:
- blocking `mypy` target: `pgwal/`
- non-blocking for now: `tests/`, `scripts/`, repo-wide type coverage

Use these defaults unless implementation proves they are insufficient:
- mypy scope: `pgwal`
- Python version: `3.10`
- strictness: `strict = true`
- plugin: `pydantic.mypy`
- stub generation command: `stubgen -p pgwal -o .local/stubs`

For packaging, the final wheel and sdist must include:
- `.pyi` files under `pgwal/`
- `pgwal/py.typed`

Do not move or rename existing package modules as part of this work.

## Acceptance Criteria
- `requirements_dev.txt` includes `mypy` and `types-psycopg2`.
- `pyproject.toml` contains strict `mypy` configuration for `pgwal/`.
- `Makefile` exposes a type-check target and a stub-generation target.
- `.pre-commit-config.yaml` includes a `mypy` hook.
- `python -m mypy pgwal` passes with no errors.
- Existing runtime behavior remains intact.
- `.pyi` files for `pgwal` are committed.
- `pgwal/py.typed` exists.
- The build configuration includes the typing artifacts in distributable packages.

## Validation
Validate with the smallest accurate checks first.

Required validation:
- `python -m mypy pgwal`
- `python -m pytest tests/test_interface.py -vv`
- `make run_tests`
- `stubgen -p pgwal -o <temp-dir>`
- rerun `python -m mypy pgwal` after committing `.pyi` files
- `pre-commit run --all-files`

## Risks
- `pika` and `kafka-python` may not provide type information usable under strict `mypy`.
- Private or runtime-only imports used for annotations may trigger avoidable type issues.
- Stub generation may reflect weak third-party typing unless package annotations are tightened first.
- If `.pyi` files are shipped without build inclusion updates, downstream consumers will not receive the typing artifacts.

## Default Decisions
- Spec filename: `MYPY1.md`
- Spec directory: `specs/`
- First enforcement target: `pgwal/`
- Test typing enforcement: deferred
- Optional publisher modules: included in the first pass
- Stub distribution: committed and shipped
- Third-party import policy: narrow overrides or minimal shims only

## Notes For The Implementer
- Keep the edits surgical and avoid unrelated cleanup.
- Preserve runtime behavior even when the current implementation shape is not ideal for typing.
- Prefer local, explicit fixes over global suppression.
- Do not continue expanding scope into `tests/` unless a package change absolutely requires a matching test edit.
