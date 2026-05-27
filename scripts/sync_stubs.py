"""Regenerate and sync checked-in type stubs for pgwal."""
from __future__ import annotations

import argparse
import ast
import filecmp
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from mypy import stubgen


ROOT = Path(__file__).resolve().parent.parent
PACKAGE_DIR = ROOT / 'pgwal'
INCOMPLETE_IMPORT = 'from _typeshed import Incomplete as Incomplete'
SUPPORTED_BUILTINS = {
    'None',
    'bool',
    'bytearray',
    'bytes',
    'dict',
    'float',
    'frozenset',
    'int',
    'list',
    'memoryview',
    'object',
    'set',
    'str',
    'tuple',
}


@dataclass
class StubAttribute:
    """A class attribute declaration that should exist in a generated stub."""

    name: str
    annotation: str


@dataclass
class StubMethod:
    """A method declaration that should exist in a generated stub."""

    name: str
    text: str


@dataclass
class SourceModule:
    """Parsed source module data used during stub enrichment."""

    path: Path
    tree: ast.Module
    classes: dict[str, ast.ClassDef]
    imports: dict[str, Path]


def _merge_attribute(
    attributes: dict[str, StubAttribute],
    order: list[str],
    name: str,
    annotation: str,
) -> None:
    """Prefer concrete annotations over Incomplete while keeping source order."""
    existing = attributes.get(name)
    if existing is None:
        attributes[name] = StubAttribute(name=name, annotation=annotation)
        order.append(name)
        return

    if existing.annotation == 'Incomplete' and annotation != 'Incomplete':
        attributes[name] = StubAttribute(name=name, annotation=annotation)


def _infer_constant_annotation(node: ast.AST) -> str:
    """Infer a narrow annotation for simple literals."""
    if isinstance(node, ast.Constant):
        value = node.value
        if isinstance(value, bool):
            return 'bool'
        if isinstance(value, int):
            return 'int'
        if isinstance(value, float):
            return 'float'
        if isinstance(value, str):
            return 'str'
        if value is None:
            return 'None'
    return 'Incomplete'


def _collect_parameter_annotations(function: ast.FunctionDef) -> dict[str, str]:
    """Collect explicit parameter annotations for a function."""
    annotations: dict[str, str] = {}
    args = [
        *function.args.posonlyargs,
        *function.args.args,
        *function.args.kwonlyargs,
    ]
    if function.args.vararg is not None and function.args.vararg.annotation is not None:
        annotations[function.args.vararg.arg] = ast.unparse(
            function.args.vararg.annotation
        )
    if function.args.kwarg is not None and function.args.kwarg.annotation is not None:
        annotations[function.args.kwarg.arg] = ast.unparse(
            function.args.kwarg.annotation
        )
    for arg in args:
        if arg.annotation is not None:
            annotations[arg.arg] = ast.unparse(arg.annotation)
    return annotations


def _collect_class_attributes(  # pylint: disable=too-many-branches
    class_node: ast.ClassDef,
) -> list[StubAttribute]:
    """Collect class and instance attributes declared in source."""
    attributes: dict[str, StubAttribute] = {}
    order: list[str] = []
    for statement in class_node.body:
        if isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            _merge_attribute(
                attributes,
                order,
                statement.target.id,
                ast.unparse(statement.annotation),
            )
            continue
        if isinstance(statement, ast.Assign):
            annotation = _infer_constant_annotation(statement.value)
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    _merge_attribute(attributes, order, target.id, annotation)
            continue
        if not isinstance(statement, ast.FunctionDef):
            continue

        parameter_annotations = _collect_parameter_annotations(statement)
        for node in ast.walk(statement):
            if isinstance(node, ast.AnnAssign):
                target = node.target
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == 'self'
                ):
                    _merge_attribute(
                        attributes,
                        order,
                        target.attr,
                        ast.unparse(node.annotation),
                    )
                continue

            if not isinstance(node, ast.Assign):
                continue
            annotation = 'Incomplete'
            value = node.value
            if isinstance(value, ast.Name):
                annotation = parameter_annotations.get(value.id, 'Incomplete')
            elif isinstance(value, ast.Constant):
                annotation = _infer_constant_annotation(value)
            for target in node.targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == 'self'
                ):
                    _merge_attribute(attributes, order, target.attr, annotation)

    return [attributes[name] for name in order]


def _normalize_function_annotations(
    function: ast.FunctionDef,
    defined_names: set[str],
) -> ast.FunctionDef:
    """Return a copy of the function with unsupported annotations replaced."""
    normalized = ast.parse(ast.unparse(function)).body[0]
    assert isinstance(normalized, ast.FunctionDef)
    all_args = [
        *normalized.args.posonlyargs,
        *normalized.args.args,
        *normalized.args.kwonlyargs,
    ]
    if (
        normalized.args.vararg is not None
        and normalized.args.vararg.annotation is not None
    ):
        annotation = ast.unparse(normalized.args.vararg.annotation)
        normalized_name = _normalize_annotation(annotation, defined_names)
        normalized.args.vararg.annotation = ast.parse(normalized_name, mode='eval').body
    if (
        normalized.args.kwarg is not None
        and normalized.args.kwarg.annotation is not None
    ):
        annotation = ast.unparse(normalized.args.kwarg.annotation)
        normalized_name = _normalize_annotation(annotation, defined_names)
        normalized.args.kwarg.annotation = ast.parse(normalized_name, mode='eval').body
    for arg in all_args:
        if arg.annotation is None:
            continue
        annotation = ast.unparse(arg.annotation)
        normalized_name = _normalize_annotation(annotation, defined_names)
        arg.annotation = ast.parse(normalized_name, mode='eval').body
    if normalized.returns is not None:
        returns = _normalize_annotation(ast.unparse(normalized.returns), defined_names)
        normalized.returns = ast.parse(returns, mode='eval').body
    return normalized


def _function_stub_line(
    function: ast.FunctionDef,
    indent: str,
    defined_names: set[str],
) -> str:
    """Render a function signature line suitable for a stub."""
    normalized = _normalize_function_annotations(function, defined_names)
    args = ast.unparse(normalized.args)
    returns = ' -> Incomplete'
    if normalized.returns is not None:
        returns = f' -> {ast.unparse(normalized.returns)}'
    return f'{indent}def {normalized.name}({args}){returns}: ...'


def _collect_local_methods(
    class_node: ast.ClassDef,
    defined_names: set[str],
) -> list[StubMethod]:
    """Collect concrete methods declared directly on a class."""
    methods: list[StubMethod] = []
    indent = ' ' * 4
    for statement in class_node.body:
        if not isinstance(statement, ast.FunctionDef):
            continue
        decorators = {ast.unparse(decorator) for decorator in statement.decorator_list}
        if 'abc.abstractmethod' in decorators:
            continue
        methods.append(
            StubMethod(
                name=statement.name,
                text=_function_stub_line(statement, indent, defined_names),
            )
        )
    return methods


def _find_import_insertion_index(lines: list[str]) -> int:
    """Insert imports after the existing import block."""
    last_import_index = -1
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('import ') or stripped.startswith('from '):
            last_import_index = index
            continue
        if stripped == '' and last_import_index >= 0:
            continue
        if last_import_index >= 0:
            break
    return last_import_index + 1 if last_import_index >= 0 else 0


def _ensure_incomplete_import(stub_text: str) -> str:
    """Add the Incomplete import when the enrichment uses it."""
    if 'Incomplete' not in stub_text or 'from _typeshed import Incomplete' in stub_text:
        return stub_text

    lines = stub_text.splitlines()
    insert_at = _find_import_insertion_index(lines)
    lines.insert(insert_at, INCOMPLETE_IMPORT)
    return '\n'.join(lines) + '\n'


def _collect_existing_stub_members(class_node: ast.ClassDef) -> set[str]:
    """Collect already declared member names from a generated stub class."""
    members: set[str] = set()
    for statement in class_node.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            members.add(statement.name)
        elif isinstance(statement, ast.AnnAssign):
            target = statement.target
            if isinstance(target, ast.Name):
                members.add(target.id)
        elif isinstance(statement, ast.Assign):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    members.add(target.id)
    return members


def _collect_stub_defined_names(stub_tree: ast.Module) -> set[str]:
    """Collect names that can be referenced safely inside stub annotations."""
    names = set(SUPPORTED_BUILTINS)
    for statement in stub_tree.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                names.add(alias.asname or alias.name.split('.')[0])
        elif isinstance(statement, ast.ImportFrom):
            for alias in statement.names:
                names.add(alias.asname or alias.name)
        elif isinstance(
            statement,
            (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            names.add(statement.name)
        elif isinstance(statement, ast.Assign):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            names.add(statement.target.id)
    return names


def _collect_source_classes(source_tree: ast.Module) -> dict[str, ast.ClassDef]:
    """Return source classes keyed by name."""
    return {
        class_node.name: class_node
        for class_node in source_tree.body
        if isinstance(class_node, ast.ClassDef)
    }


def _resolve_local_import(
    source_path: Path,
    module_name: str | None,
    level: int,
) -> Path | None:
    """Resolve a relative import path within the local package."""
    if level <= 0:
        return None
    package_dir = source_path.parent
    for _ in range(level - 1):
        package_dir = package_dir.parent
    parts = [] if module_name is None else module_name.split('.')
    candidate = package_dir.joinpath(*parts)
    module_file = candidate.with_suffix('.py')
    if module_file.exists():
        return module_file
    init_file = candidate / '__init__.py'
    if init_file.exists():
        return init_file
    return None


def _collect_local_imports(
    source_tree: ast.Module, source_path: Path
) -> dict[str, Path]:
    """Collect locally resolvable imported class names."""
    imports: dict[str, Path] = {}
    for statement in source_tree.body:
        if not isinstance(statement, ast.ImportFrom):
            continue
        module_path = _resolve_local_import(
            source_path, statement.module, statement.level
        )
        if module_path is None:
            continue
        for alias in statement.names:
            imports[alias.asname or alias.name] = module_path
    return imports


def _load_source_module(
    source_path: Path,
    cache: dict[Path, SourceModule],
) -> SourceModule:
    """Parse and cache a local source module."""
    cached = cache.get(source_path)
    if cached is not None:
        return cached
    tree = ast.parse(source_path.read_text(encoding='utf-8'))
    module = SourceModule(
        path=source_path,
        tree=tree,
        classes=_collect_source_classes(tree),
        imports=_collect_local_imports(tree, source_path),
    )
    cache[source_path] = module
    return module


def _resolve_class_node(
    class_name: str,
    module: SourceModule,
    cache: dict[Path, SourceModule],
    visiting: set[tuple[Path, str]],
) -> ast.ClassDef | None:
    """Resolve a class from the current module or a locally imported module."""
    resolved = module.classes.get(class_name)
    if resolved is not None:
        return resolved
    imported_path = module.imports.get(class_name)
    if imported_path is None:
        return None
    visit_key = (imported_path, class_name)
    if visit_key in visiting:
        return None
    visiting.add(visit_key)
    imported_module = _load_source_module(imported_path, cache)
    return _resolve_class_node(class_name, imported_module, cache, visiting)


def _collect_inherited_methods(
    class_node: ast.ClassDef,
    module: SourceModule,
    cache: dict[Path, SourceModule],
    visiting: set[tuple[Path, str]],
    defined_names: set[str],
) -> list[StubMethod]:
    """Collect concrete methods from local bases for IDE-friendly stubs."""
    methods: dict[str, StubMethod] = {}
    for base in class_node.bases:
        if not isinstance(base, ast.Name):
            continue
        base_class = _resolve_class_node(base.id, module, cache, visiting.copy())
        if base_class is None:
            continue
        for method in _collect_local_methods(base_class, defined_names):
            methods.setdefault(method.name, method)
        base_module = module
        imported_path = module.imports.get(base.id)
        if imported_path is not None:
            base_module = _load_source_module(imported_path, cache)
        for method in _collect_inherited_methods(
            base_class,
            base_module,
            cache,
            visiting | {(module.path, class_node.name)},
            defined_names,
        ):
            methods.setdefault(method.name, method)
    return list(methods.values())


def _normalize_annotation(annotation: str, defined_names: set[str]) -> str:
    """Fall back to Incomplete when the annotation references missing names."""
    try:
        expression = ast.parse(annotation, mode='eval')
    except SyntaxError:
        return 'Incomplete'

    for node in ast.walk(expression):
        if isinstance(node, ast.Name) and node.id not in defined_names:
            return 'Incomplete'
    return annotation


def _statement_start_line(statement: ast.stmt) -> int:
    """Return the first line occupied by a statement, including decorators."""
    if (
        isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef))
        and statement.decorator_list
    ):
        return min(decorator.lineno for decorator in statement.decorator_list)
    return statement.lineno


def _collect_empty_enum_bases(
    source_tree: ast.Module,
) -> dict[str, list[str]]:
    """Find enum helper classes with no members so stubs can normalize them."""
    empty_enum_bases: dict[str, list[str]] = {}
    for class_node in source_tree.body:
        if not isinstance(class_node, ast.ClassDef):
            continue
        base_names = [ast.unparse(base) for base in class_node.bases]
        if 'Enum' not in base_names:
            continue
        has_members = any(
            isinstance(statement, (ast.Assign, ast.AnnAssign))
            and (
                any(isinstance(target, ast.Name) for target in statement.targets)
                if isinstance(statement, ast.Assign)
                else isinstance(statement.target, ast.Name)
            )
            for statement in class_node.body
        )
        if not has_members:
            empty_enum_bases[class_node.name] = base_names
    return empty_enum_bases


def _is_enum_like_class(
    class_node: ast.ClassDef,
    source_classes: dict[str, ast.ClassDef],
) -> bool:
    """Return True when a class is an enum or inherits from a local enum helper."""
    for base in class_node.bases:
        if not isinstance(base, ast.Name):
            continue
        if base.id == 'Enum':
            return True
        base_class = source_classes.get(base.id)
        if base_class is not None and _is_enum_like_class(base_class, source_classes):
            return True
    return False


def _normalize_empty_enum_stubs(  # pylint: disable=too-many-locals
    source_tree: ast.Module,
    stub_tree: ast.Module,
    stub_lines: list[str],
) -> None:
    """Rewrite empty enum helpers so generated stubs remain valid for mypy."""
    empty_enum_bases = _collect_empty_enum_bases(source_tree)
    if not empty_enum_bases:
        return

    replacements: list[tuple[int, str]] = []
    deletions: list[int] = []
    for class_node in stub_tree.body:
        if not isinstance(class_node, ast.ClassDef):
            continue
        bases = [ast.unparse(base) for base in class_node.bases]
        if class_node.name in empty_enum_bases:
            normalized_bases = [base for base in bases if base != 'Enum'] or ['object']
            if stub_lines[class_node.lineno - 1].strip().endswith('...'):
                deletions.append(class_node.lineno - 1)
        else:
            normalized_bases = []
            changed = False
            for base in bases:
                if base in empty_enum_bases:
                    normalized_bases.extend(empty_enum_bases[base])
                    changed = True
                else:
                    normalized_bases.append(base)
            if not changed:
                continue

        indent = ' ' * class_node.col_offset
        bases_text = ', '.join(normalized_bases)
        suffix = (
            ' ...' if stub_lines[class_node.lineno - 1].strip().endswith('...') else ''
        )
        replacements.append(
            (
                class_node.lineno - 1,
                f'{indent}class {class_node.name}({bases_text}):{suffix}',
            )
        )

    for index, line in reversed(replacements):
        stub_lines[index] = line

    for index in sorted(deletions, reverse=True):
        del stub_lines[index]


def _enrich_stub_text(  # pylint: disable=too-many-locals
    source_text: str,
    stub_text: str,
    source_path: Path | None = None,
) -> str:
    """Insert missing class and instance attributes into a generated stub."""
    source_tree = ast.parse(source_text)
    stub_tree = ast.parse(stub_text)
    defined_names = _collect_stub_defined_names(stub_tree)
    source_class_nodes = _collect_source_classes(source_tree)
    module_path = source_path or PACKAGE_DIR / '__synthetic__.py'
    module = SourceModule(
        path=module_path,
        tree=source_tree,
        classes=source_class_nodes,
        imports=_collect_local_imports(source_tree, module_path) if source_path else {},
    )
    source_module_cache = {module.path: module}
    source_classes = {
        class_name: _collect_class_attributes(class_node)
        for class_name, class_node in source_class_nodes.items()
    }
    stub_lines = stub_text.splitlines()
    _normalize_empty_enum_stubs(source_tree, stub_tree, stub_lines)
    insertions: list[tuple[int, list[str]]] = []

    for class_node in stub_tree.body:
        if not isinstance(class_node, ast.ClassDef):
            continue
        source_class_node = source_class_nodes.get(class_node.name)
        if source_class_node is None:
            continue
        source_attributes = source_classes.get(class_node.name, [])

        existing_members = _collect_existing_stub_members(class_node)
        local_methods: list[StubMethod] = []
        inherited_methods: list[StubMethod] = []
        if not _is_enum_like_class(source_class_node, source_class_nodes):
            local_methods = [
                method
                for method in _collect_local_methods(source_class_node, defined_names)
                if method.name not in existing_members
            ]
        missing_attributes = [
            StubAttribute(
                name=attribute.name,
                annotation=_normalize_annotation(attribute.annotation, defined_names),
            )
            for attribute in source_attributes
            if attribute.name not in existing_members
        ]
        if not _is_enum_like_class(source_class_node, source_class_nodes):
            inherited_methods = [
                method
                for method in _collect_inherited_methods(
                    source_class_node,
                    module,
                    source_module_cache,
                    {(module.path, source_class_node.name)},
                    defined_names,
                )
                if method.name not in existing_members
                and method.name
                not in {local_method.name for local_method in local_methods}
            ]
        if not missing_attributes and not local_methods and not inherited_methods:
            continue

        indent = ' ' * (class_node.col_offset + 4)
        new_lines = [
            f'{indent}{attribute.name}: {attribute.annotation}'
            for attribute in missing_attributes
        ]
        new_lines.extend(f'{indent}{method.text.strip()}' for method in local_methods)
        new_lines.extend(
            f'{indent}{method.text.strip()}' for method in inherited_methods
        )
        insertions.append(
            (
                _statement_start_line(class_node.body[0]) - 1,
                new_lines,
            )
        )

    for index, new_lines in sorted(insertions, reverse=True):
        stub_lines[index:index] = new_lines

    enriched = '\n'.join(stub_lines)
    if stub_text.endswith('\n'):
        enriched += '\n'
    return _ensure_incomplete_import(enriched)


def _copy_package_sources(source_root: Path) -> Path:
    """Copy the package source without checked-in stubs for generation."""
    copied_package_dir = source_root / 'pgwal'
    shutil.copytree(PACKAGE_DIR, copied_package_dir)
    for stub_path in copied_package_dir.rglob('*.pyi'):
        stub_path.unlink()
    return copied_package_dir


def _run_stubgen(source_package_dir: Path, output_dir: Path) -> None:

    stubgen.main(
        [
            '--no-import',
            '-o',
            str(output_dir),
            str(source_package_dir),
        ]
    )


def _normalize_package_stubs(output_dir: Path) -> Path:
    generated_package_dir = output_dir / 'pgwal'
    package_stub = output_dir / 'pgwal.pyi'
    if package_stub.exists():
        shutil.move(package_stub, generated_package_dir / '__init__.pyi')

    for package_init in PACKAGE_DIR.rglob('__init__.py'):
        if package_init.parent == PACKAGE_DIR:
            continue
        relative_package = package_init.parent.relative_to(PACKAGE_DIR)
        package_stub_file = generated_package_dir / f'{relative_package}.pyi'
        package_stub_dir = generated_package_dir / relative_package
        if package_stub_file.exists():
            package_stub_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(package_stub_file, package_stub_dir / '__init__.pyi')

    return generated_package_dir


def _source_path_for_stub(stub_path: Path, generated_package_dir: Path) -> Path:
    """Map a generated stub path back to its source file."""
    relative_path = stub_path.relative_to(generated_package_dir)
    return PACKAGE_DIR / relative_path.with_suffix('.py')


def _enrich_generated_stubs(generated_package_dir: Path) -> None:
    """Update generated stubs with source-defined attributes."""
    for stub_path in generated_package_dir.rglob('*.pyi'):
        source_path = _source_path_for_stub(stub_path, generated_package_dir)
        if not source_path.exists():
            continue
        stub_text = stub_path.read_text(encoding='utf-8')
        source_text = source_path.read_text(encoding='utf-8')
        enriched = _enrich_stub_text(source_text, stub_text, source_path=source_path)
        if enriched != stub_text:
            stub_path.write_text(enriched, encoding='utf-8')


def _clear_existing_stubs() -> None:
    for stub_path in PACKAGE_DIR.rglob('*.pyi'):
        stub_path.unlink()


def _copy_generated_stubs(generated_package_dir: Path) -> None:
    for source_path in generated_package_dir.rglob('*.pyi'):
        relative_path = source_path.relative_to(generated_package_dir)
        destination_path = PACKAGE_DIR / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)


def _stubs_are_synced(generated_package_dir: Path) -> bool:
    generated_stubs = {
        path.relative_to(generated_package_dir)
        for path in generated_package_dir.rglob('*.pyi')
    }
    existing_stubs = {
        path.relative_to(PACKAGE_DIR) for path in PACKAGE_DIR.rglob('*.pyi')
    }
    if generated_stubs != existing_stubs:
        return False

    for relative_path in generated_stubs:
        generated_path = generated_package_dir / relative_path
        existing_path = PACKAGE_DIR / relative_path
        if not filecmp.cmp(generated_path, existing_path, shallow=False):
            return False
    return True


def sync_stubs(check: bool) -> int:
    """Synchronize checked-in stubs or report drift."""
    with tempfile.TemporaryDirectory(prefix='pgwal-stubs-') as tmp_dir:
        output_dir = Path(tmp_dir)
        source_package_dir = _copy_package_sources(output_dir / 'source')
        _run_stubgen(source_package_dir, output_dir)
        generated_package_dir = _normalize_package_stubs(output_dir)
        _enrich_generated_stubs(generated_package_dir)

        if check:
            return 0 if _stubs_are_synced(generated_package_dir) else 1

        _clear_existing_stubs()
        _copy_generated_stubs(generated_package_dir)
    return 0


def main() -> int:
    """Parse CLI arguments and run stub synchronization."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='fail if the checked-in stubs are out of sync',
    )
    args = parser.parse_args()
    return sync_stubs(check=args.check)


if __name__ == '__main__':
    raise SystemExit(main())
