"""Regenerate and sync checked-in type stubs for pgwal."""
from __future__ import annotations

import argparse
import filecmp
import shutil
import tempfile
from pathlib import Path

from mypy import stubgen


ROOT = Path(__file__).resolve().parent.parent
PACKAGE_DIR = ROOT / 'pgwal'


def _run_stubgen(output_dir: Path) -> None:
    stubgen.main(
        [
            '--no-import',
            '-o',
            str(output_dir),
            str(PACKAGE_DIR),
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
        _run_stubgen(output_dir)
        generated_package_dir = _normalize_package_stubs(output_dir)

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
