"""Tests for stub synchronization helpers."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


def _load_sync_stubs_module():
    module_path = Path(__file__).resolve().parent.parent / 'scripts' / 'sync_stubs.py'
    spec = importlib.util.spec_from_file_location('sync_stubs', module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync_stubs = _load_sync_stubs_module()


@pytest.fixture(scope='session', autouse=True)
def db_conn():
    """Override the integration database fixture for pure unit tests."""
    yield None


def test_enrich_stub_text_adds_class_and_instance_attributes():
    source_text = '''
class Demo:
    FLAG = True
    _QUEUE = factory()

    def __init__(self, name: str, payload: object) -> None:
        self.name = name
        self.payload: object = payload
'''
    stub_text = '''
class Demo:
    def __init__(self, name: str, payload: object) -> None: ...
'''

    enriched = sync_stubs._enrich_stub_text(source_text, stub_text)

    assert 'FLAG: bool' in enriched
    assert '_QUEUE: Incomplete' in enriched
    assert 'name: str' in enriched
    assert 'payload: object' in enriched
    assert 'from _typeshed import Incomplete as Incomplete' in enriched


def test_enrich_stub_text_preserves_method_signatures():
    source_text = '''
class Demo:
    INTERVAL = 1

    def __init__(self, destination: str) -> None:
        self.destination = destination

    def publish(self, payload: bytes) -> None:
        return None
'''
    stub_text = '''
class Demo:
    def __init__(self, destination: str) -> None: ...
    def publish(self, payload: bytes) -> None: ...
'''

    enriched = sync_stubs._enrich_stub_text(source_text, stub_text)

    assert 'def __init__(self, destination: str) -> None: ...' in enriched
    assert 'def publish(self, payload: bytes) -> None: ...' in enriched
    assert 'INTERVAL: int' in enriched
    assert 'destination: str' in enriched


def test_enrich_stub_text_is_idempotent():
    source_text = '''
class Demo:
    KIND = "demo"

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled
'''
    stub_text = '''
class Demo:
    def __init__(self, enabled: bool) -> None: ...
'''

    enriched = sync_stubs._enrich_stub_text(source_text, stub_text)

    assert sync_stubs._enrich_stub_text(source_text, enriched) == enriched


def test_enrich_stub_text_normalizes_empty_enum_helpers():
    source_text = '''
from enum import Enum

class StrEnum(str, Enum):
    def __str__(self) -> str:
        return str(self.value)

class DemoValue(StrEnum):
    one = "1"
'''
    stub_text = '''
from enum import Enum

class StrEnum(str, Enum): ...

class DemoValue(StrEnum):
    one = '1'
'''

    enriched = sync_stubs._enrich_stub_text(source_text, stub_text)

    assert 'class StrEnum' not in enriched
    assert 'class DemoValue(str, Enum):' in enriched


def test_enrich_stub_text_copies_local_inherited_methods():
    source_text = '''
class MsgQueueMixin:
    def _get_message(self) -> str | None:
        return None

class PublisherBase:
    def is_running(self) -> bool:
        return True

class DemoPublisher(PublisherBase, MsgQueueMixin):
    def run(self) -> None:
        self._get_message()
'''
    stub_text = '''
class MsgQueueMixin:
    def _get_message(self) -> str | None: ...

class PublisherBase:
    def is_running(self) -> bool: ...

class DemoPublisher(PublisherBase, MsgQueueMixin):
    def run(self) -> None: ...
'''

    enriched = sync_stubs._enrich_stub_text(source_text, stub_text)

    assert 'class DemoPublisher(PublisherBase, MsgQueueMixin):' in enriched
    assert '    def _get_message(self) -> str | None: ...' in enriched
    assert '    def is_running(self) -> bool: ...' in enriched


def test_enrich_stub_text_copies_imported_local_inherited_methods(tmp_path):
    package_dir = tmp_path / 'pkg'
    package_dir.mkdir()
    (package_dir / '__init__.py').write_text('', encoding='utf-8')
    (package_dir / 'base.py').write_text(
        '''
class MsgQueueMixin:
    def _get_message(self) -> str | None:
        return None

class PublisherBase:
    def is_running(self) -> bool:
        return True
''',
        encoding='utf-8',
    )
    source_path = package_dir / 'publisher.py'
    source_path.write_text(
        '''
from .base import MsgQueueMixin, PublisherBase

class DemoPublisher(PublisherBase, MsgQueueMixin):
    def run(self) -> None:
        self._get_message()
''',
        encoding='utf-8',
    )
    source_text = source_path.read_text(encoding='utf-8')
    stub_text = '''
from .base import MsgQueueMixin as MsgQueueMixin, PublisherBase as PublisherBase

class DemoPublisher(PublisherBase, MsgQueueMixin):
    def run(self) -> None: ...
'''

    enriched = sync_stubs._enrich_stub_text(
        source_text,
        stub_text,
        source_path=source_path,
    )

    assert '    def _get_message(self) -> str | None: ...' in enriched
    assert '    def is_running(self) -> bool: ...' in enriched


def test_enrich_stub_text_adds_missing_local_private_methods():
    source_text = '''
class Demo:
    def _helper(self) -> int:
        return 1

    def public(self) -> None:
        self._helper()
'''
    stub_text = '''
class Demo:
    def public(self) -> None: ...
'''

    enriched = sync_stubs._enrich_stub_text(source_text, stub_text)

    assert '    def _helper(self) -> int: ...' in enriched


def test_enrich_stub_text_adds_missing_local_private_methods_with_imported_types(
    tmp_path,
):
    package_dir = tmp_path / 'pkg'
    package_dir.mkdir()
    (package_dir / '__init__.py').write_text('', encoding='utf-8')
    source_path = package_dir / 'publisher.py'
    source_path.write_text(
        '''
from threading import Event

class DemoPublisher:
    def _request_shutdown(self) -> None:
        return None

    def ready(self) -> Event:
        return Event()
''',
        encoding='utf-8',
    )
    source_text = source_path.read_text(encoding='utf-8')
    stub_text = '''
from threading import Event

class DemoPublisher:
    def ready(self) -> Event: ...
'''

    enriched = sync_stubs._enrich_stub_text(
        source_text,
        stub_text,
        source_path=source_path,
    )

    assert '    def _request_shutdown(self) -> None: ...' in enriched
