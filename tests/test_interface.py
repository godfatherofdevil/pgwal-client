# pylint: disable=C0116
"""Test interface models"""
import pytest
from pydantic import ValidationError

from lib.src.pgwal.interface import (
    WALReplicationOpts,
    _WALReplicationActions,
    WALReplicationValues,
)


def test_repl_opts_actions_default():
    repl_opts = WALReplicationOpts()
    assert isinstance(repl_opts.actions, list)
    assert repl_opts.actions == _WALReplicationActions


def test_repl_opts_actions_serializes():
    repl_opts = WALReplicationOpts(
        actions=[
            WALReplicationValues.insert,
            WALReplicationValues.update,
        ]
    )
    repl_opts = repl_opts.model_dump(by_alias=True)

    assert repl_opts['actions'] == 'insert, update'


def test_repl_opts_actions_init_with_str():
    repl_opts = WALReplicationOpts(actions='insert, update, delete')
    repl_opts_ = repl_opts.model_dump(by_alias=True)

    assert repl_opts.actions == 'insert, update, delete'
    assert repl_opts_['actions'] == 'insert, update, delete'


def test_repl_opts_actions_validates_list():
    with pytest.raises(ValidationError):
        WALReplicationOpts(
            actions=[
                WALReplicationValues.insert,
                '1234',
                'foo',
            ]
        )


def test_repl_opts_actions_validates_str():
    with pytest.raises(ValidationError):
        WALReplicationOpts(actions='insert, update, invalid')


@pytest.mark.parametrize(
    'format_version, expected',
    ((WALReplicationValues.one, '1'), (WALReplicationValues.two, '2')),
)
def test_format_version_expected(format_version, expected):
    repl_opts = WALReplicationOpts(format_version=format_version)
    repl_opts_ = repl_opts.model_dump(by_alias=True)

    assert repl_opts_['format-version'] == expected


@pytest.mark.parametrize(
    'format_version',
    (
        WALReplicationValues.zero,
        WALReplicationValues.nil,
        WALReplicationValues.insert,
        WALReplicationValues.update,
        WALReplicationValues.delete,
        WALReplicationValues.truncate,
    ),
)
def test_format_version_not_expected(format_version):
    with pytest.raises(ValidationError):
        WALReplicationOpts(format_version=format_version)
