from datetime import datetime
from socket import socket as _socket
from unittest.mock import Mock

import pytest

from dredispy.command import CommandProcessor, build_re_from_pattern, Storage
from dredispy.protocol import (
    WrongNumberOfArgumentsError,
    RedisString,
    RedisArray,
    RedisNullBulkString,
    RedisBulkString,
)
from dredispy.server import Connection


@pytest.fixture()
def storage():
    return Storage()


@pytest.fixture()
def command_processor(storage):
    return CommandProcessor(storage)


@pytest.fixture()
def connection():
    return Connection(Mock(spec=_socket), ('127.0.0.1', 50002))


class TestBuildReFromPattern(object):
    @pytest.mark.parametrize('pattern,expected_re_pattern', [
        (b'foo', r'^foo$'),
        (b'foo bar baz', r'^foo bar baz$'),

        # Globs.
        (b'*', r'^.*$'),
        (b'?', r'^.$'),
        (b'[a-z]', r'^[a-z]$'),
        (b'f?o b[a-z]r*', r'^f.o b[a-z]r.*$'),
        (b'a*b*c*', r'^a.*b.*c.*$'),
        (b'a?b?c?', r'^a.b.c.$'),

        # Escaped globs.
        (b'\*', r'^\*$'),
        (b'\?', r'^\?$'),
    ])
    def test_build_re_from_pattern(self, pattern, expected_re_pattern):
        pattern_re = build_re_from_pattern(pattern)
        assert pattern_re.pattern == expected_re_pattern, pattern_re


class TestCmdPing(object):
    def test_ping_no_arguments_returns_pong(self, command_processor, connection):
        result = command_processor.process_command([b'ping'], connection=connection)

        assert result == RedisString('PONG')

    def test_ping_one_argument_echoes_back(self, command_processor, connection):
        result = command_processor.process_command(
            [b'ping', b'echo this please'], connection=connection,
        )

        assert result == RedisString('echo this please')

    def test_ping_wrong_number_of_arguments(self, command_processor, connection):
        with pytest.raises(WrongNumberOfArgumentsError):
            command_processor.process_command([b'ping', b'2', b'3'], connection=connection)


class TestCmdKeys(object):
    @pytest.mark.parametrize('args,expected_result', [
        ([b'foo'], [b'foo']),
        ([b'foo3'], []),
        ([b'foo '], []),
        ([b'with spaces in '], [b'with spaces in ']),
        ([b'non-existing'], []),
        ([b'expired'], []),
        ([b'not-yet-expired'], [b'not-yet-expired']),

        # With patterns.
        ([b'foo*'], [b'foo', b'foo1', b'foo2']),
        ([b'foo?'], [b'foo1', b'foo2']),
        ([b'*expired'], [b'not-yet-expired']),
    ])
    def test_keys_return_matching_keys(
            self, storage, command_processor, connection, args, expected_result):
        now = datetime.utcnow()
        storage.dbs[0].kv = {
            b'foo': b'1',
            b'foo1': b'1',
            b'foo2': b'1',
            b'with spaces in ': b'1',
            b'expired': b'1',
            b'not-yet-expired': b'1',
        }
        storage.dbs[3].kv = {
            b'foo3': b'1'
        }

        storage.dbs[0].set_expiry_for_key(b'expired', 0, now)
        storage.dbs[0].set_expiry_for_key(b'not-yet-expired', 2000, now)

        result = command_processor.process_command([b'keys'] + args, connection=connection)

        assert result == RedisArray([RedisString(r) for r in expected_result]), result

    @pytest.mark.parametrize('args', [[b'*'], [b'foo']])
    def test_keys_no_keys_return_empty_array(self, command_processor, connection, args):
        result = command_processor.process_command([b'keys'] + args, connection=connection)
        assert result == RedisArray([])

    @pytest.mark.parametrize('args', [[], [b'foo', b'bar']])
    def test_keys_wrong_number_of_arguments(self, command_processor, connection, args):
        with pytest.raises(WrongNumberOfArgumentsError):
            command_processor.process_command([b'keys'] + args, connection=connection)


class TestCmdGet(object):
    @pytest.mark.parametrize('args,expected_result', [
        ([b'foo'], RedisBulkString(b'foo-val')),
        ([b'foo '], RedisNullBulkString()),
        ([b'with spaces in '], RedisBulkString(b'spaces val here\r\nx')),
        ([b'non-existing'], RedisNullBulkString()),
        ([b'expired'], RedisNullBulkString()),
        ([b'not-yet-expired'], RedisBulkString(b'2')),

        # Don't support globbing
        ([b'fo*'], RedisNullBulkString()),
    ])
    def test_keys_return_matching_keys(
            self, storage, command_processor, connection, args, expected_result):
        now = datetime.utcnow()
        storage.dbs[0].kv = {
            b'foo': b'foo-val',
            b'with spaces in ': b'spaces val here\r\nx',
            b'expired': b'1',
            b'not-yet-expired': b'2',
        }

        storage.dbs[0].set_expiry_for_key(b'expired', 0, now)
        storage.dbs[0].set_expiry_for_key(b'not-yet-expired', 2000, now)

        result = command_processor.process_command([b'get'] + args, connection=connection)

        assert result == expected_result, result

    def test_get_no_keys_return_null(self, command_processor, connection):
        result = command_processor.process_command([b'get', b'foo'], connection=connection)
        assert result == RedisNullBulkString(), result

    @pytest.mark.parametrize('args', [[], [b'foo', b'bar']])
    def test_get_wrong_number_of_arguments(self, command_processor, connection, args):
        with pytest.raises(WrongNumberOfArgumentsError):
            command_processor.process_command([b'get'] + args, connection=connection)


class TestCmdSet(object):
    pass


class TestCmdMget(object):
    pass


class TestCmdMset(object):
    pass
