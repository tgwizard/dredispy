from socket import socket as _socket
from unittest.mock import Mock

import pytest

from dredispy.command import CommandProcessor
from dredispy.data import RedisString, WrongNumberOfArgumentsError
from dredispy.server import Connection


@pytest.fixture()
def command_processor():
    return CommandProcessor()


@pytest.fixture()
def connection():
    return Connection(Mock(spec=_socket), ('127.0.0.1', 50002))


class TestPingHandler(object):
    def test_ping_no_arguments_returns_pong(self, command_processor, connection):
        result = command_processor.process_command([b'ping'], connection=connection)

        assert result == RedisString('PONG')

    def test_ping_one_argument_echoes_back(self, command_processor, connection):
        result = command_processor.process_command(
            [b'ping', b'echo this please'], connection=connection,
        )

        assert result == RedisString('echo this please')

    def test_wrong_number_of_arguments(self, command_processor, connection):
        with pytest.raises(WrongNumberOfArgumentsError):
            command_processor.process_command([b'ping', b'2', b'3'], connection=connection)
