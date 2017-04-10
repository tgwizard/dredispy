import pytest

from dredispy.command import CommandProcessor
from dredispy.data import RedisString, WrongNumberOfArgumentsError


@pytest.fixture()
def command_processor():
    return CommandProcessor()


class TestPingHandler(object):
    def test_ping_no_arguments_returns_pong(self, command_processor):
        result = command_processor.process_command([b'ping'])

        assert result == RedisString('PONG')

    def test_ping_one_argument_echoes_back(self, command_processor):
        result = command_processor.process_command([b'ping', b'echo this please'])

        assert result == RedisString('echo this please')

    def test_wrong_number_of_arguments(self, command_processor):
        with pytest.raises(WrongNumberOfArgumentsError):
            command_processor.process_command([b'ping', b'2', b'3'])
