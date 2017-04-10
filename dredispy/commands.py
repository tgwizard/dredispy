from typing import List, Union


_storage = {}


class QuitConnection(Exception):
    pass


class RedisData(object):
    def to_resp(self) -> bytes:
        raise NotImplementedError()

    def __str__(self):
        return self.to_resp().decode()


class RedisError(Exception, RedisData):
    kind = 'ERR'
    msg = 'Something went wrong'

    def __init__(self, msg=None, kind=None):
        self.msg = msg if msg else self.msg
        self.kind = kind if kind else self.kind

    def to_resp(self) -> bytes:
        return ('-%s %s\r\n' % (self.kind, self.msg)).encode('utf-8')


class UnknownCommandError(RedisError):
    def __init__(self, command: bytes):
        super(UnknownCommandError, self).__init__('unknown command \'%s\'' % command.decode())


class WrongNumberOfArgumentsError(RedisError):
    def __init__(self, command: bytes):
        super(WrongNumberOfArgumentsError, self).__init__(
            'wrong number of arguments for \'%s\' command' % command.decode(),
        )


class RedisString(RedisData):
    def __init__(self, data: Union[str, bytes]):
        self.data = data if isinstance(data, bytes) else data.encode('utf-8')

    def to_resp(self) -> bytes:
        return b''.join((b'+', self.data, b'\r\n'))


class BulkString(RedisString):
    def to_resp(self):
        l = str(len(self.data)).encode()
        return b''.join((b'$', l, '\r\n', self.data, b'\r\n'))


class NullBulkString(RedisData):
    def to_resp(self):
        return b'$-1\r\n'


def get_handler(command, args):
    global _storage

    if len(args) != 1:
        raise WrongNumberOfArgumentsError(command)

    key = args[0]
    value = _storage.get(key)
    if value is None:
        return NullBulkString()
    return RedisString(value)


def ping_handler(command, args):
    if len(args) == 0:
        return RedisString('PONG')
    elif len(args) == 1:
        return RedisString(args[0])
    else:
        raise WrongNumberOfArgumentsError(command)


def quit_handler(command, args):
    raise QuitConnection()


def set_handler(command, args):
    global _storage

    assert len(args) >= 2
    key = args[0]
    value = args[1]
    _storage[key] = value

    return RedisString(b'OK')


_CMD_HANDLERS = {
    b'GET': get_handler,
    b'PING': ping_handler,
    b'QUIT': quit_handler,
    b'SET': set_handler,
}


def process_command(cmd_parts: List[bytes]):
    assert len(cmd_parts) > 0
    command = cmd_parts[0]
    command_handler = _CMD_HANDLERS.get(command.upper())
    if not command_handler:
        raise UnknownCommandError(command)

    return command_handler(command, cmd_parts[1:])
