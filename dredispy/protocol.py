from typing import Union, List


class RESPType(object):
    String = b'+'
    Error = b'-'
    Integer = b':'
    BulkString = b'$'
    Array = b'*'


class RedisData(object):
    def to_resp(self) -> bytes:
        raise NotImplementedError()

    def __str__(self):
        return self.to_resp().decode()

    def __eq__(self, other):
        if not isinstance(other, RedisData):
            return False
        return self.to_resp() == other.to_resp()

    def __hash__(self):
        return hash(self.to_resp())


class RedisError(Exception, RedisData):
    kind = 'ERR'
    msg = 'Something went wrong'

    def __init__(self, msg: Union[str, None]=None, kind: Union[str, None]=None):
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


class CommandSyntaxError(RedisError):
    def __init__(self):
        super(CommandSyntaxError, self).__init__('syntax error')


class RedisString(RedisData):
    def __init__(self, data: Union[str, bytes]):
        self.data = data if isinstance(data, bytes) else data.encode('utf-8')

    def to_resp(self) -> bytes:
        return b''.join((b'+', self.data, b'\r\n'))


class RedisBulkString(RedisString):
    def to_resp(self):
        l = str(len(self.data)).encode()
        return b''.join((b'$', l, b'\r\n', self.data, b'\r\n'))


class RedisNullBulkString(RedisData):
    def to_resp(self):
        return b'$-1\r\n'


class RedisInteger(RedisData):
    def __init__(self, data: int):
        self.data = data

    def to_resp(self) -> bytes:
        b = str(self.data).encode()
        return b''.join((b':', b, b'\r\n'))


class RedisArray(RedisData):
    def __init__(self, items: List[RedisData]):
        self.items = items

    def to_resp(self):
        l = str(len(self.items)).encode()
        items_resp = tuple(i.to_resp() for i in self.items)
        return b''.join((b'*', l, b'\r\n') + items_resp)


class RedisMultipleResponses(RedisData):
    def __init__(self, responses: List[RedisData]):
        self.responses = responses

    def to_resp(self):
        return b''.join(r.to_resp() for r in self.responses)
