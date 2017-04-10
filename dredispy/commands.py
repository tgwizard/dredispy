import re
import logging
import heapq
from datetime import datetime, timedelta

import gevent
import itertools
from typing import List, Union


logger = logging.getLogger(__name__)

_storage = {}
_expiration_pq = []
_expiration_key_finder = {}
_expiration_counter = itertools.count()
_removed_sentinel = object()


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


class CommandSyntaxError(RedisError):
    def __init__(self):
        super(CommandSyntaxError, self).__init__('syntax error')


class RedisString(RedisData):
    def __init__(self, data: Union[str, bytes]):
        self.data = data if isinstance(data, bytes) else data.encode('utf-8')

    def to_resp(self) -> bytes:
        return b''.join((b'+', self.data, b'\r\n'))


class BulkString(RedisString):
    def to_resp(self):
        l = str(len(self.data)).encode()
        return b''.join((b'$', l, b'\r\n', self.data, b'\r\n'))


class NullBulkString(RedisData):
    def to_resp(self):
        return b'$-1\r\n'


class RedisArray(RedisData):
    def __init__(self, items: List[RedisData]):
        self.items = items

    def to_resp(self):
        l = str(len(self.items)).encode()
        items_resp = tuple(i.to_resp() for i in self.items)
        return b''.join((b'*', l, b'\r\n') + items_resp)


def build_re_from_pattern(pattern: str):
    logger.warning('P!!!!!: %s', pattern)
    pattern = re.sub('(?<!\\\\)\\*', '.*', pattern)
    pattern = re.sub('(?<!\\\\)\\?', '.?', pattern)
    logger.warning('PPPP: %s', pattern)
    return re.compile('^%s$' % pattern)


def ensure_int(v: bytes) -> int:
    try:
        return int(v)
    except ValueError:
        raise RedisError('value is not an integer or out of range')


def get_handler(command, args):
    global _storage

    if len(args) != 1:
        raise WrongNumberOfArgumentsError(command)

    key = args[0]
    value = _storage.get(key)
    if value is None:
        return NullBulkString()
    return RedisString(value)


def keys_handler(command, args):
    global _storage

    if len(args) != 1:
        raise WrongNumberOfArgumentsError(command)

    result = []
    pattern_re = build_re_from_pattern(args[0].decode())
    logger.info('Will filter keys by pattern: re=%s', pattern_re.pattern)
    for key in _storage.keys():
        if pattern_re.fullmatch(key.decode()):
            result.append(key)

    return RedisArray([RedisString(key) for key in result])


def mget_handler(command, args):
    global _storage

    if len(args) == 0:
        raise WrongNumberOfArgumentsError(command)

    result = [_storage.get(key) for key in args]
    return RedisArray([RedisString(key) if key is not None else NullBulkString() for key in result])


def mset_handler(command, args):
    global _storage

    if len(args) % 2 != 0:
        raise WrongNumberOfArgumentsError(command)

    i = 0
    while i < len(args):
        key = args[i]
        value = args[i+1]

        logger.info('Setting key: key=%s, value=%s', key, value)
        _storage[key] = value
        _remove_key_from_expiration(key)

        i += 2

    return RedisString(b'OK')


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

    if len(args) < 2:
        raise WrongNumberOfArgumentsError(command)

    key = args[0]
    value = args[1]

    nx = False
    xx = False
    ex = None
    px = None

    if len(args) > 2:
        i = 2
        while i < len(args):
            if args[i] == b'nx':
                nx = True
            elif args[i] == b'xx':
                xx = True
            elif args[i] == b'ex':
                if i >= len(args):
                    raise CommandSyntaxError()
                ex = ensure_int(args[i+1])
                i += 1
            elif args[i] == b'px':
                if i >= len(args):
                    raise CommandSyntaxError()
                px = ensure_int(args[i+1])
                i += 1
            i += 1

    if xx and nx:
        raise CommandSyntaxError()

    if ex and px:
        raise CommandSyntaxError()

    key_exists = key in _storage
    if key_exists and nx:
        logger.info('Not setting key, exists and nx is specified: key=%s', key)
        return NullBulkString()
    if not key_exists and xx:
        logger.info('Not setting key, doesnt exist and xx is specified: key=%s', key)
        return NullBulkString()

    logger.info('Setting key: key=%s, value=%s', key, value)
    _storage[key] = value

    if px:
        _set_expiry_for_key(key, px)
    elif ex:
        _set_expiry_for_key(key, ex * 1000)
    else:
        _remove_key_from_expiration(key)

    return RedisString(b'OK')


_CMD_HANDLERS = {
    b'GET': get_handler,
    b'KEYS': keys_handler,
    b'MGET': mget_handler,
    b'MSET': mset_handler,
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


def periodic_handler():
    while True:
        gevent.sleep(1)
        now = datetime.utcnow()

        logger.debug('Running periodic handler: now=%s', now)

        while _expiration_pq and _expiration_pq[0][0] < now:
            key = _pop_expiration_key()
            if not key:
                continue
            logger.info('Evicting expired key: key=%s', key)
            _storage.pop(key, None)


def _set_expiry_for_key(key: bytes, milliseconds: int):
    expires_at = datetime.utcnow() + timedelta(milliseconds=milliseconds)
    logger.info(
        'Setting expiry for key: key=%s, milliseconds=%s, expires_at=%s',
        key, milliseconds, expires_at
    )

    if key in _expiration_key_finder:
        _remove_key_from_expiration(key)

    count = next(_expiration_counter)
    entry = [expires_at, count, key]
    _expiration_key_finder[key] = entry

    heapq.heappush(_expiration_pq, entry)


def _remove_key_from_expiration(key):
    logger.info('Removing expiry for key: key=%s', key)
    entry = _expiration_key_finder.pop(key, None)
    if entry is None:
        return
    entry[-1] = _removed_sentinel


def _pop_expiration_key():
    while _expiration_pq:
        expires_at, count, key = heapq.heappop(_expiration_pq)
        if key is not _removed_sentinel:
            del _expiration_key_finder[key]
            return key
    return None


def _peek_expiration_entry():
    if not _expiration_pq:
        return None
    return _expiration_pq
