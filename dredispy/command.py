import re
import logging
import heapq
import itertools
from datetime import datetime, timedelta
from typing import List


from dredispy.data import (
    RedisError,
    UnknownCommandError,
    CommandSyntaxError,
    WrongNumberOfArgumentsError,
    RedisData,
    RedisString,
    RedisNullBulkString,
    RedisArray,
)

logger = logging.getLogger(__name__)


def build_re_from_pattern(pattern: str):
    pattern = re.sub('(?<!\\\\)\\*', '.*', pattern)
    pattern = re.sub('(?<!\\\\)\\?', '.?', pattern)
    return re.compile('^%s$' % pattern)


def ensure_int(v: bytes) -> int:
    try:
        return int(v)
    except ValueError:
        raise RedisError('value is not an integer or out of range')


class CommandProcessor(object):
    _storage = {}
    _expiration_pq = []
    _expiration_key_finder = {}
    _expiration_counter = itertools.count()
    _removed_sentinel = object()

    def get_handler(self, command, args):
        if len(args) != 1:
            raise WrongNumberOfArgumentsError(command)

        key = args[0]
        value = self._storage.get(key)
        if value is None:
            return RedisNullBulkString()
        return RedisString(value)

    def keys_handler(self, command, args):
        if len(args) != 1:
            raise WrongNumberOfArgumentsError(command)

        result = []
        pattern_re = build_re_from_pattern(args[0].decode())
        logger.info('Will filter keys by pattern: re=%s', pattern_re.pattern)
        for key in self._storage.keys():
            if pattern_re.fullmatch(key.decode()):
                result.append(key)

        return RedisArray([RedisString(key) for key in result])

    def mget_handler(self, command, args):
        if len(args) == 0:
            raise WrongNumberOfArgumentsError(command)

        result = [self._storage.get(key) for key in args]
        return RedisArray([
            RedisString(key) if key is not None else RedisNullBulkString() for key in result
        ])

    def mset_handler(self, command, args):
        global _storage

        if len(args) % 2 != 0:
            raise WrongNumberOfArgumentsError(command)

        i = 0
        while i < len(args):
            key = args[i]
            value = args[i+1]

            logger.info('Setting key: key=%s, value=%s', key, value)
            self._storage[key] = value
            self._remove_key_from_expiration(key)

            i += 2

        return RedisString(b'OK')

    def ping_handler(self, command, args):
        if len(args) == 0:
            return RedisString('PONG')
        elif len(args) == 1:
            return RedisString(args[0])
        else:
            raise WrongNumberOfArgumentsError(command)

    def set_handler(self, command, args):
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

        key_exists = key in self._storage
        if key_exists and nx:
            logger.info('Not setting key, exists and nx is specified: key=%s', key)
            return RedisNullBulkString()
        if not key_exists and xx:
            logger.info('Not setting key, doesnt exist and xx is specified: key=%s', key)
            return RedisNullBulkString()

        logger.info('Setting key: key=%s, value=%s', key, value)
        self._storage[key] = value

        if px:
            self._set_expiry_for_key(key, px)
        elif ex:
            self._set_expiry_for_key(key, ex * 1000)
        else:
            self._remove_key_from_expiration(key)

        return RedisString(b'OK')

    def process_command(self, cmd_parts: List[bytes]) -> RedisData:
        assert len(cmd_parts) > 0
        command = cmd_parts[0]

        command_handler = getattr(self, '%s_handler' % command.decode().lower()[:50], None)
        if not command_handler:
            raise UnknownCommandError(command)

        return command_handler(command, cmd_parts[1:])

    def process_periodic_task(self):
        now = datetime.utcnow()

        logger.debug('Running periodic handler: now=%s', now)

        while self._expiration_pq and self._expiration_pq[0][0] < now:
            key = self._pop_expiration_key()
            if not key:
                continue
            logger.info('Evicting expired key: key=%s', key)
            self._storage.pop(key, None)

    def _set_expiry_for_key(self, key: bytes, milliseconds: int):
        expires_at = datetime.utcnow() + timedelta(milliseconds=milliseconds)
        logger.info(
            'Setting expiry for key: key=%s, milliseconds=%s, expires_at=%s',
            key, milliseconds, expires_at
        )

        if key in self._expiration_key_finder:
            self._remove_key_from_expiration(key)

        count = next(self._expiration_counter)
        entry = [expires_at, count, key]
        self._expiration_key_finder[key] = entry

        heapq.heappush(self._expiration_pq, entry)

    def _remove_key_from_expiration(self, key):
        logger.info('Removing expiry for key: key=%s', key)
        entry = self._expiration_key_finder.pop(key, None)
        if entry is None:
            return
        entry[-1] = self._removed_sentinel

    def _pop_expiration_key(self):
        while self._expiration_pq:
            expires_at, count, key = heapq.heappop(self._expiration_pq)
            if key is not self._removed_sentinel:
                del self._expiration_key_finder[key]
                return key
        return None

    def _peek_expiration_entry(self):
        if not self._expiration_pq:
            return None
        return self._expiration_pq
