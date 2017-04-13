import re
import logging
import heapq
import itertools
import weakref
from collections import defaultdict
from datetime import datetime, timedelta

from gevent.pool import Pool
from typing import List, Union, Tuple

from dredispy.protocol import (
    RedisError,
    UnknownCommandError,
    CommandSyntaxError,
    WrongNumberOfArgumentsError,
    RedisData,
    RedisString,
    RedisBulkString,
    RedisNullBulkString,
    RedisInteger,
    RedisArray,
    RedisMultipleResponses,
)

logger = logging.getLogger(__name__)


def build_re_from_pattern(pattern: Union[str, bytes]):
    if isinstance(pattern, bytes):
        pattern = pattern.decode()
    pattern = re.sub('(?<!\\\\)\\*', '.*', pattern)
    pattern = re.sub('(?<!\\\\)\\?', '.', pattern)
    return re.compile('^%s$' % pattern)


def ensure_int(v: bytes) -> int:
    try:
        return int(v)
    except ValueError:
        raise RedisError('value is not an integer or out of range')


PUB_SUB_COMMANDS = frozenset((
    'subscribe',
    'psubscribe',
    'unsubscribe',
    'punsubscribe',
    'publish',
    'pubsub',
))


class DB(object):
    def __init__(self, index: int):
        self.index = index
        self.kv = {}
        self.expiration_pq = []
        self._expiration_key_finder = {}
        self._expiration_counter = itertools.count()
        self._removed_sentinel = object()

    def get_active_key(self, key: bytes, now: datetime):
        value = self.kv.get(key)
        if value is None:
            return None
        if not self.is_key_active(key, now):
            return None
        return value

    def set_expiry_for_key(self, key: bytes, milliseconds: int, now: datetime):
        expires_at = now + timedelta(milliseconds=milliseconds)
        logger.info(
            'Setting expiry for key: key=%s, milliseconds=%s, expires_at=%s',
            key, milliseconds, expires_at
        )

        if key in self._expiration_key_finder:
            self.remove_key_from_expiration(key)

        count = next(self._expiration_counter)
        entry = [expires_at, count, key]
        self._expiration_key_finder[key] = entry

        heapq.heappush(self.expiration_pq, entry)

    def remove_key_from_expiration(self, key: bytes):
        logger.info('Removing expiry for key: key=%s', key)
        entry = self._expiration_key_finder.pop(key, None)
        if entry is None:
            return
        entry[-1] = self._removed_sentinel

    def pop_expiration_key(self):
        while self.expiration_pq:
            expires_at, count, key = heapq.heappop(self.expiration_pq)
            if key is not self._removed_sentinel:
                del self._expiration_key_finder[key]
                return key
        return None

    def is_key_active(self, key: bytes, now: datetime):
        entry = self._expiration_key_finder.get(key)
        if not entry:
            return True
        if entry[0] >= now:
            return True
        return False

    def num_keys(self, now: datetime) -> Tuple[int, int]:
        num_active_keys = 0
        num_keys_with_expiry = 0
        for key in self.kv.keys():
            if self.is_key_active(key, now):
                num_active_keys += 1
            if key in self._expiration_key_finder:
                num_keys_with_expiry += 1

        return num_active_keys, num_keys_with_expiry


class Storage(object):
    def __init__(self):
        self.dbs = [DB(i) for i in range(16)]

    def process_periodic_task(self):
        now = datetime.utcnow()
        logger.debug('Running periodic handler: now=%s', now)

        for db_index, db in enumerate(self.dbs):
            while db.expiration_pq and db.expiration_pq[0][0] < now:
                key = db.pop_expiration_key()
                if not key:
                    continue
                logger.info('Evicting expired key: db=%s, key=%s', db_index, key)
                db.kv.pop(key, None)


class CommandHandler(object):
    def __init__(self, storage: Storage):
        self.storage = storage

    def _get_db(self, connection) -> DB:
        return self.storage.dbs[connection.db_index]

    def cmd_ping(self, command, args, now, connection):
        if len(args) == 0:
            return RedisString('PONG')
        elif len(args) == 1:
            return RedisString(args[0])
        else:
            raise WrongNumberOfArgumentsError(command)

    def cmd_info(self, command, args, now, connection):
        def db_info(db_index):
            db = self.storage.dbs[db_index]
            num_active_keys, num_keys_with_expiry = db.num_keys(now)
            if db_index > 0 and num_active_keys == 0:
                return None

            return f'db{db_index}:keys={num_active_keys},expires={num_keys_with_expiry}'

        db_info_rows = [db_info(0)] + [db_info(i) for i in range(1, len(self.storage.dbs))]
        s = """
# Keyspace
{db_info}
""".format(
            db_info='\n'.join(r for r in db_info_rows if r),
        )
        return RedisBulkString(s.lstrip())

    def cmd_select(self, command, args, now, connection):
        if len(args) != 1:
            raise WrongNumberOfArgumentsError(command)

        db_index = ensure_int(args[0])
        if db_index < 0 or db_index > len(self.storage.dbs):
            raise RedisError('invalid DB index')

        connection.db_index = db_index
        return RedisString(b'OK')

    def cmd_keys(self, command, args, now, connection):
        if len(args) != 1:
            raise WrongNumberOfArgumentsError(command)

        db = self._get_db(connection)

        result = []
        pattern_re = build_re_from_pattern(args[0])
        logger.info('Will filter keys by pattern: re=%s', pattern_re.pattern)
        for key in db.kv.keys():
            if not pattern_re.fullmatch(key.decode()):
                continue
            if not db.is_key_active(key, now):
                continue
            result.append(key)

        return RedisArray([RedisString(key) for key in result])

    def cmd_get(self, command, args, now, connection):
        if len(args) != 1:
            raise WrongNumberOfArgumentsError(command)

        db = self._get_db(connection)

        key = args[0]
        value = db.get_active_key(key, now)
        if value is None:
            return RedisNullBulkString()
        return RedisBulkString(value)

    def cmd_set(self, command, args, now, connection):
        if len(args) < 2:
            raise WrongNumberOfArgumentsError(command)

        db = self._get_db(connection)

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

        key_exists = db.get_active_key(key, now) is not None
        if key_exists and nx:
            logger.info('Not setting key, exists and nx is specified: db=%s, key=%s', db.index, key)
            return RedisNullBulkString()
        if not key_exists and xx:
            logger.info(
                'Not setting key, doesnt exist and xx is specified: db=%s, key=%s', db.index, key,
            )
            return RedisNullBulkString()

        logger.info('Setting key: db=%s, key=%s, value=%s', db.index, key, value)
        db.kv[key] = value

        if px:
            db.set_expiry_for_key(key, px, now)
        elif ex:
            db.set_expiry_for_key(key, ex * 1000, now)
        else:
            db.remove_key_from_expiration(key)

        return RedisString(b'OK')

    def cmd_mget(self, command, args, now, connection):
        if len(args) == 0:
            raise WrongNumberOfArgumentsError(command)

        db = self._get_db(connection)

        result = [db.get_active_key(key, now) for key in args]
        return RedisArray([
            RedisString(key) if key is not None else RedisNullBulkString() for key in result
        ])

    def cmd_mset(self, command, args, now, connection):
        global _storage

        if len(args) < 2:
            raise WrongNumberOfArgumentsError(command)
        if len(args) % 2 != 0:
            raise WrongNumberOfArgumentsError(command)

        db = self._get_db(connection)

        i = 0
        while i < len(args):
            key = args[i]
            value = args[i+1]

            logger.info('Setting key: db=%s, key=%s, value=%s', db.index, key, value)
            db.kv[key] = value
            db.remove_key_from_expiration(key)

            i += 2

        return RedisString(b'OK')


class PubSubHandler(object):
    def __init__(self):
        self._channel_subscriptions = defaultdict(weakref.WeakSet)
        self._connection_channel_map = weakref.WeakKeyDictionary()
        self._connection_pattern_channel_map = weakref.WeakKeyDictionary()

        self._write_pool = Pool()

    def _ensure_connection(self, connection):
        self._connection_channel_map.setdefault(connection, set())
        self._connection_pattern_channel_map.setdefault(connection, set())

    def _current_connection_subscriptions(self, connection):
        return len(self._connection_channel_map[connection]) + \
               len(self._connection_pattern_channel_map[connection])

    def cmd_subscribe(self, command, args: List[bytes], now, connection):
        if len(args) == 0:
            raise WrongNumberOfArgumentsError(command)

        self._ensure_connection(connection)

        responses = []
        for channel in args:
            self._channel_subscriptions[channel].add(connection)
            self._connection_channel_map[connection].add(channel)

            responses.append(
                RedisArray([
                    RedisBulkString(b'subscribe'),
                    RedisBulkString(channel),
                    RedisInteger(self._current_connection_subscriptions(connection))
                ])
            )

        connection.state = 'pubsub'

        return RedisMultipleResponses(responses)

    def cmd_unsubscribe(self, command, args, now, connection):
        self._ensure_connection(connection)

        channels_to_remove = args
        if len(args) == 0:
            channels_to_remove = list(self._connection_channel_map[connection])

        responses = []
        for channel in channels_to_remove:
            if connection in self._channel_subscriptions[channel]:
                self._channel_subscriptions[channel].remove(connection)
                self._connection_channel_map[connection].remove(channel)

            responses.append(
                RedisArray([
                    RedisBulkString(b'unsubscribe'),
                    RedisBulkString(channel),
                    RedisInteger(self._current_connection_subscriptions(connection))
                ])
            )

        if self._current_connection_subscriptions(connection) == 0:
            connection.state = 'normal'

        return RedisMultipleResponses(responses)

    def cmd_psubscribe(self, command, args, now, connection):
        if len(args) == 0:
            raise WrongNumberOfArgumentsError(command)

        self._ensure_connection(connection)

        responses = []
        for pattern in args:
            self._connection_pattern_channel_map[connection].add(pattern)

            responses.append(
                RedisArray([
                    RedisBulkString(b'psubscribe'),
                    RedisBulkString(pattern),
                    RedisInteger(self._current_connection_subscriptions(connection))
                ])
            )

        return RedisMultipleResponses(responses)

    def cmd_punsubscribe(self, command, args, now, connection):
        self._ensure_connection(connection)

        patterns_to_remove = args
        if len(args) == 0:
            patterns_to_remove = list(self._connection_pattern_channel_map[connection])

        responses = []
        for pattern in patterns_to_remove:
            if pattern in self._connection_pattern_channel_map[connection]:
                self._connection_pattern_channel_map[connection].remove(pattern)

            responses.append(
                RedisArray([
                    RedisBulkString(b'punsubscribe'),
                    RedisBulkString(pattern),
                    RedisInteger(self._current_connection_subscriptions(connection))
                ])
            )

        if self._current_connection_subscriptions(connection) == 0:
            connection.state = 'normal'

        return RedisMultipleResponses(responses)

    def cmd_publish(self, command, args, now, connection):
        if len(args) != 2:
            raise WrongNumberOfArgumentsError(command)

        channel = args[0]
        message = args[1]

        # All connections subscribing to the channel.
        channel_connections = set(self._channel_subscriptions[channel])
        # All connections subscribing to a pattern matching the channel (may contain duplicates).
        pattern_connection_tuples = []
        for c, patterns in self._connection_pattern_channel_map.items():
            for pattern in patterns:
                if build_re_from_pattern(pattern).match(channel.decode()):
                    pattern_connection_tuples.append((pattern, c))

        if len(channel_connections) >= 0:
            data = RedisArray([
                RedisBulkString(b'message'),
                RedisBulkString(channel),
                RedisBulkString(message)
            ])
            logger.info(
                'Triggering sending message to subscribed connections: '
                'channel=%s, data=%s, num_connections=%s, connections=%s',
                channel, data, len(channel_connections), channel_connections
            )

            for c in channel_connections:
                # TODO: This might result in out-of-order messages. Do it with a queue and
                # connection-specific job instead?
                self._write_pool.spawn(c.write, data=data)

        if len(pattern_connection_tuples) >= 0:
            for pattern, c in pattern_connection_tuples:
                data = RedisArray([
                    RedisBulkString(b'pmessage'),
                    RedisBulkString(pattern),
                    RedisBulkString(message)
                ])
                logger.info(
                    'Triggering sending message to subscribed connections matching pattern: '
                    'channel=%s, pattern=%s, data=%s, connection=%s',
                    channel, pattern, data, c
                )

                self._write_pool.spawn(c.write, data=data)

        count = len(channel_connections) + len(pattern_connection_tuples)
        return RedisInteger(count)

    def cmd_pubsub(self, command, args, now, connection):
        if len(args) < 1:
            raise WrongNumberOfArgumentsError(command)

        subcommand = args[0].lower()
        if subcommand == b'channels':
            # Return the active channels, matching the optional pattern.
            pattern_re = re.compile('^.*$')
            if len(args) == 2:
                pattern_re = build_re_from_pattern(args[1].decode())
            elif len(args) > 2:
                raise WrongNumberOfArgumentsError(command)

            result = [
                RedisBulkString(channel)
                for channel, connections in self._channel_subscriptions.items()
                if pattern_re.match(channel.decode()) and len(connections) > 0
            ]
            return RedisArray(result)
        elif subcommand == b'numsub':
            # Return the number of subscribers for each queried channel.
            channels = args[1:]
            result = []
            for channel in channels:
                result.extend([
                    RedisBulkString(channel),
                    RedisInteger(len(self._channel_subscriptions[channel])),
                ])
            return RedisArray(result)
        elif subcommand == b'numpat':
            # Return the number pattern subscriptions.
            count = sum(map(lambda x: len(x), self._connection_pattern_channel_map.keys()))
            return RedisInteger(count)
        else:
            raise RedisError(
                'Unknown PUBSUB subcommand or wrong number of arguments '
                'for \'%s\'' % subcommand.decode(),
            )


class CommandProcessor(object):
    def __init__(self, storage: Storage):
        self.command_handler = CommandHandler(storage)
        self.pubsub_handler = PubSubHandler()

    def process_command(self, cmd_parts: List[bytes], connection) -> RedisData:
        now = datetime.utcnow()
        assert len(cmd_parts) > 0
        command = cmd_parts[0]
        args = cmd_parts[1:]
        # TODO: Proper validation of this
        clean_command = command.decode().lower()[:50]

        logger.info('Processing command: command=%s, args=%s', command, args)

        try:
            if clean_command in PUB_SUB_COMMANDS:
                command_handler = getattr(self.pubsub_handler, 'cmd_%s' % clean_command, None)
            else:
                command_handler = getattr(self.command_handler, 'cmd_%s' % clean_command, None)

            if not command_handler:
                raise UnknownCommandError(command)

            if connection.state == 'pubsub':
                if clean_command not in PUB_SUB_COMMANDS and clean_command != 'ping':
                    raise RedisError(
                        'only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context'
                    )

            result = command_handler(command, args, now=now, connection=connection)
        except RedisError as e:
            logger.info(
                'Redis error during command processing: command=%s, args=%s, result=%s',
                command, args, e.to_resp(),
            )
            raise
        logger.info('Command processed: result=%s', result)
        return result
