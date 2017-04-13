import logging

from socket import socket as _socket

from dredispy.command import CommandProcessor
from dredispy.data import RedisError, RedisData, RESPType

logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self, socket: _socket, address):
        self.socket = socket
        self.address = address

    def write(self, data: RedisData):
        b = data.to_resp()
        self.socket.sendall(b)

    def command_stream(self):
        buffer = b''

        def read():
            nonlocal buffer
            chunk: bytes = self.socket.recv(2048)
            if not chunk:
                return False

            buffer = b''.join((buffer, chunk))
            return True

        def ensure_buffer(min_len=1):
            nonlocal buffer
            if len(buffer) >= min_len:
                return True
            return read()

        def read_to_crlf():
            nonlocal buffer
            while True:
                crlf_pos = buffer.find(b'\r\n')
                if crlf_pos == -1:
                    ensure_buffer(min_len=len(buffer) + 1)
                    continue

                s = buffer[1:crlf_pos]
                buffer = buffer[crlf_pos + 2:]
                return s

        while True:
            if not read():
                return None

            assert buffer[0:1] == RESPType.Array, 'Invalid start'
            crlf_pos = buffer.find(b'\r\n')
            if crlf_pos == -1:
                logger.debug('Have not read enough client data: buffer=%s', buffer)
                continue

            array_len = int(buffer[1:crlf_pos])
            buffer = buffer[crlf_pos + 2:]

            if array_len == 0:
                continue

            parts = []
            for i in range(array_len):
                if not ensure_buffer():
                    return None

                t = buffer[0:1]
                if t == RESPType.String:
                    # String
                    v = read_to_crlf()
                    parts.append((RESPType.String, v))
                elif t == RESPType.Error:
                    # Error
                    v = read_to_crlf()
                    parts.append((RESPType.Error, v))
                elif t == RESPType.Integer:
                    # Integer
                    v = int(read_to_crlf())
                    parts.append((RESPType.Integer, v))
                elif t == RESPType.BulkString:
                    # Bulk string
                    str_len = int(read_to_crlf())
                    ensure_buffer(str_len + 2)
                    v = buffer[:str_len]
                    buffer = buffer[str_len + 2:]
                    parts.append((RESPType.String, v))
                elif t == RESPType.Array:
                    # Array
                    raise Exception('Cannot read arrays')
                else:
                    raise Exception('Unexpected client type: t=%s, buffer=%s' % (t, buffer))

            assert len(parts) == array_len

            assert (t == RESPType.String for t, _ in parts), \
                'All client command parts must be strings'

            cmd = [d for _, d in parts]

            yield cmd

    def __repr__(self):
        return f'<Connection({self.socket}, {self.address})>'


class RedisServer(object):
    def __init__(self, command_processor: CommandProcessor):
        self.command_processor = command_processor

    def connection_handler(self, socket: _socket, address):
        connection = Connection(socket, address)
        logger.info('New connection: connection=%s', connection)

        try:
            for cmd in connection.command_stream():
                try:
                    result = self.command_processor.process_command(cmd)
                    connection.write(result)
                except RedisError as e:
                    connection.write(e)
        except Exception:
            logger.exception('Unhandled exception for connection: connection=%s', connection)
        finally:
            logger.info('Closing connection: connection=%s', connection)
            try:
                socket.close()
            except Exception:
                logger.exception(
                    'Exception closing connection, ignoring: connection=%s', connection,
                )
