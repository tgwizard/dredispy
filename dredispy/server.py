import logging

from socket import socket as _socket

from dredispy.commands import process_command, RedisError, RedisData

logger = logging.getLogger(__name__)


class RESPType(object):
    String = b'+'
    Error = b'-'
    Integer = b':'
    BulkString = b'$'
    Array = b'*'


def connection_handler(socket: _socket, address):
    logger.info('New connection: socket=%s, address=%s', socket, address)

    try:
        for cmd_parts in command_reader(socket):
            logger.info('Received data: cmd_parts=%s', cmd_parts)
            assert (t == RESPType.String for t, _ in cmd_parts), \
                'All client command parts must be strings'

            cmd = [d for _, d in cmd_parts]
            try:
                result = process_command(cmd)
                logger.info('Command response: r=%s', result)
                write(socket, result)
            except RedisError as e:
                logger.info('Redis error: e=%s', e.to_resp())
                write(socket, e)

    except Exception:
        logger.exception('Exception using socket: socket=%s, address=%s', socket, address)
    finally:
        logger.info('Closing connection: socket=%s, address=%s', socket, address)
        try:
            socket.close()
        except Exception:
            logger.exception('Exception closing socket: socket=%s, address=%s', socket, address)


def command_reader(socket):
    buffer = b''

    def read():
        nonlocal buffer
        chunk: bytes = socket.recv(2048)
        if not chunk:
            return False

        logger.debug('Read chunk: chunk=%s', chunk)

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
        logger.debug('Client command array: length=%s', array_len)
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
                logger.debug('Found string: v=%s', v)
                parts.append((RESPType.String, v))
            elif t == RESPType.Error:
                # Error
                v = read_to_crlf()
                logger.debug('Found error: v=%s', v)
                parts.append((RESPType.Error, v))
            elif t == RESPType.Integer:
                # Integer
                v = int(read_to_crlf())
                logger.debug('Found integer: v=%s', v)
                parts.append((RESPType.Integer, v))
            elif t == RESPType.BulkString:
                # Bulk string
                str_len = int(read_to_crlf())
                logger.debug('Will read bulk string: len=%s', str_len)
                ensure_buffer(str_len + 2)
                v = buffer[:str_len]
                buffer = buffer[str_len+2:]
                logger.debug('Found bulk string: v=%s', v)
                parts.append((RESPType.String, v))
            elif t == RESPType.Array:
                # Array
                raise Exception('Cannot read arrays')
            else:
                raise Exception('Unexpected client type: t=%s, buffer=%s' % (t, buffer))

        assert len(parts) == array_len
        yield parts


def write(socket, data: RedisData):
    b = data.to_resp()
    socket.sendall(b)
