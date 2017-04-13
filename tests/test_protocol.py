import pytest

from dredispy.protocol import (
    RedisError,
    WrongNumberOfArgumentsError,
    RedisString,
    RedisBulkString,
    RedisNullBulkString,
    RedisInteger,
    RedisArray,
    RedisMultipleResponses,
)


@pytest.mark.parametrize('msg,kind,expected_resp', [
    (None, None, b'-ERR Something went wrong\r\n'),
    ('oops', None, b'-ERR oops\r\n'),
    (None, 'MYERR', b'-MYERR Something went wrong\r\n'),
    ('bad stuff happened', 'MYERR', b'-MYERR bad stuff happened\r\n'),
])
def test_redis_error(msg, kind, expected_resp):
    e = RedisError(msg, kind)
    assert e.to_resp() == expected_resp, e.to_resp()


def test_wrong_number_of_arguments_error():
    e = WrongNumberOfArgumentsError(b'dummy')
    assert e.to_resp() == b'-ERR wrong number of arguments for \'dummy\' command\r\n'


@pytest.mark.parametrize('data,expected_resp', [
    (b'', b'+\r\n'),
    ('foo', b'+foo\r\n'),
    (b'asdf xyz', b'+asdf xyz\r\n'),
])
def test_redis_string(data, expected_resp):
    r = RedisString(data)
    assert r.to_resp() == expected_resp, r.to_resp()


@pytest.mark.parametrize('data,expected_resp', [
    (b'', b'$0\r\n\r\n'),
    ('foo', b'$3\r\nfoo\r\n'),
    (b'asdf xyz\r\n', b'$10\r\nasdf xyz\r\n\r\n'),
])
def test_redis_bulk_string(data, expected_resp):
    r = RedisBulkString(data)
    assert r.to_resp() == expected_resp, r.to_resp()


def test_redis_null_bulk_string():
    r = RedisNullBulkString()
    assert r.to_resp() == b'$-1\r\n'


@pytest.mark.parametrize('data,expected_resp', [
    (0, b':0\r\n'),
    (3, b':3\r\n'),
    (-3, b':-3\r\n'),
    (2**32, b':4294967296\r\n'),
])
def test_redis_integer(data, expected_resp):
    r = RedisInteger(data)
    assert r.to_resp() == expected_resp, r.to_resp()


@pytest.mark.parametrize('items,expected_resp', [
    ([], b'*0\r\n'),
    ([RedisInteger(5)], b'*1\r\n:5\r\n'),
    ([RedisString(b'foo')], b'*1\r\n+foo\r\n'),
    ([RedisString(b'foo'), RedisBulkString(b'bar')], b'*2\r\n+foo\r\n$3\r\nbar\r\n'),
])
def test_redis_array(items, expected_resp):
    r = RedisArray(items)
    assert r.to_resp() == expected_resp, r.to_resp()


@pytest.mark.parametrize('items,expected_resp', [
    ([], b''),
    ([RedisInteger(5)], b':5\r\n'),
    ([RedisString(b'foo')], b'+foo\r\n'),
    ([RedisString(b'foo'), RedisBulkString(b'bar')], b'+foo\r\n$3\r\nbar\r\n'),
])
def test_redis_multiple_responses(items, expected_resp):
    r = RedisMultipleResponses(items)
    assert r.to_resp() == expected_resp, r.to_resp()
