# dredispy - Dumb Redis in Python

This is a redis-server clone, in Python. It's not meant to be used except for learning by doing. Lots of dumb stuff in here.

Supports:

 - Multiple DBs.
 - Storage disk persistence.
 - Key expiration.
 - Commands: `PING`, `INFO`, `SELECT`, `KEYS`, `GET`, `MGET`, `SET`, `MSET`, `DEL`.
 - The full PubSub protocol.

Documentation:

 - https://redis.io/topics/protocol
 - https://redis.io/commands


## Develop

Requires Python 3.6

```bash
make setup
make test
```

## Run

```bash
make serve
```

Connect to the server with redis-cli:

```bash
redis-cli -p 9000
```

Or with telnet:

```bash
↳ $ telnet
telnet> toggle crlf
Will send carriage returns as telnet <CR><LF>.
telnet> open 127.0.0.1 9000
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
*3
$3
set
$3
foo
$2
xy
+OK
*2
$4
keys
$1
*
*1
+foo
```
