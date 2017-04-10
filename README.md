# dredispy - Dumb Redis in Python

This is a redis-server clone, in Python. It's not meant to be used except for learning by doing. Lots of dumb stuff in here.

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
