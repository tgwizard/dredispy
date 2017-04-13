import logging
import sys

import gevent
from gevent.server import StreamServer

from dredispy.command import CommandProcessor, Storage
from dredispy.server import RedisServer


logger = logging.getLogger(__name__)


def main():
    configure_logging()

    storage = Storage.from_snapshot('./tmp/data')
    command_processor = CommandProcessor(storage)
    server = RedisServer(command_processor)

    gevent.spawn(periodic_task, storage=storage)

    address = '127.0.0.1'
    port = 9000

    server = StreamServer((address, port), server.connection_handler)
    logger.info('Listening on %s:%s', address, port)
    server.serve_forever()


def periodic_task(storage: Storage):
    while True:
        gevent.sleep(5)
        storage.process_periodic_task()


def configure_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


if __name__ == '__main__':
    main()
