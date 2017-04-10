import logging
import sys

import gevent
from gevent.server import StreamServer

from dredispy.commands import periodic_handler
from dredispy.server import connection_handler


logger = logging.getLogger(__name__)


def main():
    configure_logging()

    gevent.spawn(periodic_handler)

    address = '127.0.0.1'
    port = 9000

    server = StreamServer((address, port), connection_handler)
    logger.info('Listening on %s:%s', address, port)
    server.serve_forever()


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
