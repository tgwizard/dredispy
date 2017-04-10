import logging
import sys

from gevent.server import StreamServer

from dredispy.server import connection_handler


def main():
    configure_logging()

    server = StreamServer(('127.0.0.1', 9000), connection_handler)
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
