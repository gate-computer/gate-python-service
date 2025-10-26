import argparse
import logging
from concurrent import futures
from importlib import import_module
from time import sleep

import grpc
from gate.grpc.pb import service_pb2_grpc as api_grpc

from . import InstanceServicer, RootServicer, service_instance_types

default_addr = "localhost:12345"

log = logging.getLogger("prototype")


def main():
    parser = argparse.ArgumentParser(__package__)
    parser.add_argument("-l", metavar="ADDR", default=default_addr,
                        help="bind address (default: {})".format(default_addr))
    parser.add_argument("module", nargs="+", help="service to import")
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s %(name)s.%(funcName)s: %(message)s",
                        level=logging.DEBUG)

    for name in args.module:
        module = import_module(name, __package__)
        service_instance_types.update(module.service_instance_types)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_grpc.add_RootServicer_to_server(RootServicer(), server)
    api_grpc.add_InstanceServicer_to_server(InstanceServicer(), server)
    server.add_insecure_port(args.l)
    server.start()

    try:
        while True:
            sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
