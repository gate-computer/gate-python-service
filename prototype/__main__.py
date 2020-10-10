import argparse
import logging
from concurrent import futures
from importlib import import_module
from struct import pack, unpack
from time import sleep

import grpc
from gate.service.grpc.api import service_pb2 as api
from gate.service.grpc.api import service_pb2_grpc as api_grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.wrappers_pb2 import BytesValue

default_addr = "localhost:12345"
service_instance_types = {}


def main():
    parser = argparse.ArgumentParser("prototype")
    parser.add_argument("-l", metavar="ADDR", default=default_addr,
                        help="bind address (default: {})".format(default_addr))
    parser.add_argument("module", nargs="+", help="prototype to import")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(name)s: %(message)s", level=logging.DEBUG)

    for name in args.module:
        module = import_module(name, "prototype")
        service_instance_types.update(module.service_instance_types)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    api_grpc.add_RootServicer_to_server(RootServicer(), server)
    api_grpc.add_ServiceServicer_to_server(ServiceServicer(), server)
    api_grpc.add_InstanceServicer_to_server(InstanceServicer(), server)
    server.add_insecure_port(args.l)
    server.start()

    try:
        while True:
            sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)


revision = "0-python"
log = logging.getLogger("api")


class RootServicer(api_grpc.RootServicer):

    def Init(self, req, ctx):
        log.debug("Root.Init")
        names = service_instance_types.keys()
        services = [api.ServiceInfo(name=n, revision=revision) for n in names]
        return api.InitResponse(services=services)


instance_count = 0
id_instances = {}


def add_instance(inst):
    global instance_count
    instance_count += 1
    n = instance_count
    id_instances[n] = inst
    return pack("Q", n)


def instance_id(id):
    n, = unpack("Q", id)
    return n


class ServiceServicer(api_grpc.ServiceServicer):

    def CreateInstance(self, req, ctx):
        log.debug("Service.%s.CreateInstance", req.name)
        inst = service_instance_types[req.name](req.config)
        inst.ready()
        id = add_instance(inst)
        log.debug("Service.%s.CreateInstance: %s", req.name, instance_id(id))
        return api.CreateInstanceResponse(id=id)

    def RestoreInstance(self, req, ctx):
        log.debug("Service.%s.RestoreInstance", req.name)
        inst = service_instance_types[req.name](req.config)
        error = inst.restore(req.snapshot)
        if error is not None:
            return api.RestoreInstanceResponse(error=error)
        inst.ready()
        id = add_instance(inst)
        log.debug("Service.%s.RestoreInstance: %s", req.name, instance_id(id))
        return api.RestoreInstanceResponse(id=id)


def get_instance(id):
    return id_instances[instance_id(id)]


def pop_instance(id):
    n, = unpack("Q", id)
    return id_instances.pop(n)


class InstanceServicer(api_grpc.InstanceServicer):

    def Receive(self, req, ctx):
        log.debug("Instance.%s.Receive", instance_id(req.id))
        for packet in get_instance(req.id).generate_packets():
            if isinstance(packet, bytearray):
                packet = bytes(packet)
            yield BytesValue(value=packet)

    def Handle(self, req, ctx):
        log.debug("Instance.%s.Handle", instance_id(req.id))
        get_instance(req.id).handle_packet(req.data)
        return Empty()

    def Shutdown(self, req, ctx):
        log.debug("Instance.%s.Shutdown", instance_id(req.id))
        pop_instance(req.id).shutdown()
        return Empty()

    def Suspend(self, req, ctx):
        log.debug("Instance.%s.Suspend", instance_id(req.id))
        get_instance(req.id).suspend()
        return Empty()

    def Snapshot(self, req, ctx):
        log.debug("Instance.%s.Snapshot", instance_id(req.id))
        snapshot = pop_instance(req.id).snapshot(req.outgoing, req.incoming)
        return BytesValue(value=snapshot)


if __name__ == "__main__":
    main()
