import argparse
import logging
from base64 import urlsafe_b64encode
from concurrent import futures
from importlib import import_module
from struct import pack, unpack
from time import sleep
from uuid import UUID

import grpc
from gate.service.grpc.api import service_pb2 as api
from gate.service.grpc.api import service_pb2_grpc as api_grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.wrappers_pb2 import BytesValue

default_addr = "localhost:12345"
service_instance_types = {}


def main():
    parser = argparse.ArgumentParser(__package__)
    parser.add_argument("-l", metavar="ADDR", default=default_addr,
                        help="bind address (default: {})".format(default_addr))
    parser.add_argument("module", nargs="+", help="prototype to import")
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s %(name)s.%(funcName)s: %(message)s",
                        level=logging.DEBUG)

    for name in args.module:
        module = import_module(name, __package__)
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


class RootServicer(api_grpc.RootServicer):
    log = logging.getLogger(__package__ + ".Root")

    def Init(self, req, ctx):
        names = sorted(service_instance_types.keys())
        self.log.debug("%s", " ".join(names))
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


def fmt_instance_config(config):
    props = []

    if config.process_key:
        key = urlsafe_b64encode(config.process_key).decode().rstrip("=")
        props.append("process={}".format(key))

    if config.principal_id:
        props.append("principal={}".format(config.principal_id))

    if config.instance_uuid:
        props.append("instance={}".format(UUID(bytes=config.instance_uuid)))

    return " ".join(props)


class ServiceServicer(api_grpc.ServiceServicer):
    log = logging.getLogger(__package__ + ".Service")

    def CreateInstance(self, req, ctx):
        self.log.debug("%s < %s", req.name, fmt_instance_config(req.config))
        try:
            inst = service_instance_types[req.name](req.config)
            inst.ready()
            id = add_instance(inst)
            return api.CreateInstanceResponse(id=id)
        finally:
            self.log.debug("%s > #%d", req.name, instance_id(id))

    def RestoreInstance(self, req, ctx):
        self.log.debug("%s < %s", req.name, fmt_instance_config(req.config))
        try:
            inst = service_instance_types[req.name](req.config)
            error = inst.restore(req.snapshot)
            if error is not None:
                return api.RestoreInstanceResponse(error=error)
            inst.ready()
            id = add_instance(inst)
            return api.RestoreInstanceResponse(id=id)
        finally:
            self.log.debug("%s > #%d", req.name, instance_id(id))


def get_instance(id):
    return id_instances[instance_id(id)]


def pop_instance(id):
    n, = unpack("Q", id)
    return id_instances.pop(n)


def fmt_packet(p):
    domain = p[6]
    msg = "len={} domain={}".format(len(p), domain)

    index = p[7]
    if index:
        msg += " index={}".format(index)

    if domain == 2:
        if len(p) == 16:
            stream, increment = unpack("<ii", p[8:])
            msg += " stream={} increment={}".format(stream, increment)
        else:
            msg += " ..."

    if domain == 3:
        stream, note = unpack("<ii", p[8:16])
        msg += " stream={}".format(stream)

        if note:
            msg += " note={}".format(note)

    return msg


class InstanceServicer(api_grpc.InstanceServicer):
    log = logging.getLogger(__package__ + ".Instance")

    def Receive(self, req, ctx):
        self.log.debug("#%d <", instance_id(req.id))
        try:
            for p in get_instance(req.id).generate_packets():
                self.log.debug("#%d %s", instance_id(req.id), fmt_packet(p))
                yield BytesValue(value=bytes(p) if isinstance(p, bytearray) else p)
        finally:
            self.log.debug("#%d >", instance_id(req.id))

    def Handle(self, req, ctx):
        self.log.debug("#%d < %s", instance_id(req.id), fmt_packet(req.data))
        try:
            get_instance(req.id).handle_packet(req.data)
            return Empty()
        finally:
            self.log.debug("#%d >", instance_id(req.id))

    def Shutdown(self, req, ctx):
        self.log.debug("#%d <", instance_id(req.id))
        try:
            pop_instance(req.id).shutdown()
            return Empty()
        finally:
            self.log.debug("#%d >", instance_id(req.id))

    def Suspend(self, req, ctx):
        self.log.debug("#%d <", instance_id(req.id))
        try:
            get_instance(req.id).suspend()
            return Empty()
        finally:
            self.log.debug("#%d >", instance_id(req.id))

    def Snapshot(self, req, ctx):
        self.log.debug("#%d <", instance_id(req.id))
        try:
            inst = pop_instance(req.id)
            snapshot = inst.snapshot(req.outgoing, req.incoming)
            return BytesValue(value=snapshot)
        finally:
            self.log.debug("#%d >", instance_id(req.id))


if __name__ == "__main__":
    main()
