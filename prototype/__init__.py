import logging
from base64 import urlsafe_b64encode
from struct import pack, unpack
from uuid import UUID

from gate.grpc.pb import service_pb2 as api
from gate.grpc.pb import service_pb2_grpc as api_grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.wrappers_pb2 import BytesValue

revision = "0-python"
service_instance_types = {}


class RootServicer(api_grpc.RootServicer):
    log = logging.getLogger(__package__ + ".Root")

    def Init(self, req, ctx):
        names = sorted(service_instance_types.keys())
        self.log.debug("%s", " ".join(names))
        services = [api.Service(name=n, revision=revision) for n in names]
        return api.InitResponse(services=services)


class InstanceConfig:

    def __init__(self, api):
        self.max_send_size = api.max_send_size

        self.proc = None
        if api.process_key:
            self.proc = urlsafe_b64encode(api.process_key).decode().rstrip("=")

        self.pri = api.principal_id

        self.uuid = None
        if api.instance_uuid:
            self.uuid = str(UUID(bytes=api.instance_uuid))

    def __str__(self):
        props = []

        if self.proc:
            props.append("proc={}".format(self.proc))

        if self.pri:
            props.append("pri={}".format(self.pri))

        if self.uuid:
            props.append("uuid={}".format(self.uuid))

        return " ".join(props)


instance_count = 0
id_instances = {}
id_services = {}


def add_instance(service, inst):
    global instance_count
    instance_count += 1
    n = instance_count
    id_instances[n] = inst
    id_services[n] = service
    return pack("Q", n)


def instance_id(id):
    n, = unpack("Q", id)
    return n


def get_instance(id):
    return id_instances[instance_id(id)]


def pop_instance(id):
    n, = unpack("Q", id)
    del id_services[n]
    return id_instances.pop(n)


def fmt_instance(id):
    n = instance_id(id)
    return "{} #{}".format(id_services[n], n)


def fmt_packet(p):
    domain = p[6]
    msg = ("call", "info", "flow", "data")[domain]

    index = p[7]
    if index:
        msg += " index={}".format(index)

    if domain in (0, 1):
        msg += " content={}".format(len(p) - 8)
    elif domain == 2:
        p = p[8:]
        while len(p) >= 8:
            stream, increment = unpack("<ii", p[:8])
            msg += " id={} n={}".format(stream, increment)
            p = p[8:]
            if len(p) >= 8:
                msg += ","
        if len(p):
            msg += ", trailing %r" % p
    elif domain == 3:
        stream, note = unpack("<ii", p[8:16])
        msg += " id={}".format(stream)

        if note:
            msg += " note={}".format(note)

        msg += " data={}".format(len(p) - 16)

    return msg


class InstanceServicer(api_grpc.InstanceServicer):
    log = logging.getLogger(__package__ + ".Instance")

    def Create(self, req, ctx):
        config = InstanceConfig(req.config)
        if req.snapshot:
            self.log.debug("%s < %s snapshot=%d",
                           req.service_name, config, len(req.snapshot))
        else:
            self.log.debug("%s < %s", req.service_name, config)

        try:
            inst = service_instance_types[req.service_name](config)
            if req.snapshot:
                error = inst.restore(req.snapshot)
                if error is not None:
                    return api.CreateResponse(restoration_error=error)

            inst.ready()
            id = add_instance(req.service_name, inst)
            return api.CreateResponse(id=id)
        finally:
            self.log.debug("%s > #%d", req.service_name, instance_id(id))

    def Receive(self, req, ctx):
        self.log.debug("%s <", fmt_instance(req.id))

        try:
            for p in get_instance(req.id).generate_packets():
                self.log.debug("%s %s", fmt_instance(req.id), fmt_packet(p))
                yield BytesValue(value=bytes(p) if isinstance(p, bytearray) else p)
        finally:
            self.log.debug("%s >", fmt_instance(req.id))

    def Handle(self, req, ctx):
        self.log.debug("%s < %s", fmt_instance(req.id), fmt_packet(req.data))

        try:
            get_instance(req.id).handle_packet(req.data)
            return Empty()
        finally:
            self.log.debug("%s >", fmt_instance(req.id))

    def Shutdown(self, req, ctx):
        desc = fmt_instance(req.id)
        self.log.debug("%s <", desc)

        try:
            pop_instance(req.id).shutdown()
            return Empty()
        finally:
            self.log.debug("%s >", desc)

    def Suspend(self, req, ctx):
        desc = fmt_instance(req.id)
        self.log.debug("%s <", desc)

        try:
            get_instance(req.id).suspend()
            return Empty()
        finally:
            self.log.debug("%s >", desc)

    def Snapshot(self, req, ctx):
        desc = fmt_instance(req.id)
        self.log.debug("%s <", desc)

        try:
            inst = pop_instance(req.id)
            snapshot = inst.snapshot(req.outgoing, req.incoming)
            return BytesValue(value=snapshot)
        finally:
            self.log.debug("%s >", desc)
