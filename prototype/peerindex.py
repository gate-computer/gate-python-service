import logging
from base64 import urlsafe_b64encode
from struct import pack, unpack
from uuid import UUID

from gevent.queue import Queue

from .peer import register_peer_group_instance

ERROR_NOT_REGISTERED = 1


class Group:
    name = "index/principal"

    def __init__(self):
        self.name_procs = {}

    def __str__(self):
        return self.name

    def register(self, proc, name):
        self.name_procs[name] = proc
        register_peer_group_instance(proc, self, name)

    def deregister(self, name):
        del self.name_procs[name]

    def peer_proc(self, peer_name):
        return self.name_procs.get(peer_name)

    def peer_names(self, my_name):
        return sorted(x for x in self.name_procs if x != my_name)


pri_groups = {}


class Instance:
    log = logging.getLogger(__name__ + ".Instance")

    def __init__(self, config):
        assert config.pri
        assert config.uuid

        self.proc = config.proc
        self.pri = config.pri
        self.group = None
        self.name = config.uuid
        self.queue = Queue()

    def __str__(self):
        return "proc={}".format(self.proc)

    def restore(self, snapshot):
        self.log.warn("not implemented")
        return None

    def ready(self):
        pass

    def generate_packets(self):
        for p in self.queue:
            yield p

    def handle_packet(self, packet):
        domain = packet[6]
        if domain == 0:
            self.handle_call()
        elif domain == 1:
            self.handle_info()
        elif domain == 2:
            raise Exception("unexpected flow packet")
        elif domain == 3:
            raise Exception("unexpected data packet")

    def handle_call(self):
        error = ERROR_NOT_REGISTERED
        names = []
        try:
            group = pri_groups[self.pri]
        except KeyError:
            pass
        else:
            names = group.peer_names(self.name)
            error = 0

        self.log.debug("%s: error=%d", self, error)

        p = bytearray(8)
        p += pack("<hH", error, len(names))

        for name in names:
            binary = name.encode()
            p += pack("B", len(binary))
            p += binary

        self.queue.put(p)

    def handle_info(self):
        try:
            self.group = pri_groups[self.pri]
        except KeyError:
            self.group = pri_groups[self.pri] = Group()

        self.group.register(self.proc, self.name)

    def stop(self):
        self.queue.put(StopIteration)

    def shutdown(self):
        self.stop()

    def suspend(self):
        self.stop()

    def snapshot(self, outgoing, incoming):
        self.log.warn("not implemented")
        return None


service_instance_types = {
    "peerindex/principal": Instance,
}
