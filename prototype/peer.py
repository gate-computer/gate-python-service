import logging
from collections import defaultdict
from functools import partial
from struct import pack, unpack

from gevent.queue import Queue

log = logging.getLogger(__name__)

ERROR_GROUP_NOT_FOUND = 1
ERROR_PEER_NOT_FOUND = 2
ERROR_SINGULARITY = 3
ERROR_ALREADY_CONNECTING = 4
ERROR_ALREADY_CONNECTED = 5

proc_instances = {}
procpair_conns = {}


def procpair(*procs):
    return tuple(sorted(procs))


def register_peer_group_instance(proc, g, name):
    try:
        inst = proc_instances[proc]
    except KeyError:
        return False

    inst.register_group(g, name)
    return True


class Conn:
    log = logging.getLogger(__name__ + ".Conn")

    def __init__(self):
        self.callbacks = {}
        self.closing = defaultdict(int)

    def already_connected(self):
        return len(self.callbacks) == 2

    def already_connecting(self, peer_name):
        return peer_name in self.callbacks

    def connect(self, peer_name, my_func):
        other = None
        if self.callbacks:
            other, = self.callbacks.items()

        assert peer_name not in self.callbacks
        self.callbacks[peer_name] = my_func

        if other:
            my_name, peer_func = other
            my_func(False, peer_name=peer_name)
            peer_func(False, peer_name=my_name)

    def transfer(self, my_name, data, note):
        if len(data) == 0:
            self.closing[my_name] |= 0b0001
            self.closing[self.peer_name(my_name)] |= 0b0100

        peer_func = self.callbacks[my_name]
        peer_func(self.closed(self.peer_name(my_name)), data=data, note=note)

    def flow(self, my_name, increment):
        if increment == 0:
            self.closing[my_name] |= 0b0010
            self.closing[self.peer_name(my_name)] |= 0b1000

        peer_func = self.callbacks[my_name]
        peer_func(self.closed(self.peer_name(my_name)), increment=increment)

    def closed(self, my_name):
        return self.closing.get(my_name, 0) == 0b1111

    def peer_name(self, my_name):
        for k, v in self.callbacks.items():
            if k != my_name:
                return k

        assert False, self.callbacks


class Instance:
    log = logging.getLogger(__name__ + ".Instance")

    def __init__(self, config):
        self.proc = config.proc
        self.queue = Queue()
        self.group = None
        self.name = None
        self.stream_count = 0
        self.stream_conns = {}

    def __str__(self):
        return "proc={}".format(self.proc)

    def restore(self, snapshot):
        self.log.warn("not implemented")
        return None

    def ready(self):
        proc_instances[self.proc] = self

    def register_group(self, g, name):
        assert not self.group
        self.group = g
        self.name = name

        self.log.debug("%s: registered in %s as %s", self, g, name)

    def generate_packets(self):
        for p in self.queue:
            yield p

    def handle_packet(self, packet):
        domain = packet[6]
        if domain == 0:
            self.handle_call(packet)
        elif domain == 2:
            self.handle_flow(packet)
        elif domain == 3:
            self.handle_data(packet)

    def handle_call(self, packet):
        error = ERROR_GROUP_NOT_FOUND
        group_name, peer_name = packet[8:].decode().split(":", 1)
        if self.group and self.group.name == group_name:
            error = ERROR_SINGULARITY
            if self.name != peer_name:
                error = ERROR_PEER_NOT_FOUND
                peer_proc = self.group.peer_proc(peer_name)
                if peer_proc:
                    assert peer_proc != self.proc
                    pair = procpair(self.proc, peer_proc)
                    try:
                        conn = procpair_conns[pair]
                    except KeyError:
                        peer = proc_instances[peer_proc]
                        peer.handle_conn(group_name, -1, False,
                                         peer_name=self.name)
                        conn = procpair_conns[pair] = Conn()

                    error = ERROR_ALREADY_CONNECTED
                    if not conn.already_connected():
                        error = ERROR_ALREADY_CONNECTING
                        if not conn.already_connecting(peer_name):
                            stream = self.stream_count
                            self.stream_count += 1
                            self.stream_conns[stream] = conn
                            conn.connect(peer_name, partial(
                                self.handle_conn, group_name, stream))
                            error = 0

        self.log.debug("%s: error=%d", self, error)

        p = bytearray(8)
        p += pack("<hH", error, 0)
        self.queue.put(p)

    def handle_flow(self, packet):
        packet = packet[8:]
        while len(packet) >= 8:
            stream, increment = unpack("<ii", packet[:8])
            packet = packet[8:]

            conn = self.stream_conns[stream]
            conn.flow(self.name, increment)
            if conn.closed(self.name):
                del self.stream_conns[stream]

    def handle_data(self, packet):
        stream, note = unpack("<ii", packet[8:16])
        data = packet[16:]

        conn = self.stream_conns[stream]
        conn.transfer(self.name, data, note)
        if conn.closed(self.name):
            del self.stream_conns[stream]

    def handle_conn(self, group_name, stream, closed, *, peer_name=None, data=None, note=None, increment=None):
        if peer_name is not None:
            p = bytearray(8)
            p[6] = 1  # info domain
            p += pack("<i", stream)
            p += group_name.encode()
            p += ":".encode()
            p += peer_name.encode()
            self.queue.put(p)

        if data is not None:
            p = bytearray(8)
            p[6] = 3  # data domain
            p += pack("<ii", stream, note)
            p += data
            self.queue.put(p)

        if increment is not None:
            p = bytearray(8)
            p[6] = 2  # flow domain
            p += pack("<ii", stream, increment)
            self.queue.put(p)

        if closed:
            del self.stream_conns[stream]

    def stop(self):
        if self.group:
            self.group.deregister(self.name)
        del proc_instances[self.proc]
        self.queue.put(StopIteration)

    def shutdown(self):
        self.stop()

    def suspend(self):
        self.stop()

    def snapshot(self, outgoing, incoming):
        self.log.warn("not implemented")
        return None


service_instance_types = {
    "peer": Instance,
}
