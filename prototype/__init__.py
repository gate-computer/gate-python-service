def __init():
    import gevent.monkey
    gevent.monkey.patch_all()

    import grpc.experimental.gevent as grpc_gevent
    grpc_gevent.init_gevent()


__init()
