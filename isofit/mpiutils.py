import numpy as np

from geometry import Geometry

class Tags:
    def __init__(self, kinds):
        for i, kind in enumerate(kinds):
            setattr(self, kind, i)

def send_io(row, col, meas, geom, configs, comm, worker, tags):
    comm.send(row, dest=worker, tag=tags.ROW)
    comm.send(col, dest=worker, tag=tags.COL)

    if meas is None:
        comm.send(None, dest=worker, tag=tags.MEAS)
    else:
        comm.send(len(meas), dest=worker, tag=tags.MEAS)
        comm.Send(meas, dest=worker, tag=tags.MEAS)

    obs = None if geom.obs is None else list(geom.obs)
    glt = None if geom.glt is None else list(geom.glt)
    loc = None if geom.loc is None else list(geom.loc)
    comm.send([obs, glt, loc], dest=worker, tag=tags.GEOM)

    comm.send(configs, dest=worker, tag=tags.CONF)

def recv_io(comm, tags, status):
    received = 0
    while received < 5:
        data = comm.recv(source=0, status=status)
        tag = status.Get_tag()

        if tag == tags.ROW:
            row = data
            received = received + 1
        if tag == tags.COL:
            col = data
            received = received + 1

        if tag == tags.MEAS:
            if data is None:
                meas = None
            else:
                meas = np.empty(data, dtype=np.float32)
                comm.Recv(meas, source=0, tag=tag)

            received = received + 1

        if tag == tags.GEOM:
            obs, glt, loc = data
            geom = Geometry(obs=obs, glt=glt, loc=loc)
            received = received + 1

        if tag == tags.CONF:
            configs = data
            received = received + 1

    return row, col, meas, geom, configs
