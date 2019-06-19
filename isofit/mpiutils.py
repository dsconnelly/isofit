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

    comm.send(obs, dest=worker, tag=tags.GEOM)
    comm.send(glt, dest=worker, tag=tags.GEOM)
    comm.send(loc, dest=worker, tag=tags.GEOM)

    surf, rt, ins = configs
    for i, model in enumerate(configs):
        tag = (tags.SURF, tags.RT, tags.INS)[i]

        for k, v in model.items():
            if v is not None:
                comm.send(f'{k} {len(v)}', dest=worker, tag=tag)
                comm.Send(v, dest=worker, tag=tag)

        comm.send(-1, dest=worker, tag=tag)

def recv_io(comm, tags, status):
    received = 0

    geom = []
    surf = {
        'prior_means' : None,
        'prior_variances' : None,
        'reflectances' : None
    }
    rt = {
        'prior_means' : None,
        'prior_variances' : None
    }
    ins = {
        'prior_means' : None,
        'prior_variances' : None
    }

    while received < 7:
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
            geom.append(data)
            if len(geom) == 3:
                received = received + 1

        if tag in [tags.SURF, tags.RT, tags.INS]:
            if data == -1:
                received = received + 1
            else:
                model = {tags.SURF : surf, tags.RT : rt, tags.INS : ins}[tag]
                name, buff_size = data.split()
                buff_size = int(buff_size)

                model[name] = np.empty(buff_size, dtype=np.float32)
                comm.Recv(model[name], source=0, tag=tag)

    obs, glt, loc = geom
    geom = Geometry(obs=obs, glt=glt, loc=loc)
    configs = (surf, rt, ins)

    return row, col, meas, geom, configs
