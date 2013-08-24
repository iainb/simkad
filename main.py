from client import Rpc_Client, Kad_Client
from network import simulate
from chan import SelectChan

from gevent import pool


def spawn_client(_pool=None, network=None, node=None):
    if not _pool:
        _pool = pool.Pool(1000)

    if not network:
        network = simulate.Simulate()

    chan = SelectChan()
    rpc_client = Rpc_Client(network, chan, node=node)

    if not node:
        node = rpc_client.return_node()

    kad_client = Kad_Client(_pool, rpc_client, chan.fetch_chan('int'))

    return _pool, network, node, rpc_client, kad_client

j = spawn_client()
j[4].join_network([])
j[0].join()




