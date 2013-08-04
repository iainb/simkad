from client import Rpc_Client, Kad_Client
from network.simulate import Simulate

from gevent import pool, queue

import gevent


def store_value(client, key, value):
    client.store_value(key, value)

def spawn_clients(pool, network, n):
    last, kad_client = spawn_client(pool, network)
    for i in xrange(0,n-1):
        last, kad_client = spawn_client(pool, network, last)

    gevent.spawn_later(5, store_value, kad_client, 'hello', 'world')

    print "test: spawned %s nodes" % n

def spawn_client(pool, network, initial_node=None):
    if initial_node is None:
        nodes = []
    else:
        nodes = [initial_node]

    rpc_chan  = queue.Queue()
    rpc_client = Rpc_Client(network, rpc_chan)
    node = rpc_client.return_node()
    kad_client = Kad_Client(pool, rpc_client, rpc_chan)
    kad_client.join_network(nodes)
    return node, kad_client

p = pool.Pool(6000)
network = Simulate(False)
p.spawn(spawn_clients, p, network, 100)
p.join()
