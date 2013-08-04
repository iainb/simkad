from client import Rpc_Client, Kad_Client
from network.simulate import Simulate

from gevent import pool, queue

from gevent.pool import Pool

def spawn_clients(pool, network, n):
    last = spawn_client(pool, network)
    for i in xrange(0,n-1):
        last = spawn_client(pool, network, last)
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
    return node

p = pool.Pool(6000)
network = Simulate(False)
p.spawn(spawn_clients, p, network, 100)
p.join()
