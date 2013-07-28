from client import Client
from network.simulate import Simulate

import random

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
    c = Client(network, nodes=nodes)
    pool.spawn(c.main, random.randint(1,10))
    return c.return_node()


pool = Pool(size=6000)
network = Simulate(False)
pool.spawn(spawn_clients, pool, network, 10)
pool.join()
