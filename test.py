from eventlet import greenpool
import eventlet
from client import Client
from network.simulate import Simulate
from routing import Node

import random

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
    c = Client(network)
    pool.spawn(c.main, random.randint(1,10))
    return c.return_node()


pool = greenpool.GreenPool(6000)
network = Simulate(False)
pool.spawn(spawn_clients, pool, network, 10)
pool.waitall()
