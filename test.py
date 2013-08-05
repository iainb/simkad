from client import Rpc_Client, Kad_Client
from network.simulate import Simulate

from gevent import pool, queue

import gevent
import random

def client_actions(client, nodes):
    count =  0
    stored = {}
    client._join_network(nodes)
    while True:
        gevent.sleep(5)
        if random.random() < 0.25:
            key = '%s_%s' % (client.node.id, count)
            value = str(count)
            client._store_value(key, value)
            stored[key] = value
            count += 1
        elif random.random() < 0.25 and stored:
            key = random.choice(stored.keys())
            print "looking up %s, expecting %s" % (key, stored[key])
            value = client._fetch_value(key)
            if value == stored[key]:
                print 'ok good job'
            else:
                print 'failed, expected %s, found %s' % (stored[key], value)


def spawn_clients(pool, network, n):
    last, kad_client = spawn_client(pool, network)
    for i in xrange(0,n-1):
        last, kad_client = spawn_client(pool, network, last)

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
    pool.spawn(client_actions, kad_client, nodes)
    return node, kad_client

p = pool.Pool(6000)
network = Simulate(False)
p.spawn(spawn_clients, p, network, 100)
p.join()
