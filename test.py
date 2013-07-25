from eventlet import greenpool
from client import Client
from network.simulate import Simulate

'''
me = routing.Node(random.randint(0,2**160), '127.0.0.1', 5000)
tree = routing.RoutingTree(20, me, 5)
for i in xrange(0,50):
    n = routing.Node(random.randint(0,2**160), '127.0.0.1', 5001 + i)
    last = n
    tree.addNode(n)



c = 0
for bucket in tree.buckets:
    c = c + len(bucket.nodes)


rand_id = random.randint(0,2**160)
nodes = tree.findClosestNodes(rand_id)
print ",".join(str(x) for x in nodes)
print len(nodes)

nodes = tree.findClosestNodes(last)
print ",".join(str(x) for x in nodes)
print len(nodes)
print "last node: %s" % last
'''

pool = greenpool.GreenPool(100)

network = Simulate(True)
client_a = Client(network)
node_a = client_a.return_node()
client_b = Client(network, nodes=[node_a])


pool.spawn(client_a.main)
pool.spawn(client_b.main)
pool.waitall()




