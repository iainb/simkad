from routing import Node, RoutingTree
from datastore.simple import simple

import random
import time

class Client:
    def __init__(self, network, addr=None, port=None, node_id=None, nodes = []):
        self.alpha = 3
        self.network = network
        self.addr, self.port, self.queue = self.network.connect(addr, port)

        if node_id is None:
            node_id = random.randint(0,2**160)

        self.node    = Node(self.addr, self.port ,node_id)

        self.routing = RoutingTree(self.node)

        self.actions = {
            'PING'        : self.handle_ping,
            'PONG'        : self.handle_pong,
            'STORE'       : self.handle_store,
            'FIND_VALUE'  : self.handle_find_value,
            'FIND_NODE'   : self.handle_find_node,
            'RETURN_NODE' : self.handle_return_node
        }

        self.data_store = simple()
        self.initial_nodes = nodes
        self.xids = {}
        self.debug = True

    def log(self, message):
        if self.debug:
            print "%s: %s" % (self.node, message)

    def return_node(self):
        return Node(self.node.addr, self.node.port, self.node.id)

    def create_message(self, msg_type, xid=None):
        ''' create_message creates a message to be sent to another node.
        If a transaction id is not specified then one is created.
        '''
        if not xid:
            xid = random.randint(0,2**160)
            while xid in self.xids:
                xid = random.randint(0,2**160)

        return {
            'source': [ self.node.addr, self.node.port, self.node.id ],
            'xid'   : xid,
            'type'  : msg_type,
            'data'  : {}
        }

    def add_transaction(self, xid, m_type, dest_node):
        self.xids[xid] = {
            'dest' : dest_node,
            'type' : m_type,
            'sent' : time.time()
        }

    def load_initial_nodes(self):
        for node in self.initial_nodes:
            self.log("adding initial node %s" % node)
            self.routing.addNode(node)
        self.perform_lookup(self.node)

    def perform_find_node(self, node):
        ''' perform_find_node starts a FIND_NODE query
        '''
        self.log('perform_find_node against %s' % node.id)
        nodes = self.routing.findClosestNodes(node)
        for dst_node in nodes[:self.alpha]:
            m = self.create_message('FIND_NODE')
            m['data'] = node.id
            self.send_message(dst_node.addr, dst_node.port, m)
            self.add_transaction(m['xid'], 'FIND_NODE', dst_node)

    def handle_find_node(self, message):
        ''' handle_find_node looks for a node_id in the 'data
        portion of the message. The client then returns upto k
        nodes from our routing tree which are closest to the
        requested node.
        '''
        self.log('handle_find_node')
        source = message['source']
        nodes = self.routing.findClosestNodes(Node(None,None,message['data']))
        m = self.create_message('RETURN_NODE', message['xid'])
        m['data'] = [(n.addr, n.port, n.id) for n in nodes]
        self.send_message(source.addr, source.port, m)

    def handle_return_node(self, message):
        self.log('handle_return_node')
        if 'xid' in message and message['xid'] in self.xids:
            for node in message['data']:
                n = Node(node[0], node[1], node[2])
                self.routing.addNode(n)
            del self.xids[message['xid']]

    def handle_ping(self, message):
        pass

    def handle_pong(self, message):
        pass

    def handle_store(self, message):
        required = ['key', 'value']
        if all(k in message for k in required):
            self.data_store[message['key']] = message['value']

    def handle_find_value(self, message):
        pass

    def process_message(self, m):
        if 'type' in m and m['type'] in self.actions:
            # add node into our routing tree
            node = Node(m['source'][0], m['source'][1], m['source'][2])
            self.routing.addNode(node)
            m['source'] = node

            # handle message
            self.actions[m['type']](m)
        else:
            self.log("process_message malformed message: %s" % m)



    def send_message(self, addr, port, message):
        self.log('send_message => %s:%s' % (addr, port))
        self.network.send(addr, port, message)

    def main(self):
        self.load_initial_nodes()
        while True:
            message = None
            try:
                message = self.queue.get(block=True, timeout=5)
                self.process_message(message)
            except:
                pass
