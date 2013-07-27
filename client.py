from routing import Node, RoutingTree
from datastore.simple import simple

import random
import time
import hashlib

import eventlet

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
        self.timers = {}
        self.debug = True

    def log(self, message):
        if self.debug:
            print "%s: %s" % (self.node, message)

    def check_timer(self, name, wait):
        if name in self.timers:
            now = time.time()
            if (now - self.timers[name]) >= wait:
                self.timers[name] = now
                return True
            else:
                return False
        else:
            self.timers[name] = time.time()
            return False

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
        ''' TODO: this need to be changed - as the find node needs to be
        followed up with a bucket refresh after a response has been
        recieved '''
        for node in self.initial_nodes:
            self.log("adding initial node %s" % node)
            self.routing.addNode(node)
        self.perform_find_node(self.node)

    def perform_find_node(self, node):
        ''' perform_find_node starts a FIND_NODE query
        '''
        self.log('perform_find_node against %s' % node.id)
        nodes = self.routing.findClosestNodes(node)
        for dst_node in nodes[:self.alpha]:
            m = self.create_message('FIND_NODE')
            m['data'] = node.id
            self.add_transaction(m['xid'], 'FIND_NODE', dst_node)
            self.send_message(dst_node.addr, dst_node.port, m)
            self.routing.performedLookup(dst_node)

    def perform_find_value(self, key):
        ''' perform a 'FIND_VALUE' request for the given (string) key '''
        key_hash = long(hashlib.sha1(key).digest())
        destination = Node(None, None, key_hash)
        nodes = self.routing.findClosestNodes(destination)
        for dst_node in nodes[:self.alpha]:
            m = self.create_message('FIND_VALUE')
            m['data'] = {
                'key': key,
                'hash': key_hash
            }
            self.add_transaction(m['xid'], 'FIND_VALUE', dst_node)
            self.send_message(dst_node.addr, dst_node.port, m)
            self.routing.performedLookup(dst_node)

    def perform_ping(self, node):
        ''' perform a 'PING' request to the given node '''
        m = self.create_message('PING')
        self.send_message(node.addr, node.port, m)
        self.add_transaction(m['xid'], 'PING', node)

    def perform_store(self, key, value):
        ''' find_node should be called before this, we need
        to keep searching until we stop finding nodes closer
        to the desired key '''

        key_hash = long(hashlib.sha1(key).digest())
        destination = Node(None, None, key_hash)
        nodes = self.routing.findClosestNodes(destination)
        for dst_node in nodes:
            m = self.create_message('STORE')
            m['data'] = {
                'key': key,
                'hash': key_hash,
                'value': value
            }
            self.send_message(dst_node.addr, dst_node.port, m)

    def handle_find_node(self, message):
        ''' handle_find_node looks for a node_id in the 'data
        portion of the message. The client then returns upto k
        nodes from our routing tree which are closest to the
        requested node.
        '''
        self.log('handle_find_node')
        source = message['source']
        node_to_find = Node(None, None, message['data'])
        nodes = self.routing.findClosestNodes(node_to_find)
        m = self.create_message('RETURN_NODE', message['xid'])
        m['data'] = [(n.addr, n.port, n.id) for n in nodes]
        self.send_message(source.addr, source.port, m)

    def handle_return_node(self, message):
        self.log('handle_return_node %s' % message['xid'])
        if 'xid' in message and message['xid'] in self.xids:
            for node in message['data']:
                n = Node(node[0], node[1], node[2])
                self.routing.addNode(n)
            del self.xids[message['xid']]

    def handle_ping(self, message):
        source = message['source']
        m = self.create_message('PONG', message['xid'])
        self.send_message(source.addr, source.port, m)

    def handle_pong(self, message):
        ''' the routing tree will have already been informed that
        we have seen this node, so just delete it from the transaction
        table '''
        if 'xid' in message and message['xid'] in self.xids:
            del self.xids[message['xid']]

    def handle_store(self, message):
        ''' store key/value pair in the data store '''
        required = ['key', 'key_hash', 'value']
        data = message['data']
        if all(k in data for k in required):
            self.data_store.store(k['key'], k['key_hash'], k['value'])

    def handle_find_value(self, message):
        key_hash = message['data']['key_hash']
        source = message['source']
        value = self.data_store.retrieve_value(key_hash)
        if value is not None:
            response = { 'value' : value, 'found' : True }
        else:
            node_to_find = Node(None, None, key_hash)
            nodes = self.routing.findClosestNodes(node_to_find)
            nodes = [(n.addr, n.port, n.id) for n in nodes]
            response = { 'value' : nodes, 'found' : True }

        m = self.create_message('RETURN_VALUE', message['xid'])
        m['data'] = response
        self.send_message(source.addr, source.port, m)

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

    def refresh_buckets(self, force=False):
        nodes  = self.routing.fetchRefreshNodes(force)
        for node in nodes:
            self.perform_find_node(node)

    def run_events(self):
        ''' run_events checks for any events which need to be run
        periodically, such as:

        * timing out rpc requests
        * refreshing buckets
        '''

        # refresh any buckets which need refreshing
        if self.check_timer('refresh_buckets', 60):
            self.refresh_buckets()

        # debug info - delete this
        if self.check_timer('debug', 5):
            c = 0
            for i in self.routing.buckets:
                c = c + len(i.nodes)
            self.log("Know about %s nodes" % c)

    def main(self, wait = None):
        if wait is not None:
            eventlet.sleep(wait)
        self.load_initial_nodes()

        while True:
            try:
                message = self.queue.get(block=True, timeout=5)
                self.process_message(message)
            except: pass

            self.run_events()

            # allow other threads the chance to run
            eventlet.sleep()
