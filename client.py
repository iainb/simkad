from routing import Node, RoutingTree
from datastore.simple import simple

import random
import time
import hashlib
import traceback

import gevent
from gevent.queue import Queue, Empty

class Kad_Client:
    ''' The Kad_Client class is responsible for performing primary kad
    actions which can involve multiple RPC requests and state which must be
    maintained across multiple requests '''
    def __init__(self, pool, rpc_client, rpc_client_chan):
        self.pool = pool
        self.rpc_client = rpc_client
        self.rpc_chan = rpc_client_chan
        self.node = self.rpc_client.return_node()

        # start the rpc client event loop
        self.pool.spawn(rpc_client.main)

    def create_message(self, m_type, chan = None):

        if not chan:
            chan = Queue()

        m = {
            'type' : m_type,
            'data' : {},
            'chan' : chan
        }
        return m, chan

    def blocking_send_message(self, message):
        m = { 'type' : 'int', 'data' : message }
        self.rpc_chan.put(m)
        return message['chan'].get(block=True)

    def send_message(self, message):
        m = { 'type' : 'int', 'data' : message }
        self.rpc_chan.put(m)

    def join_network(self, nodes):
        self.pool.spawn(self._join_network, nodes)

    def _join_network(self, nodes):
        ''' _join_network is a blocking method call that joins a kad node
        to the network.
        '''

        # 1. add nodes to the network
        m, chan = self.create_message('ADD_NODE')
        m['data'] = nodes
        self.blocking_send_message(m)

        # 2. perform a find node on ourselves
        self._node_lookup(self.node)

        # 3. force refresh buckets
        '''
        m, chan = self.create_message('REFRESH_BUCKETS')
        self.blocking_send_message(m)
        '''

    def store_value(self, key, value):
        pass

    def fetch_value(self, key):
        pass

    def _node_lookup(self, node):
        ''' _node_lookup is a blocking call that finds k closest nodes
        to a given node '''

        # 1. fetch k closest nodes from our routing table
        m, chan = self.create_message('FIND_CLOSEST_NODES')
        m['data'] = node
        nodes = self.blocking_send_message(m)

        # 2. perform 'SEND_FIND_NODE' against alpha of them
        if nodes:
            m, chan = self.create_message('SEND_FIND_NODE')
            m['data']['req_node'] = self.node
            m['data']['node'] = nodes[0]
            self.send_message(m)
            response = chan.get(block=True)


class Rpc_Client:
    ''' The Rpc_Client layer is responsible for communicating with other nodes
    on the network, responding to their RPC requests and starting new RPC
    requests on behalf of the Kad_Client class '''
    def __init__(self, network, client_chan, node=None, alpha = 3):
        self.alpha = alpha       # concurrent network queries
        self.chan = client_chan  # channel for internal & network rpcs
        self.network = network   # interface to our network (nonblocking sends)

        if node == None:
            node = Node(None, None, None)

        self.addr, self.port = self.network.connect(self.chan, node.addr, node.port)
        node.addr = self.addr
        node.port = self.port

        if node.id is None:
            node.id = random.randint(0,2**160)

        self.node    = node

        self.routing = RoutingTree(self.node)

        self.rpc_actions = {
            'PING'         : self.rpc_handle_ping,
            'PONG'         : self.rpc_handle_pong,
            'STORE'        : self.rpc_handle_store,
            'FIND_VALUE'   : self.rpc_handle_find_value,
            'FIND_NODE'    : self.rpc_handle_find_node,
            'RETURN_NODE'  : self.rpc_handle_return_node,
            'RETURN_VALUE' : self.rpc_handle_return_value
        }

        self.internal_actions = {
            'ADD_NODE'           : self.int_add_node,
            'FIND_CLOSEST_NODES' : self.int_find_closest_nodes,
            'SEND_FIND_NODE'     : self.int_send_find_node
        }

        self.data_store = simple()
        self.rpc_xids = {}
        self.timers = {}
        self.debug = True

    def log(self, message):
        if self.debug:
            print "%s: %s" % (self.node, message)

    def int_add_node(self, message):
        if 'data' in message:
            if type(message['data']) == list:
                for node in message['data']:
                    self.routing.addNode(node)
            else:
                self.routing.addNode(message['data'])
            message['chan'].put(True)
        else:
            message['chan'].put(False)

    def int_find_closest_nodes(self, message):
        if 'data' in message:
            nodes = self.routing.findClosestNodes(message['data'])
            message['chan'].put(nodes)
        else:
            message['chan'].put([])

    def int_send_find_node(self, message):
        if 'data' in message:
            d = message['data']
            self.rpc_perform_find_node(d['node'], d['req_node'], message['chan'])

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

    def rpc_add_transaction(self, xid, m_type, dest_node, chan = None):
        self.rpc_xids[xid] = {
            'dest' : dest_node,
            'type' : m_type,
            'sent' : time.time(),
            'chan' : chan
        }

    def rpc_create_message(self, msg_type, xid=None):
        ''' rpc_create_message creates a message to be sent to another node.
        If a transaction id is not specified then one is created.
        '''
        if not xid:
            xid = random.randint(0,2**160)
            while xid in self.rpc_xids:
                xid = random.randint(0,2**160)

        return {
            'source': [ self.node.addr, self.node.port, self.node.id ],
            'xid'   : xid,
            'type'  : msg_type,
            'data'  : {}
        }

    def rpc_perform_find_node(self, node, req_node, chan):
        ''' rpc_find_node sends a 'FIND_NODE' query to 'node' against
        the 'req_node' node '''
        m = self.rpc_create_message('FIND_NODE')
        m['data'] = req_node.id
        self.rpc_add_transaction(m['xid'], 'FIND_NODE', node, chan)
        self.rpc_send_message(node.addr, node.port, m)
        self.routing.performedLookup(node)

    def rpc_perform_find_value(self, node, key, chan):
        ''' rpc_find_value sends a 'FIND_VALUE' rpc to the requested
        node for the requested key '''
        key_hash = long(hashlib.sha1(key).digest())
        m = self.rpc_create_message('FIND_VALUE')
        m['data'] = {
            'key': key,
            'hash': key_hash
        }

        self.add_transaction(m['xid'], 'FIND_VALUE', node, chan)
        self.rpc_send_message(node.addr, node.port, m)
        self.routing.performedLookup(node)

    def rpc_perform_ping(self, node, chan):
        ''' rpc_perform_ping sends 'PING' request to the given node '''
        m = self.rpc_create_message('PING')
        self.send_rpc_message(node.addr, node.port, m, chan)
        self.add_transaction(m['xid'], 'PING', node)

    def rpc_perform_store(self, node, key, value):
        ''' rpc_perform_store sends a 'STORE' rpc to the
        requested node '''

        key_hash = long(hashlib.sha1(key).digest())

        m = self.rpc_create_message('STORE')
        m['data'] = {
            'key': key,
            'hash': key_hash,
            'value': value
        }
        self.rpc_send_message(node.addr, node.port, m)

    def rpc_handle_find_node(self, message):
        ''' rpc_handle_find_node looks for a node_id in the 'data
        portion of the message. The client then returns upto k
        nodes from our routing tree which are closest to the
        requested node.
        '''
        source = message['source']
        node_to_find = Node(None, None, message['data'])
        nodes = self.routing.findClosestNodes(node_to_find)
        m = self.rpc_create_message('RETURN_NODE', message['xid'])
        m['data'] = [(n.addr, n.port, n.id) for n in nodes]
        self.rpc_send_message(source.addr, source.port, m)

    def rpc_handle_return_node(self, message):
        ''' rpc_handle_return_node handles a 'RETURN_NODE' message,
        add all returned nodes to the routing tree
        if there is a channel associated with this request then
        send the node list back
        '''
        if 'xid' in message and message['xid'] in self.rpc_xids:
            nodes = []

            for node in message['data']:
                n = Node(node[0], node[1], node[2])
                self.routing.addNode(n)
                nodes.append(n)

            if self.rpc_xids[message['xid']]['chan']:
                self.rpc_xids[message['xid']]['chan'].put(nodes)

            del self.rpc_xids[message['xid']]

    def rpc_handle_ping(self, message):
        ''' rpc_handle_ping handles the rpc 'PING' message '''
        source = message['source']
        m = self.rpc_create_message('PONG', message['xid'])
        self.rpc_send_message(source.addr, source.port, m)

    def rpc_handle_pong(self, message):
        ''' rpc_handle_pong handles the rpc 'PONG' message '''
        if 'xid' in message and message['xid'] in self.rpc_xids:
            if self.rpc_xids[message['xid']]['chan']:
                self.rpc_xids[message['xid']]['chan'].put(True)

            del self.rpc_xids[message['xid']]

    def rpc_handle_store(self, message):
        ''' rpc_handle_store handles the rpc 'STORE' which is to
        store the requested key/value in our datastore '''
        required = ['key', 'key_hash', 'value']
        data = message['data']
        if all(k in data for k in required):
            self.data_store.store(k['key'], k['key_hash'], k['value'])

    def rpc_handle_find_value(self, message):
        ''' rpc_handle_find_value handles the rpc 'FIND_VALUE' message
        it either returns the value, if it is stored at this node or
        returns the closest k nodes to the requested key from our
        routing tree
        '''
        key_hash = message['data']['key_hash']
        source = message['source']
        value = self.data_store.retrieve_value(key_hash)
        if value is not None:
            response = { 'value' : value, 'found' : True }
        else:
            node_to_find = Node(None, None, key_hash)
            nodes = self.routing.findClosestNodes(node_to_find)
            nodes = [(n.addr, n.port, n.id) for n in nodes]
            response = { 'value' : nodes, 'found' : False }

        m = self.rpc_create_message('RETURN_VALUE', message['xid'])
        m['data'] = response
        self.rpc_send_message(source.addr, source.port, m)

    def rpc_handle_return_value(self, message):
        if 'xid' in message and message['xid'] in self.rpc_xids:
            if self.rpc_xids[message['xid']]['chan']:
                self.rpc_xids[message['xid']]['chan'].put(message['data'])
            del self.rpc_xids[message['xid']]

    def rpc_handle_message(self, m):
        ''' rpc_handle_message is the initial handler for all rpc messages
        the correct method will be called based on the message type
        '''
        if 'type' in m and m['type'] in self.rpc_actions:
            # add node into our routing tree
            node = Node(m['source'][0], m['source'][1], m['source'][2])
            self.routing.addNode(node)
            m['source'] = node

            # handle message
            self.rpc_actions[m['type']](m)
        else:
            self.log("process_message malformed message: %s" % m)

    def rpc_send_message(self, addr, port, message):
        ''' rpc_send_message sends an rpc message onto the network '''
        self.log('send_rpc_message => %s:%s' % (addr, port))
        self.network.send(addr, port, message)

    def int_handle_message(self, m):
        ''' handle messages from ourselves '''
        if 'type' in m and m['type'] in self.internal_actions:
            self.internal_actions[m['type']](m)
        else:
            self.log("process_internal_message: malformed: %s" % m)

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
            gevent.sleep(wait)

        while True:
            try:
                message = self.chan.get(block=True, timeout=5)
                if 'type' in message and message['type'] == 'rpc':
                    self.rpc_handle_message(message['data'])
                elif 'type' in message and message['type'] == 'int':
                    self.int_handle_message(message['data'])
                else:
                    self.log('unhandled message: %s' % message)
            except Empty: pass
            except:
                self.log('Client exception')
                if self.debug:
                    traceback.print_exc()

            self.run_events()

            # allow other threads the chance to run
            gevent.sleep()
