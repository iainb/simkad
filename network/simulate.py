from eventlet import queue
import random
import socket
import struct

class Simulate:
    ''' Simulate is a network interface to a simulated ip network
    all nodes on the network must share the same instance of this
    class.

    The nodes can join the network by calling connect and send messages
    to other nodes by using 'send'
    '''
    def __init__(self, debug=False):
        self.queues = {}
        self.debug = debug

    def log(self, message):
        if self.debug:
            print "network: %s" % message

    def send(self, addr, port, message):
        ''' send sends a single message to a simulated
        remote host. As we are simulating UDP the message
        will be silently dropped with the destination
        isn't connected to the network
        '''
        if (addr, port) in self.queues:
            self.queues[(addr, port)].put(message, block=False)
            self.log("%s => %s:%s" % (message, addr, port))
        else:
            self.log("%s:%s not on the network" % (addr, port))


    def connect(self, addr = None, port = None):
        ''' connect to simulated network, if addr
        or port are specified then assign them randomly

        return addr, port and queue to read from for incoming messages
        '''
        addr, port = self._assign_address(addr, port)
        self.queues[(addr, port)] = queue.Queue()
        self.log("%s:%s joined the network" % (addr, port))
        return addr, port, self.queues[(addr, port)]

    def disconnect(self, addr, port, queue):
        ''' disconnect removes a connection from the pool.
        The client's queue must also be passed to prevent
        disconnecting other clients by accident '''
        if (addr, port) in self.queues and self.queues[(addr, port)] == queue:
            del self.queues[(addr, port)]
            self.log("%s:%s left the network" % (addr, port))
        else:
            raise Exception('Disconnection error')

    def _assign_address(self, addr, port):
        ''' assign_address if address and port are not specified then
        create a random address. If the address is already in use
        (unlikely but possible) then raise an exception
        '''

        if addr is None:
            addr = random.randint(0, 2**32)
            addr = self._long2ip(addr)

        if port is None:
            port = random.randint(49152, 2**16)

        if (addr,port) in self.queues:
            raise Exception("%s:%s already in use" % (addr, port))
        else:
            return addr, port

    def _ip2long(self, ip):
        ''' convert ip address string to long '''
        packed = socket.inet_aton(ip)
        return struct.unpack('!L', packed)[0]

    def _long2ip(self, ip):
        ''' convert long to ip address string '''
        return socket.inet_ntoa(struct.pack('!L', ip))
