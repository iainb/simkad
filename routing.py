import time
from bisect import bisect_left
import random

class Node:
    ''' Node class contains information about a node in the k network '''
    def __init__(self, addr, port, id):
        self.id   = id
        self.addr = addr
        self.port = port
        self.last_seen = 0
        self.errors    = 0

    def seen(self):
        self.last_seen = time.time()
        self.errors    = 0

    def error(self):
        self.errors += 1
        return self.errors

    def __eq__(self, a):
        ''' TODO: two nodes may have the same id but different ports / address?
        '''
        if isinstance(a, Node):
            a = a.id
        return self.id == a

    def __ne__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.id != a

    def __lt__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.id < a

    def __le__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.id <= a

    def __gt__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.id > a

    def __ge__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.id >= a

    def __str__(self):
        return "Node %s:%s (%s)" % (self.addr, self.port, self.id)

class KbucketFull(Exception):
    ''' KbucketFull is raised when a Kbucket is full '''

class Kbucket:
    ''' Kbucket represents a bucket of nodes within a specific range
    within the k network '''
    def __init__(self, k, range_min, range_max, error_threshold):
        ''' * k is a kademilia defined constant which in this case indicates
              bucket size
            * range_min - starting range of node ids that this bucket contains
            * range_max - ending range of node ids that this bucket contains
            * error_threshold - number of errors a node can suffer before
              being removed from the bucket as stale.
        '''
        self.last_lookup = 0
        self.range_min = range_min
        self.range_max = range_max
        self.k = k
        self.error_threshold = error_threshold
        self.nodes  = []

    def updateRange(self, new_max):
        ''' updateRange is called when a bucket is split, a new max
        position is given and nodes which fall outside this are
        evicted from the bucket and returned to the caller '''
        self.range_max = new_max
        evicted = []
        for node in self.nodes:
            if node.id >= new_max:
                evicted.append(node)
                self.nodes.remove(node)

        return evicted

    def needsRefresh(self):
        if (time.time() - self.last_lookup) > 3600:
            return True
        else:
            return False

    def performedLookup(self):
        ''' performedLookup is called by the client to indicate
        that a lookup has been performed on this bucket range '''
        self.last_lookup = time.time()

    def addNode(self, node, updateSeen=True):
        ''' addNode adds a new node to the bucket, if the bucket is
        full then KbucketFull will be raised. If the node is already
        in the bucket then it will be moved to the end of the list '''
        if node in self.nodes:
            # move node to list tail
            self.nodes.remove(node)
            self.nodes.append(node)
            if updateSeen:
                node.seen()
        elif len(self.nodes) < self.k:
            self.nodes.append(node)
            if updateSeen:
                node.seen()
        else:
            raise KbucketFull("kBucket full")

    def getNode(self, node):
        ''' getNode returns a node from the bucket, node can be a long
        or a Node object.
        '''
        idx = self.nodes.index(node)
        return self.nodes[idx]

    def getRandomNode(self):
        return random.choice(self.nodes)

    def getNodes(self, count=None):
        ''' getNodes returns a fixed number of nodes from the bucket '''
        if count == None:
            count = len(self.nodes)

        if count > self.k:
            count = self.k

        return self.nodes[0:count]

    def getLeastRecentlySeen(self):
        ''' the least recently seen node will be at the head of
        the list '''
        if self.nodes:
            return self.nodes[0]
        else:
            return None

    def removeContact(self, node):
        self.nodes.remove(node)

    def errorNode(self, node):
        ''' errorNode incremements the error count for a node
        if it exceeds the error count then the node is removed
        from the bucket '''
        index = self.nodes.index(node)
        num_errors = self.nodes[index].error()
        if num_errors > self.error_threshold:
            self.removeContact(node)

    def __eq__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.range_min <= a < self.range_max

    def __ne__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.range_min >= a > self.range_max

    def __lt__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.range_max <= a

    def __le__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.range_min < a

    def __gt__(self, a):
        if isinstance(a, Node):
            a = a.id
        return self.range_max >= a

    def __ge__ (self, a):
        if isinstance(a, Node):
            a = a.id
        return self.range_max >= a

class RoutingTree:
    ''' RoutingTree represents the collection of buckets which contain
    all nodes an individual node knows about '''
    def __init__(self, node, k=20, error_threshold=5):
        self.k       = k
        self.error_threshold = error_threshold
        self.node    = node
        self.buckets = []
        self.buckets.append(Kbucket(k, 0, 2**160, self.error_threshold))
        self.addNode(self.node)

    def bucketIndex(self, n):
        if isinstance(n, Node):
            n = n.id
        return bisect_left(self.buckets, n)

    def addNode(self, node):
        ''' addNode adds a Node object into the correct bucket in the tree
        * If the node exists in the tree already then the last seen time will
          be updated.
        * If the bucket is full and the owning node exists in that bucket then
          the bucket will be split
        * If the bucket is full and it is not the owning noding then the node
          has not been seen for the longest will be returned.
          It is the callers responsibility to call replaceStaleNode, if the
          stale node does not respond.
        '''
        index = self.bucketIndex(node)
        try:
            self.buckets[index].addNode(node)
        except KbucketFull:
            if self.buckets[index].range_min <= self.node < self.buckets[index].range_max:
                # our node is in this bucket, split it
                self._splitBucket(self.buckets[index])
            else:
                return self.buckets[index].getLeastRecentlySeen()

        return None

    def _splitBucket(self, bucket):
        ''' _splitBucket splits two buckets within the tree into two separate
        buckets dividing the contents between them '''
        diff = (bucket.range_max - bucket.range_min) / 2
        new  = Kbucket(self.k, bucket.range_max - diff, bucket.range_max, self.error_threshold)
        self.buckets.insert(self.buckets.index(bucket.range_min) + 1, new)
        evicted = bucket.updateRange(bucket.range_max - diff)
        for node in evicted:
            new.addNode(node, updateSeen=False)

    def replaceStaleNode(self, remove, new):
        ''' replaceStaleNode is to be called when a node must be removed from
        the tree with a new node seen '''
        index = self.bucketIndex(remove)
        self.buckets[index].removeContact(remove)
        self.buckets[index].addNode(new)

    def findClosestNodes(self, node):
        ''' findClosestNodes returns the closest nodes this node
        knows about to the target node (node_id). If we know if the
        target node then return a list containing only that.
        '''
        index = self.bucketIndex(node)

        # fetch all nodes from the nearest bucket
        nodes = self.buckets[index].getNodes()

        # remove our node from the list (may be in this bucket)
        try:
            nodes.remove(self.node)
        except ValueError:
            pass

        # if we don't have enough nodes try and return as many as possible
        min_index = index - 1
        max_index = index + 1
        while len(nodes) < self.k:
            if min_index >= 0:
                nodes = nodes + self.buckets[min_index].getNodes()
                min_index = min_index - 1
            if max_index < len(self.buckets):
                nodes = nodes + self.buckets[max_index].getNodes()
                max_index = max_index + 1

            if min_index < 0 and max_index > len(self.buckets):
                break
            else:
                min_index = min_index - 1
                max_index = max_index + 1

        # remove our node from the list (may have been in another bucket)
        try:
            nodes.remove(self.node)
        except ValueError:
            pass

        # sort nodes using xor to determine distance
        nodes.sort(lambda a, b, num=node.id: cmp(num ^ a.id, num ^ b.id))

        # return at most self.k nodes
        return nodes[:self.k]

    def fetchRefreshNodes(self, force=False):
        nodes = []
        for bucket in self.buckets:
            if force:
                nodes.append(bucket.getRandomNode())
            elif bucket.needsRefresh():
                nodes.append(bucket.getRandomNode())

        return nodes

    def performedLookup(self, node):
        index = self.bucketIndex(node)
        self.buckets[index].performedLookup()

    def errorNode(self, node):
        ''' indicate that a node has not responded to a request of some
        kind '''
        index = self.bucketIndex(node)
        self.buckets[index].errorNode(node)
