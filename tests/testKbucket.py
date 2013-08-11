import unittest
from mock import patch
from routing import Kbucket, Node, KbucketFull

class TestKbucket(unittest.TestCase):

    def setUp(self):
        self.bucket = Kbucket(5,0,(2**8)-1,5)
        self.nodes  = [
            Node(None, None, 0),
            Node(None, None, 25),
            Node(None, None, 50),
            Node(None, None, 255),
            Node(None, None, 230),
        ]

    def test_create(self):
        b = Kbucket(20,0,(2**160)-1,5)
        self.assertIsInstance(b, Kbucket)

    def test_add_node(self):
        self.bucket.addNode(self.nodes[0])
        self.assertIn(self.nodes[0], self.bucket.nodes)

    def test_recently_seen(self):
        for n in self.nodes:
            self.bucket.addNode(n)

        # first added node will be least recently seen
        self.assertEqual(self.bucket.nodes.index(self.nodes[0]), 0)
        self.assertEqual(self.bucket.getLeastRecentlySeen(), self.nodes[0])

        # add first node again, moving it to tail of the list
        self.bucket.addNode(self.nodes[0])
        self.assertEqual(self.bucket.nodes.index(self.nodes[0]), 4)

        # now least recently seen node will be the node added second
        self.assertEqual(self.bucket.getLeastRecentlySeen(), self.nodes[1])

    def test_get_node(self):
        for n in self.nodes:
            self.bucket.addNode(n)

        a = self.nodes[0]
        b = self.bucket.getNode(a)
        self.assertEqual(a,b)

    def test_raises_KbucketFull_exception(self):
        for n in self.nodes:
            self.bucket.addNode(n)

        self.assertRaises(KbucketFull, self.bucket.addNode, Node(None, None, 231))




