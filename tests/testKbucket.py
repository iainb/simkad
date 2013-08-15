import unittest
from mock import patch
from routing import Kbucket, Node, KbucketFull, KbucketWrong

class TestKbucket(unittest.TestCase):

    def setUp(self):
        self.key_space= 8
        self.bucket = Kbucket(5,0,(2**self.key_space)-1,5,self.key_space)
        self.nodes  = [
            Node(None, None, 0),
            Node(None, None, 25),
            Node(None, None, 50),
            Node(None, None, 255),
            Node(None, None, 230),
        ]

    def test_create(self):
        b = Kbucket(20,0,(2**160)-1,5, 160)
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

    def test_raises_KbucketWrong_exception(self):
        b = Kbucket(20,0,127,5,7)
        self.assertRaises(KbucketWrong, b.addNode, Node(None, None, 255))

    def test_depth(self):
        # inital tree with one bucket for 7 bit node id space
        # depth: 0
        a = Kbucket(20,0,127,5,7)
        self.assertEquals(a.getDepth(), 0)

        # tree split once
        # depth: 1
        a = Kbucket(20,0,63,5,7)
        b = Kbucket(20,64,127,5,7)
        self.assertEquals(a.getDepth(), 1)
        self.assertEquals(b.getDepth(), 1)

        # tree split twice
        # depth: 2
        a = Kbucket(20,0,31,5,7)
        b = Kbucket(20,32,63,5,7)
        c = Kbucket(20,64,95,5,7)
        d = Kbucket(20,96,127,5,7)
        self.assertEquals(a.getDepth(), 2)
        self.assertEquals(b.getDepth(), 2)
        self.assertEquals(c.getDepth(), 2)
        self.assertEquals(d.getDepth(), 2)

    def test_split(self):
        for n in self.nodes:
            self.bucket.addNode(n)

        expected_kept = [Node(None,None,0), Node(None,None,25), Node(None,None, 50)]
        expected_evicted = [Node(None,None,255), Node(None,None,230)]
        evicted = self.bucket.updateRange(128)
        self.assertEquals(expected_evicted, evicted)
        self.assertEquals(expected_kept, self.bucket.nodes)

    def test_bad_bucket(self):
        # bucket range min > max
        self.assertRaises(KbucketWrong, Kbucket, 20,20,0,5,7)
        # bucket range min < 0
        self.assertRaises(KbucketWrong, Kbucket, 20,-1,20,5,7)
        # bucket range max > key space
        self.assertRaises(KbucketWrong, Kbucket, 20, 0,500,5,7)

    def test_eq(self):
        a = Kbucket(20,0,127,5,7)
        n = Node(None, None, 100)
        self.assertTrue(a == n)

        n = Node(None, None, 128)
        self.assertFalse(a == n)

    def test_ne(self):
        a = Kbucket(20,0,63,5,7)
        n = Node(None, None, 100)
        self.assertTrue(a != n)

        n = Node(None, None, 50)
        self.assertFalse(a != n)

    def test_gt(self):
        a = Kbucket(20,64,127,5,7)
        n = Node(None, None, 10)
        self.assertTrue(a > n)

        n = Node(None, None, 128)
        self.assertFalse(a > n)

    def test_ge(self):
        a = Kbucket(20,64,127,5,7)
        n = Node(None, None, 65)
        self.assertTrue(a >= n)

        n = Node(None, None, 128)
        self.assertFalse(a >= n)

    def test_lt(self):
        a = Kbucket(20,0,63,5,7)
        n = Node(None, None, 65)
        self.assertTrue(a < n)

        n = Node(None, None, 63)
        self.assertFalse(a < n)

    def test_le(self):
        a = Kbucket(20,0,63,5,7)
        n = Node(None, None, 63)
        self.assertTrue(a <= n)

        n = Node(None, None, -1)
        self.assertFalse(a <= n)
