import unittest
from mock import patch
from routing import Kbucket, Node, KbucketFull, KbucketWrong, RoutingTree

class TestRoutingTree(unittest.TestCase):

    def test_create(self):
         r = RoutingTree(Node(None, None, 100))
         self.assertIsInstance(r, RoutingTree)

    def test_split_bucket(self):
        this = Node(None, None, 100)
        r = RoutingTree(this, 2, 5, 5, 8)
        nodes  = [
            Node(None, None, 0),
            Node(None, None, 25),
            Node(None, None, 30),
            Node(None, None, 50),
            Node(None, None, 200),
            Node(None, None, 255),
            Node(None, None, 230),
        ]

        for node in nodes:
            r.addNode(node)

        self.assertEquals(len(r.buckets), 4)
        expected = [(0,63),(64,127),(128,191),(192,255)]
        for i in zip(r.buckets, expected):
            bucket, (mi, mx) = i
            self.assertEqual(bucket.range_min, mi)
            self.assertEqual(bucket.range_max, mx)
            # TODO ensure all nodes end up in the routing tree!
            for j in bucket.nodes:
                print j.id
