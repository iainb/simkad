import unittest
from routing import  Node, RoutingTree

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

        # expected nodes split into buckets along with the bucket min/max
        expected = [(0,15,[0]),(16,31,[25,30]),(32,63,[50]),
                    (64,127,[100]),(128,191,[]),(192,223,[200]),
                    (224,255,[255,230])]

        for i in zip(r.buckets, expected):
            bucket, (mi, mx, nodes) = i
            self.assertEqual(bucket.range_min, mi)
            self.assertEqual(bucket.range_max, mx)
            for j in zip(bucket.nodes, nodes):
                self.assertEqual(j[0].id, j[1])
