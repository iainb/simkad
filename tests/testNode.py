import unittest
from mock import patch
from routing import Node


class TestNode(unittest.TestCase):

    def test_create(self):
        n = Node('127.0.0.1',5000,100)
        self.assertIsInstance(n, Node)

    def test_equal(self):
        a = Node('127.0.0.1',5000,100)
        b = Node('127.0.0.1',5000,100)
        self.assertEqual(a,b)

    def test_not_equal(self):
        a = Node('127.0.0.1',5000,100)
        b = Node('127.0.0.1',5000,500)
        self.assertNotEqual(a,b)

    def test_greater_than(self):
        a = Node('127.0.0.1',5000,100)
        b = Node('127.0.0.1',5000,500)
        self.assertTrue(b > a)

    def test_less_than(self):
        a = Node('127.0.0.1',5000,100)
        b = Node('127.0.0.1',5000,500)
        self.assertTrue(a < b)

    @patch('routing.time')
    def test_seen(self,time_mock):
        time_mock.time.return_value = 1000
        n = Node('127.0.0.1',5000,100)
        n.error()
        self.assertEquals(n.errors, 1)
        self.assertEquals(n.last_seen, 0)
        n.seen()
        self.assertEquals(n.errors, 0)
        self.assertEquals(n.last_seen, 1000)
