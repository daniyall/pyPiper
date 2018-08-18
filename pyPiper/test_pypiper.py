import unittest
import sys

from pyPiper import NodeGraph, Node

def get_output():
    sys.stdout.flush()
    return sys.stdout.getvalue().strip().split("\n")

n1 = Node("n1")
n2 = Node("n2")
n3 = Node("n3")
n4 = Node("n4")
n5 = Node("n5")
n6 = Node("n6")
n7 = Node("n7")

class PyPiperTests(unittest.TestCase):

    def test_parse0(self):
        g = n1 | n2

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)

        self.assertEquals(g, expected_g)

    def test_parse1(self):
        g = n1 | n2 | n3 | n4

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n2, n3)
        expected_g.add(n3, n4)

        self.assertEquals(g, expected_g)

    def test_parse2(self):
        g = n1 | [n2 | n3 | n4]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n2, n3)
        expected_g.add(n3, n4)

        self.assertEquals(g, expected_g)

    def test_parse3(self):
        g = n1 | [n2, n3, n4]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n1, n3)
        expected_g.add(n1, n4)

        self.assertEquals(g, expected_g)

    def test_parse4(self):
        g = n1 | [n2, [n3, n4]]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n1, n3)
        expected_g.add(n1, n4)

        self.assertEquals(g, expected_g)

    def test_parse5(self):
        g = n1 | [n2 | [n3, n4]]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n2, n3)
        expected_g.add(n2, n4)

        self.assertEquals(g, expected_g)

    def test_parse6(self):
        g = n1 | [n2 | [n3 | n4], n5 | n6]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n2, n3)
        expected_g.add(n3, n4)
        expected_g.add(n1, n5)
        expected_g.add(n5, n6)

        self.assertEquals(g, expected_g)

    def test_parse7(self):
        g = n1 | [n2 | [n3 , n4], n5 , n6]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n2, n3)
        expected_g.add(n2, n4)
        expected_g.add(n1, n5)
        expected_g.add(n1, n6)

        self.assertEquals(g, expected_g)

    def test_parse8(self):
        g = n1 | [n2 , [n3 | n4], [n5, n6]]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n1, n3)
        expected_g.add(n3, n4)
        expected_g.add(n1, n5)
        expected_g.add(n1, n6)

        self.assertEquals(g, expected_g)

    def test_parse9(self):
        g = n1 | [n2 , n4 | [n5] , [n6], n3]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n1, n4)
        expected_g.add(n4, n5)
        expected_g.add(n1, n6)
        expected_g.add(n1, n3)

        self.assertEquals(g, expected_g)

    def test_parse10(self):
        g = n1 | [[n2 | n3], [n5 | n6]]

        expected_g = NodeGraph(n1)
        expected_g.add(n1, n2)
        expected_g.add(n2, n3)
        expected_g.add(n1, n5)
        expected_g.add(n5, n6)

        self.assertEquals(g, expected_g)

if __name__ == '__main__':
    unittest.main(buffer=True)