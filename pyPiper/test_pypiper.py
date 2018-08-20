import unittest
import sys

from pyPiper import NodeGraph, Node, Pipeline
from example import Generate, Double, Square, Printer, Half, EvenOddGenerate


def get_output():
    sys.stdout.flush()
    return sys.stdout.getvalue().strip().split("\n")

class DummyNode(Node):
    def run(self, data):
        pass

n1 = DummyNode("n1")
n2 = DummyNode("n2")
n3 = DummyNode("n3")
n4 = DummyNode("n4")
n5 = DummyNode("n5")
n6 = DummyNode("n6")
n7 = DummyNode("n7")


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

    def test_double(self):
        gen = Generate("gen", size=10)
        double = Double("double")
        p = Pipeline(gen | double)

        p.run()
        output = get_output()

        expected_out = [str(x * 2) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_double_square(self):
        gen = Generate("gen", size=10)
        double = Double("double")
        square = Square("square")
        p = Pipeline(gen | double | square)

        p.run()
        output = get_output()

        expected_out = [str((x * 2) ** 2) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_double_and_square(self):
        gen = Generate("gen", size=10)
        double = Double("double")
        square = Square("square")
        p = Pipeline(gen | [double, square])

        p.run()
        output = get_output()

        expected_out = [str(x * 2) for x in range(10)] + [str(x ** 2) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_printer(self):
        gen = Generate("gen", size=10)
        printer = Printer("printer", batch_size=1)
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str(x) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_printer_batch(self):
        gen = Generate("gen", size=10)
        printer = Printer("printer", batch_size=Node.BATCH_SIZE_ALL)
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str([x for x in range(10)])]

        self.assertCountEqual(output, expected_out)

if __name__ == '__main__':
    unittest.main(buffer=True)