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

    def test_gen_stream(self):
        gen = Generate("gen", size=10, out_streams="num")
        p = Pipeline(NodeGraph(gen))

        p.run()
        output = get_output()

        expected_out = [str(x) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_gen_reverse_stream(self):
        gen = Generate("gen", size=10, reverse=True, out_streams="num")
        p = Pipeline(NodeGraph(gen))

        p.run()
        output = get_output()

        expected_out = reversed([str(x) for x in range(10)])

        self.assertCountEqual(output, expected_out)

    def test_double_stream(self):
        gen = Generate("gen", size=10, out_streams="num")
        double = Double("double", out_streams="num", in_streams="num")
        p = Pipeline(gen | double)

        p.run()
        output = get_output()

        expected_out = [str(x * 2) for x in range(10)]

        self.maxDiff = None
        self.assertCountEqual(output, expected_out)

    def test_double_square_stream(self):
        gen = Generate("gen", size=10, out_streams="num")
        double = Double("double", out_streams="num", in_streams="num")
        square = Square("square", out_streams="num", in_streams="num")
        p = Pipeline(gen | double | square)

        p.run()
        output = get_output()

        expected_out = [str((x * 2) ** 2) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_double_and_square_stream(self):
        gen = Generate("gen", size=10, out_streams="num")
        double = Double("double", out_streams="num", in_streams="num")
        square = Square("square", out_streams="num", in_streams="num")
        p = Pipeline(gen | [double, square])

        p.run()
        output = get_output()

        expected_out = [str(x * 2) for x in range(10)] + [str(x ** 2) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_printer_stream(self):
        gen = Generate("gen", size=10, out_streams="num")
        printer = Printer("printer", batch_size=1, in_streams="*")
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str(x) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_printer_batch_stream(self):
        gen = Generate("gen", size=10, out_streams="num")
        printer = Printer("printer", batch_size=Node.BATCH_SIZE_ALL)
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str([x for x in range(10)])]

        self.assertCountEqual(output, expected_out)

    def test_streams(self):
        gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])
        printer = Printer("printer", batch_size=Node.BATCH_SIZE_ALL)
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str([[x, x + 1] for x in range(20)[::2]])]

        self.assertCountEqual(output, expected_out)

    def test_streams2(self):
        gen = Generate("gen", size=10, out_streams="*")
        double = Double("double", out_streams="num", in_streams="num")
        p = Pipeline(gen | double)

        p.run()
        output = get_output()

        expected_out = [str(x * 2) for x in range(10)]

        self.maxDiff = None
        self.assertCountEqual(output, expected_out)

    def test_streams3(self):
        gen = Generate("gen", size=10, out_streams="*")
        double = Double("double", out_streams="num")
        p = Pipeline(gen | double)

        p.run()
        output = get_output()

        expected_out = [str(x * 2) for x in range(10)]

        self.maxDiff = None
        self.assertCountEqual(output, expected_out)

    def test_streams4(self):
        gen = Generate("gen", size=10, out_streams="*")
        double = Double("double", in_streams=["even", "odd"])
        p = Pipeline(gen | double)

        with self.assertRaises(Exception) as context:
            p.run()
            self.assertTrue('Node %s emits %i items' % (gen, 1) in str(context.exception))

    def test_streams_even(self):
        gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])
        printer = Printer("printer", in_streams="even")
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str([x for x in range(20)[::2]])]

        self.assertCountEqual(output, expected_out)

    def test_streams_odd(self):
        gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])
        printer = Printer("printer", in_streams="odd")
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str([x + 1 for x in range(20)[::2]])]

        self.assertCountEqual(output, expected_out)

    def test_streams_odd_batch(self):
        gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])
        printer = Printer("printer", in_streams="odd", batch_size=Node.BATCH_SIZE_ALL)
        p = Pipeline(gen | printer)

        p.run()
        output = get_output()

        expected_out = [str([x + 1 for x in range(20)[::2]])]

        self.assertCountEqual(output, expected_out)

    def test_streams_complex(self):
        gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])
        double = Double("double", out_streams="num", in_streams="even")
        square = Square("square", out_streams="num", in_streams="odd")

        printer1 = Printer("p1", in_streams="num", batch_size=Node.BATCH_SIZE_ALL)
        printer2 = Printer("p2", in_streams="num", batch_size=Node.BATCH_SIZE_ALL)

        p = Pipeline(gen | [double | printer1, square | printer2], quiet=False)

        p.run()
        output = get_output()

        expected_out = [str([x + x for x in range(20)[::2]])] + [str([(x + 1) ** 2 for x in range(20)[::2]])]

        self.assertCountEqual(output, expected_out)

if __name__ == '__main__':
    unittest.main(buffer=True)

    # gen = Generate("gen", size=10)
    # double = Double("double")
    # square = Square("square")
    # p = Pipeline(gen | [double, square])
    #
    # p.run()
