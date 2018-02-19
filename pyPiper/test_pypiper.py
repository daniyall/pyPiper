import unittest
import sys

from pyPiper import Pipeline, Node

from example import Generate, Double, Square, Printer, Half

def get_output():
    sys.stdout.flush()
    return sys.stdout.getvalue().strip().split("\n")

class PyPiperTests(unittest.TestCase):

    def test_gen(self):
        gen = Generate("gen", size=10)
        p = Pipeline(gen)

        p.run()
        output = get_output()

        expected_out = [str(x) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_gen_reverse(self):
        gen = Generate("gen", size=10, reverse=True)
        p = Pipeline(gen)

        p.run()
        output = get_output()

        expected_out = reversed([str(x) for x in range(10)])

        self.assertCountEqual(output, expected_out)

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
        square = Square("double")
        p = Pipeline(gen | double | square)

        p.run()
        output = get_output()

        expected_out = [str((x * 2)**2) for x in range(10)]

        self.assertCountEqual(output, expected_out)

    def test_double_and_square(self):
        gen = Generate("gen", size=10)
        double = Double("double")
        square = Square("double")
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

    # def test_double_mt(self):
    #     gen = Generate("gen", size=10)
    #     double = Double("double")
    #     p = Pipeline(gen | double, n_threads=4)
    #
    #     p.run()
    #     output = get_output()
    #
    #     expected_out = [str(x * 2) for x in range(10)]
    #
    #     print(output, expected_out)
    #
    #     self.assertCountEqual(output, expected_out)

if __name__ == '__main__':
    unittest.main(buffer=True)