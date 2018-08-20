from pyPiper import Node, Pipeline
import time
from tqdm import tqdm

class Generate(Node):
    def setup(self, size, reverse=False):
        self.size = size
        self.reverse = reverse
        self.pos = 0
        self.stateless = False

    def run(self, data):
        if self.pos < self.size:
            if self.reverse:
                res = self.size - self.pos - 1
            else:
                res = self.pos
            self.pos += 1

            self.emit(res)
        else:
            self.close()

class EvenOddGenerate(Node):
    def setup(self, size, reverse=False):
        self.size = size
        self.reverse = reverse
        self.pos = 0
        self.stateless = False

    def run(self, data):
        if self.pos < self.size:
            if self.reverse:
                res = self.size - self.pos - 1
            else:
                res = self.pos
            self.pos += 2

            self.emit([res, res+1])
        else:
            self.close()

class Square(Node):
    def run(self, data):
        self.emit(data**2)

class Double(Node):
    def run(self, data):
        import time
        time.sleep(0.5)
        self.emit(data*2)

class Sleep(Node):
    def run(self, data):
        time.sleep(5)

class Half(Node):
    def run(self, data):
        self.emit(data/2.0)

class Printer(Node):
    def setup(self):
        self.stateless = False
        self.batch_size = Node.BATCH_SIZE_ALL

    def run(self, data):
        print(data)


class TqdmUpdate(tqdm):
    def update(self, delta=1, total_size=None):
        if total_size is not None:
            self.total = total_size
        super().update(delta)


def tmp(delta, total):
    print(delta, total)

if __name__ == '__main__':
    gen = Generate("gen", size=20)
    double = Double("double")
    printer = Printer("printer", batch_size=1)

    p = Pipeline(gen | double | printer, n_threads=4)
    p.run()

    # with TqdmUpdate(desc="Progress") as pbar:
    # p = Pipeline(gen | double | printer, n_threads=4)
    # p.run()
