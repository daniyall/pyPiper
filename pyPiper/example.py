from pyPiper import Node, Pipeline
import time

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

class Square(Node):
    def run(self, data):
        self.emit(data**2)

class Double(Node):
    def run(self, data):
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

if __name__ == '__main__':
    p1 = Pipeline(Generate("gen", size=10) | Square("square"), quiet=False)
    print(p1)
    p1.run()

    p2 = Pipeline(Generate("gen", size=10) | Square("square") | Printer("print"))
    print(p2)
    p2.run()

    p3 = Pipeline(Generate("gen", size=10) | Square("square") | Double("double"), n_threads=4, quiet=False)
    print(p3)
    p3.run()

