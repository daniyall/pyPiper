from pyPiper import Node, Pipeline

class Generate(Node):
    def setup(self):
        self.pos = 0
        self.stateless = False

    def run(self, data):
        if self.pos < self.size:
            res = self.pos
            self.pos = self.pos + 1
            self.emit(res)
        else:
            self.close()

class Square(Node):
    def run(self, data):
        self.emit(data**2)

class Double(Node):
    def run(self, data):
        self.emit(data*2)


pipeline = Pipeline(Generate("gen", size=10) | Square("square"), quiet=True)
print(pipeline)
pipeline.run()


class Printer(Node):
    def setup(self):
        self.batch_size = Node.BATCH_SIZE_ALL

    def run(self, data):
        print(data)

pipeline = Pipeline(Generate("gen", size=10) | Square("square") | Printer("print"))
print(pipeline)
pipeline.run()

p = Pipeline(Generate("g", size=10) | Square("s") | Double("d"), n_threads=4, quiet=False)
p.run()
