from pyPiper import Node, Pipeline

class Generate(Node):
    def setup(self):
        self.pos = 0

    def run(self, data):
        if self.pos < self.size:
            self.emit(self.pos)
            self.pos = self.pos + 1
        else:
            self.close()

class Square(Node):
    def run(self, data):
        self.emit(data**2)


pipeline = Pipeline(Generate("gen", size=10) | Square("square"))
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