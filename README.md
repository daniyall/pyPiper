A pipelining framework for Python. Developers can create nodes and chain them together to create pipelines. 

Classes that extend ```Node``` must implement ```run``` method that will be called whenever new data is available.  

A simple example

```python
from pyPipelining import Node, Pipeline

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
```

Nodes can also specify a batch size that dictates how much data should be pushed to the node.
For example, building on the previous example. In this case ```batch_size``` is specified in the nodes ```setup``` method. Alternatively, it can be set when creating the node (ex. ```Printer("print", batch_size=5)```)

```python
class Printer(Node):
    def setup(self):
        self.batch_size = Node.BATCH_SIZE_ALL

    def run(self, data):
        print(data)

pipeline = Pipeline(Generate("gen", size=10) | Square("square") | Printer("print"))
print(pipeline)
pipeline.run()
```