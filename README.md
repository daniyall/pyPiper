A parallel pipelining framework for Python. Developers can create nodes and chain them together to create pipelines. 

Classes that extend ```Node``` must implement ```run``` method that will be called whenever new data is available.  

## Installation

```bash
pip install pypiper
```


## Example

```python
from pyPiper import Node, Pipeline

class Generate(Node):
    def setup(self, size):
        self.size = size
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
For example, building on the previous example. In this case `batch_size` is specified in the nodes `setup` 
method. Alternatively, it can be set when creating the node (ex. `Printer("print", batch_size=5)`)

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

## Parallel Execution 
To process pipelines in parallel, pass `n_threads` > 1 when creating the pipeline.
Parallel execution is done using `multiprocessing` and is well suited to CPU intensive tasks such as audio processing 
and feature extraction. For example:

```python
class Generate(Node):
    def setup(self, size):
        self.pos = 0
        self.stateless = False

    def run(self, data):
        if self.pos < self.size:
            self.emit(self.pos)
            self.pos = self.pos + 1
        else:
            self.close()

pipeline = Pipeline(Generate("gen", size=10) | Square("square") | Printer("print"), n_threads=2)
print(pipeline)
pipeline.run()
```

Note that since the generate Node needs to store state (pos` variable), it must set `self.stateless = False`. 
This keeps the nodes state synchronized between different processes.


## Stream Names
You can also name input and output streams. For example:

```python
gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])
double = Double("double", out_streams="num", in_streams="even")
square = Square("square", out_streams="num", in_streams="odd")

printer1 = Printer("p1", in_streams="num", batch_size=Node.BATCH_SIZE_ALL)
printer2 = Printer("p2", in_streams="num", batch_size=Node.BATCH_SIZE_ALL)

p = Pipeline(gen | [double | printer1, square | printer2], quiet=False)

p.run()
```

EvenOddGenerate generates a pair of numbers. using the `out_streams` parameter, we name the first number even and second
number odd. When initializing the double and square nodes, we tell double to take the even number and square to take
the odd number. 

