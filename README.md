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

If multiple output streams are passed into a node, by default, they will be come into the node as a list. For example,

```python
gen = EvenOddGenerate("gen", size=10, out_streams=["even", "odd"])
printer = Printer("p1", batch_size=1)

p = Pipeline(gen | printer, quiet=False)
p.run()
``` 
Will output

```python
[0,1],
[2,3],
...
```

However, if you can split the streams by specifying their names in the input streams `in_streams` parameter. So,
 ```python
gen = EvenOddGenerate("gen", size=20, out_streams=["even", "odd"])

printer = Printer("p1", in_streams=["even", "odd"], batch_size=1)

p = Pipeline(gen | printer, quiet=False)
p.run()
```

Will generate:
```python
0,
1,
2,
3,
...
```

 



## Progress updates
When calling `pipeline.run()`, you can provide a callback function for progress updates. Whenever
the pipelines makes progress, it calls this function with the number of items that have been processed
so far and the total number of items that need to be processed. For example, if you were using a
tqdm progress bar, you could use the following code:

```python
from tqdm import tqdm

class TqdmUpdate(tqdm):
    def update(self, done, total_size=None):
        if total_size is not None:
            self.total = total_size
        self.n = done
        super().refresh()

if __name__ == '__main__':
    gen = Generate("gen", size=10)
    double = Double("double")
    sleeper = Sleep("sleep")

    p = Pipeline(gen | [double, sleeper], n_threads=4, quiet=True)
    with TqdmUpdate(desc="Progress") as pbar:
        p.run(update_callback=pbar.update)
```
