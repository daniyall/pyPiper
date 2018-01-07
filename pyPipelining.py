from collections import defaultdict

class Pipeline():
    def __init__(self, start):
        if isinstance(start, Node):
            self.start = [start]
        elif type(start) == list:
            self.start = start
        else:
            raise Exception("Start node is of wrong type")

        self._nodes = self.get_nodes(self.start)
        for node in self._nodes:
            node._set_pipeline(self)

        self.reset()

    def reset(self):
        self.running = True
        for node in self._nodes:
            node._reset()

    def get_nodes(self, start_set):
        res = start_set.copy()
        for s in start_set:
            res  += self.get_nodes(s.next)
        return res

    def run(self):
        while True:
            running = False
            for s in self.start:
                if s.running:
                    s._step()
                    running = True

            if not running:
                break

        self.running = False


    def submit_task(self, node, data):
        node.run(data=data)

    def __str__(self):
        return "\n".join([s.graph_to_str() for s in self.start])

    def __repr__(self):
        return self.__str__()

class Node():
    BATCH_SIZE_ALL = -1

    batch_size = 1

    def __init__(self, name, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.name = name


        self.next = []
        self._reset()

    def _reset(self):
        self.next_buffers = defaultdict(list)
        self.running = True
        self.setup()

    def setup(self):
        pass

    def __str__(self):
        return 'N({})'.format(self.name)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __lt__(self, other):
        return self.name < other.name

    def graph_to_str(self):
        if len(self.next) == 1:
            return "%s -> %s" %  (str(self), ", ".join([n.graph_to_str() for n in self.next]))
        elif len(self.next) > 1:
            return "%s -> [%s]" %  (str(self), ", ".join([n.graph_to_str() for n in self.next]))
        else:
            return "%s" % str(self)

    def _add_next(self, n):
        # No next nodes have been added. Meaning n is immediately after self
        if len(self.next) == 0:
            if isinstance(n, Node):
                self.next.append(n)
            elif isinstance(n, list):
                for _n in n:
                    if not isinstance(_n, Node):
                        raise Exception("Can only have node types in pipeline")
                    self.next.append(_n)

        else:
            for _n in self.next:
                _n._add_next(n)


    def __or__(self, other):
        self._add_next(other)

        return self

    def __ror__(self, other):
        if type(other) == list:
            for o in other:
                if not isinstance(o, Node):
                    raise Exception("Can only chain together nodes")
                o._add_next(self)
        else:
            other._add_next(self)

        return other

    def _set_pipeline(self, pipeline):
        self.pipeline = pipeline

    def close(self):
        for n in self.next:
            if n.batch_size > 1 or n.batch_size == self.BATCH_SIZE_ALL:
                self._push_buffer(n, force=True)

        for n in self.next:
            n.close()

        self.running = False

    def _get_next_buffer(self, to):
        return self.next_buffers[str(to)]

    def _push_buffer(self, to, force=False):
        next_buffer = self._get_next_buffer(to)

        if len(next_buffer) == to.batch_size or force:
            to._step(next_buffer)
            next_buffer.clear()


    def emit(self, data):
        for n in self.next:
            # if batch size is 1, don't bother saving to buffer
            if n.batch_size == 1:
                n._step(data)
            else:
                self._push_buffer(n)
                self._get_next_buffer(n).append(data)

    def _step(self, data=None):
        self.pipeline.submit_task(node=self, data=data)


    def run(self, data):
        raise NotImplementedError("Child classes must override run method")


if __name__ == '__main__':
    class Gen(Node):
        def setup(self):
            self.pos = 0
            self.reverse = False

        def run(self, data):
            if self.pos < self.size:
                if self.reverse:
                    self.emit(self.size - self.pos)
                else:
                    self.emit(self.pos)
                self.pos = self.pos + 1
            else:
                self.close()

    class Square(Node):
        def run(self, data):
            self.emit(data*data)

    class Half(Node):
        def run(self, data):
            self.emit(data/2.0)

    class Print(Node):
        def setup(self):
            pass
            # self.batch_size = 2

        def run(self, data):
            print(data)

    gen = Gen("gen", size=10)
    gen1 = Gen("gen1", size=10, reverse=True)
    sq = Square("sq")
    half = Half("half")
    pr = Print("print", batch_size=1)
    pr1 = Print("print all", batch_size=Node.BATCH_SIZE_ALL)

    p = Pipeline(gen | [half, sq] | [pr, pr1])
    print(p)

    p.run()
