from abc import ABC, abstractmethod
import json
from collections import defaultdict, deque

from executors import Executor, ParallelExecutor

class Pipeline():
    def __init__(self, graph, n_threads=1, quiet=False, update_callback=None, **kwargs):
        if not isinstance(graph, NodeGraph):
            raise Exception("Graph must be a node graph. Got %s" % type(graph))

        self.graph = graph
        self.update_callback = update_callback

        if n_threads == 1:
            self._executor = Executor(graph, quiet, update_callback,**kwargs)
        elif n_threads > 1:
            self._executor = ParallelExecutor(graph, n_threads, quiet, update_callback, **kwargs)
        else:
            raise Exception("n_threads must be >=1. Got %s" % n_threads)

    def run(self):
        self._executor.run()


class _Parcel(object):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return "Parcel<%s>" % str(self.data)

    def __repr__(self):
        return str(self)

class Node(ABC):
    BATCH_SIZE_ALL = float("inf")
    STATE_RUNNING = 1
    STATE_CLOSING = 2
    STATE_CLOSED = 3


    def __init__(self, name, in_streams="*", out_streams="*", **kwargs):
        """
        :param name: Name of the node
        :type name: str
        :param in_name: Name of the input stream this node should expect. If "*" is given, all inputs are accepted
        :type in_name: str or list of str
        :param out_streams: Name of the output streams
        :type out_streams: str or list of str
        :param kwargs: Extra arguments, can be used to specify stateless. All other arguments are passed to setup
        """

        override = {}
        if "batch_size" in kwargs:
            override["batch_size"] = kwargs.get("batch_size")
            kwargs.pop("batch_size")
        else:
            self.batch_size = 1

        self.name = name

        self.size = None

        if in_streams == "*":
            self.in_streams = "*"
        elif isinstance(in_streams, str):
            self.in_streams = [in_streams]
        elif isinstance(in_streams, list):
            self.in_streams = in_streams
        else:
            raise Exception("in_names must be a string or list of strings")

        if out_streams == "*":
            self.out_streams = "*"
        elif isinstance(out_streams, str):
            self.out_streams = [out_streams]
        elif isinstance(out_streams, list):
            self.out_streams = out_streams
        else:
            raise Exception("out_names must be a string or list of strings")

        self._output_buffer = []
        self.input_buffer = []
        self._state = self.STATE_RUNNING
        self.setup(**kwargs)

        for k in override:
            self.__setattr__(k, override[k])

        assert self.batch_size > 0


    def __str__(self):
        return 'Node<%s>' % (self.name)

    def __repr__(self):
        return self.__str__()

    def __or__(self, other):
        g = NodeGraph(self)
        return g | other

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def setup(self, **kwargs):
        pass

    def state_transition(self):
        if self._state == self.STATE_CLOSING:
            self._state = self.STATE_CLOSED

    def close(self):
        self._state = self.STATE_CLOSING

    def emit(self, data):
        self._output_buffer.append(_Parcel(data))

    def _run(self, data):
        if self._state != self.STATE_CLOSED:
            self.run(data)

    @abstractmethod
    def run(self, data):
        raise NotImplementedError("Child classes must override run method")

class NodeGraph(object):
    def __init__(self, root):
        self._root = root

        self._graph = {}
        self._node_list = set()

        self._graph[self._root] = set()
        self._node_list.add(self._root)
        self._last_added = self._root

    def _add_node(self, predecessor, successor):
        if predecessor not in self._graph:
            raise Exception(predecessor, "not found in graph")

        if successor in self._node_list:
            raise Exception("Cannot two instances of a node to the graph. \n%s\n%s being added twice" % (self, successor))

        if predecessor.out_streams == "*":
            pass
        elif successor.in_streams == "*":
            if len(predecessor.out_streams) == 0:
                raise Exception("%s accepts all inputs but %s does not output anything" % (successor, predecessor))
        else:
            if not set(successor.in_streams).issubset(set(predecessor.out_streams)):
                raise Exception("%s inputs should be a subset of %s outputs" % (successor, predecessor))

        self._graph[predecessor].add(successor)
        self._graph[successor] = set()
        self._node_list.add(successor)

        self._last_added = successor

    def _add_from_graph(self, predecessor, graph):
        to_add = [(predecessor, graph._root)]

        while len(to_add) > 0:
            pred, succ = to_add.pop()

            self._add_node(pred, succ)

            next_nodes = graph._graph[succ]
            for n in next_nodes:
                to_add.append((succ, n))

    def _add_from_list(self, predecessor, successors):
        for s in successors:
            self.add(predecessor, s)

    def add(self, predecessor, successors):
        if isinstance(successors, Node):
            self._add_node(predecessor, successors)
        elif isinstance(successors, NodeGraph):
            self._add_from_graph(predecessor, successors)
        elif isinstance(successors, (list, tuple)):
            self._add_from_list(predecessor, successors)
        else:
            raise Exception("Nodes must be a node or list/tuple or nodes. Got %s" % type(successors))

    def is_all_closed(self):
        for n in self._node_list:
            if n._state != Node.STATE_CLOSED:
                return False

        return True

    def __str__(self):
        result = "Graph<\n"
        for n in self:
            if len(self._graph[n]) > 0:
                result += "\t%s -> [%s]\n" %(n, ",".join([str(x) for x in self._graph[n]]))

        result += ">"
        return result

    def __repr__(self):
        return self.__str__()

    def __or__(self, other):
        self.add(self._last_added, other)
        return self

    def __eq__(self, other):
        if not isinstance(other, NodeGraph):
            return False

        if self._node_list != other._node_list:
            return False

        return self._graph == other._graph and self._root == other._root

    def __hash__(self):
        return hash(json.dumps(self._graph, sort_keys=True))

    def __iter__(self):
        to_iter = [self._root]
        while to_iter:
            to_yield = to_iter.pop()
            to_iter.extend(self._graph[to_yield])
            yield to_yield



if __name__ == '__main__':
    class DummyNode(Node):
        def run(self, data):
            pass

    n1 = DummyNode("N1")
    n2 = DummyNode("N2")
    n3 = DummyNode("N3")
    n4 = DummyNode("N4")
    n5 = DummyNode("N5")
    n6 = DummyNode("N6")
    n7 = DummyNode("N7")

    # g1 = NodeGraph(n1)
    # g1.add(n1, n2)
    # g1.add(n1, n3)
    #
    # g2 = NodeGraph(n7)
    # g2.add(n7, n6)
    #
    # print(g1)
    # print(g2)
    #
    # g1.add(n2, g2)
    #
    # print(g1)

    # g = NodeGraph(n1)
    # g.add(n1, n2)
    # g.add(n1, [n3, n4])
    #
    # g.add(n2, [n5, n6])
    #
    # print(g)

    g = n1 | [n2 | [n3, n4]]

    print(g)

    # print(n1 | n2 | [n3, [n4, n5]])