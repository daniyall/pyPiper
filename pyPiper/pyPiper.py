from abc import ABC, abstractmethod
import json
from collections import defaultdict, deque

class Pipeline():
    def __init__(self, graph, n_threads=1, quiet=False):
        if not isinstance(graph, NodeGraph):
            raise Exception("Graph must be a node graph. Got %s" % type(graph))

        self.graph = graph

        if n_threads == 1:
            self._executor = Executor(self.graph, quiet)
        elif n_threads > 1:
            self._executor = ParallelExecutor(quiet)
        else:
            raise Exception("n_threads must be >=1. Got %s" % n_threads)

    def run(self):
        self._executor.run()


def _filter_data_stream(node, next_node, data):
    to_push = []
    if next_node.in_streams == "*":
        to_push = data
    else:
        if node.out_streams == "*":
            if len(data) != len(next_node.in_streams):
                raise Exception(
                    "Node %s emits %i items, but next node (%s) expects %i" % (node, len(data), node, node.in_streams))
            to_push = data
        else:
            for k in node.in_streams:
                to_push.append(data[node.out_streams.index(k)])

    if isinstance(to_push, list) and len(to_push) == 1:
        to_push = to_push[0]

    return to_push

def is_all_closed(graph):
    for n in graph._node_list:
        if n._state != Node.STATE_CLOSED:
            return False

    return True


class Executor():
    def __init__(self, graph, quiet=False):
        self.graph = graph
        self.quiet = quiet

        self.queues = {}
        for node in graph._node_list:
            for successor in graph._graph[node]:
                self.queues[self.get_key(node, successor)] = deque()

    def send(self, node, successor, data):
        self.queues[self.get_key(node, successor)].append(data)

    def get_data_to_push(self, node, successor):
        queue = self.queues[self.get_key(node, successor)]

        if node._state != Node.STATE_CLOSED:
            size = successor.batch_size
        else:
            size = len(queue)

        if len(queue) >= size:
            return [queue.popleft() for x in range(size)]

        return None

    def print_buffer(self, buffer):
        if not self.quiet and buffer:
            if len(buffer) == 1:
                print(buffer[0])
            else:
                print(buffer)

    @staticmethod
    def get_key(node, successor):
        return "%s%s" % (node, successor)

    def _run_root(self):
        root = self.graph._root

        if root._state == Node.STATE_RUNNING:
            root._run(None)
        else:
            root.close()

        for d in root._output_buffer:
            for successor in self.graph._graph[root]:
                self.send(root, successor, d)
        root._output_buffer.clear()


    def _step(self):
        for node in self.graph:
            successors = self.graph._graph[node]

            for successor in successors:
                data = self.get_data_to_push(node, successor)

                if data:
                    data = _filter_data_stream(node, successor, data)
                    successor._run(data)

                for d in successor._output_buffer:
                    super_successors = self.graph._graph[successor]
                    for ss in super_successors:
                        self.send(successor, ss, d)

                    if len(super_successors) == 0:
                        self.print_buffer(successor._output_buffer)
                    successor._output_buffer.clear()


                if node._state != Node.STATE_RUNNING:
                    successor.close()


    def run(self):
        while not is_all_closed(self.graph):
            self._run_root()
            self._step()


class ParallelExecutor():
    def __init__(self, quiet):
        self.quiet = quiet

    def run(self, graph):
        raise NotImplementedError()


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
        g.add(self, other)
        return g

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def setup(self, **kwargs):
        pass

    def close(self):
        if self._state == self.STATE_RUNNING:
            self._state = self.STATE_CLOSING
        elif self._state == self.STATE_CLOSING:
            self._state = self.STATE_CLOSED

    def emit(self, data):
        if not isinstance(data, list):
            data = [data]

        self._output_buffer.extend(data)

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