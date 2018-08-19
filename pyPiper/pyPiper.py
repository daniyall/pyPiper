from abc import ABC, abstractmethod
import json
from collections import defaultdict, deque

class Pipeline():
    def __init__(self, graph, n_threads=1, quiet=False):
        if not isinstance(graph, NodeGraph):
            raise Exception("Graph must be a node graph. Got %s" % type(graph))

        self.graph = graph

        if n_threads == 1:
            self._executor = Executor(quiet)
        elif n_threads > 1:
            self._executor = ParallelExecutor(quiet)
        else:
            raise Exception("n_threads must be >=1. Got %s" % n_threads)

    def run(self):
        self._executor.run(self.graph)


def _get_push_data(node, next_node, data):
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
    def __init__(self, quiet=False, input_queue=deque()):
        self.input_queue = input_queue
        self.quiet = quiet


    def _run_root(self, root):
        root._run(None)
        print(root._output_buffer)

        if len(root._output_buffer) >= root.batch_size:
            print(len(root._output_buffer), root.batch_size, Node.BATCH_SIZE_ALL)
            self.input_queue.append(root._output_buffer.copy())
            root._output_buffer = []


    def _step(self, graph):
        root = graph._root

        if len(self.input_queue) > 0:
            data = self.input_queue.popleft()

            nodes_to_traverse = [(root, n, data) for n in graph._graph[root]]

            while len(nodes_to_traverse) > 0:
                src_node, node, data = nodes_to_traverse.pop()

                # TODO: Switch to input buffers so that batch size specified by the dest node is used.

                incoming_data = _get_push_data(src_node, node, data)

                node.run(incoming_data)

                if len(node._output_buffer) >= node.batch_size or node._state == Node.STATE_CLOSING:
                    outgoing_data = node._output_buffer.copy()
                    if len(outgoing_data) == 1:
                        outgoing_data = outgoing_data[0]

                    if len(graph._graph[node]) == 0 and not self.quiet:
                        print(outgoing_data)

                    nodes_to_traverse.extend([(node, x, outgoing_data) for x in graph._graph[node]])
                    node._output_buffer = []


        nodes_to_traverse = [root]
        while len(nodes_to_traverse) > 0:
            node = nodes_to_traverse.pop()

            nodes_to_traverse.extend(graph._graph[node])

            if node._state == Node.STATE_CLOSING:
                for n in graph._graph[node]:
                    n.close()
                node._state = Node.STATE_CLOSED


    def run(self, graph):
        root = graph._root

        while not is_all_closed(graph):
            self._run_root(root)
            self._step(graph)


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

        print("END CONS", self, self.batch_size)
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
        self._state = self.STATE_CLOSING

    def emit(self, data):
        if not isinstance(data, list):
            data = [data]

        self._output_buffer.extend(data)

    def _run(self, data):
        if self._state == self.STATE_RUNNING:
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
        to_print = [self._root]

        result = "Graph<\n"
        while len(to_print) > 0:
            n = to_print.pop()
            if len(self._graph[n]) > 0:
                result += "\t%s -> [%s]\n" %(n, ",".join([str(x) for x in self._graph[n]]))

            to_print.extend(self._graph[n])

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


if __name__ == '__main__':
    n1 = Node("N1")
    n2 = Node("N2")
    n3 = Node("N3")
    n4 = Node("N4")
    n5 = Node("N5")
    n6 = Node("N6")
    n7 = Node("N7")

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