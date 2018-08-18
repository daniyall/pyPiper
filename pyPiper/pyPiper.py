from abc import ABC, abstractmethod
import json

class Node(object):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return 'Node<%s>' % (self.name)

    def __repr__(self):
        return self.__str__()

    def __or__(self, other):
        g = NodeGraph(self)
        g.add(self, other)
        return g

    def __ror__(self, other):
        raise NotImplementedError()

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


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

    def __ror__(self, other):
        raise NotImplementedError()

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