from collections import deque
from abc import ABC, abstractmethod

def _filter_data_stream(node, next_node, parcels):
    to_push = []

    for parcel in parcels:
        data = parcel.data

        if next_node.in_streams == "*":
            to_push.append(data)
        else:
            if not isinstance(data, (list, tuple)):
                data = [data]

            if node.out_streams == "*":
                if len(data) != len(next_node.in_streams):
                    raise Exception(
                        "Node %s emits %i items, but next node (%s) expects %i" % (node, len(data), node, node.in_streams))
                to_push = data
            else:
                for k in next_node.in_streams:
                    to_push.append(data[node.out_streams.index(k)])

    if len(to_push) == 1:
        to_push = to_push[0]

    return to_push


class BaseExecutor(ABC):
    def __init__(self, graph, quiet=False, update_callback=None):
        self.graph = graph
        self.quiet = quiet
        self.update_callback = update_callback
        self.use_callback = False

    def print_buffer(self, buffer):
        if not self.quiet and buffer:
            for parcel in buffer:
                print(parcel.data)

    @staticmethod
    def get_key(node, successor):
        return "%s%s" % (node, successor)

    @abstractmethod
    def _run_root(self):
        pass

    @abstractmethod
    def _step(self):
        pass

    def run(self):
        if self.update_callback is not None and self.graph._root.size is not None:
            self.use_callback = True
            self.total_size = self.graph._root.size

        while not self.graph.is_all_closed():
            self._run_root()
            self._step()



class Executor(BaseExecutor):
    def __init__(self, graph, quiet=False, update_callback=None):
        super().__init__(graph, quiet, update_callback)

        self.queues = {}
        for node in graph._node_list:
            for successor in graph._graph[node]:
                self.queues[self.get_key(node, successor)] = deque()

    def send(self, node, successor, data):
        self.queues[self.get_key(node, successor)].append(data)

    def get_data_to_push(self, node, successor):
        queue = self.queues[self.get_key(node, successor)]

        if node._state != node.STATE_CLOSED:
            size = successor.batch_size
        else:
            size = len(queue)

        if len(queue) >= size:
            return [queue.popleft() for x in range(size)]

        return None

    def _run_root(self):
        root = self.graph._root

        if root._state == root.STATE_RUNNING:
            root._run(None)
            if self.use_callback:
                self.update_callback(1, self.total_size)

        else:
            root.close()

        for parcel in root._output_buffer:
            for successor in self.graph._graph[root]:
                self.send(root, successor, parcel)
        if len(self.graph._graph[root]) == 0:
            self.print_buffer(root._output_buffer)
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


                if node._state != node.STATE_RUNNING:
                    successor.close()


class ParallelExecutor():
    def __init__(self, graph, quiet=False, p_bar=None):
        self.graph = graph
        self.quiet = quiet
        self.p_bar = p_bar

    def run(self):
        raise NotImplementedError()