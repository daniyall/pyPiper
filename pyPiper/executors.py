import ctypes
import multiprocessing
import os
from collections import deque
from abc import ABC, abstractmethod
from functools import reduce

from multiprocessing import Pool, Manager, Queue
from multiprocessing.pool import ApplyResult, AsyncResult
from queue import Empty

STATE_RUNNING = 1
STATE_CLOSING = 2
STATE_CLOSED = 3

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

        self.progress_max = 0
        self.progress_current = 0

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

    def update_progress(self):
        if self.use_callback:
            self.update_callback(self.progress_current, self.progress_max)

    def is_finished(self):
        return self.graph.is_all_closed()

    def run(self):
        if self.update_callback is not None and self.graph._root.size is not None:
            self.use_callback = True
            self.progress_max = self.graph._root.size

        while not self.is_finished():
            self._run_root()
            self._step()

            self.update_progress()


class Executor(BaseExecutor):
    def __init__(self, graph, quiet=False, update_callback=None):
        super().__init__(graph, quiet, update_callback)
        self.queues = {}
        self.total_done = 0

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

        root.state_transition()

        root._run(None)

        for parcel in root._output_buffer:
            for successor in self.graph._graph[root]:
                self.send(root, successor, parcel)

        if len(root._output_buffer) > 0:
            self.progress_current += 1

        if len(self.graph._graph[root]) == 0:
            self.print_buffer(root._output_buffer)
        root._output_buffer.clear()


    def _step(self):
        self.total_done += 1
        for node in self.graph:
            node.state_transition()
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


class ParallelExecutor(BaseExecutor):
    def __init__(self, graph, n_threads, quiet=False, update_callback=None, max_task_queue_size=1000):
        super().__init__(graph, quiet, update_callback)

        self.n_threads = n_threads
        self.manager = Manager()

        root = self.graph._root
        self.queues = []
        self.executors = list(range(n_threads))

        self.results = [False for i in range(n_threads)]
        self.finished = [False for i in range(n_threads)]

        self.done_count = self.manager.Value(ctypes.c_int, 0, lock=True)

        self.max_task_queue_size = max_task_queue_size

        self.executor = SingleExecRunner(Executor(graph, quiet))

        for i in range(n_threads):
            self.queues.append({})

            for successor in self.graph._graph[root]:
                self.queues[i][self.get_key(root, successor)] = deque()

    def _run_root(self):
        root = self.graph._root

        if root._state == root.STATE_RUNNING:
            for i in range(self.n_threads):
                root._run(None)
        else:
            root.state_transition()

        thread = 0
        for parcel in root._output_buffer:
            for successor in self.graph._graph[root]:
                self.queues[thread][self.get_key(root, successor)].append(parcel)
            thread += 1

        if len(self.graph._graph[root]) == 0:
            self.print_buffer(root._output_buffer)
        root._output_buffer.clear()

    def is_finished(self):
        all_finished = reduce(lambda x,y: x and y, self.finished)

        return all_finished


    def finished_count(self):
        return sum([1 for f in self.finished if f])

    def almost_finished(self):
        return self.n_threads - self.finished_count() <= 1

    def _step(self):
        root = self.graph._root

        for i in range(self.n_threads):
            res = self.results[i]

            if isinstance(res, (ApplyResult, AsyncResult)):
                if self.pool._taskqueue.qsize() >= self.max_task_queue_size or res.ready() or self.almost_finished():
                    is_finished = res.get()
                    self.finished[i] = self.finished[i] | is_finished

            queues = self.queues[i]
            args = []
            for q in queues.values():
                for successor in self.graph._graph[root]:
                    if len(q) > 0:
                        parcel = q.popleft()
                        args.append((root, successor, parcel))

            res = self.pool.apply_async(self.executor.step, (root._state, self.done_count, args), error_callback=error_func)
            self.results[i] = res

        self.progress_current = self.done_count.value


    def run(self):
        self.pool = Pool(processes=self.n_threads)

        super().run()

        self.pool.close()
        self.pool.join()

        self.progress_current = self.progress_max
        self.update_progress()


class SingleExecRunner(object):
    def __init__(self, executor):
        self.executor = executor

    def step(self, root_state, done_count, args):
        for root, successor, parcel in args:
            self.executor.send(root, successor, parcel)

        self.executor._step()

        if len(args) > 0:
            done_count.value += len(args)

        if root_state == STATE_CLOSING:
            self.executor.graph._root._state = STATE_CLOSING

        return self.executor.is_finished()

    def is_finished(self):
        return self.executor.is_finished()

def error_func(value):
    print(type(value), value)
    raise value