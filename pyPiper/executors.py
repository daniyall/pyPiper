import ctypes
import multiprocessing
import os
import queue
import time
from collections import deque
from abc import ABC, abstractmethod
from functools import reduce
from itertools import islice

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

    return to_push


class BaseExecutor(ABC):
    def __init__(self, graph, quiet=False):
        self.graph = graph
        self.quiet = quiet

        self.update_callback = None
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

    def _init_update(self, update_callback):
        self.update_callback = update_callback
        if self.update_callback is not None and hasattr(self.graph._root, "size"):
            self.use_callback = True
            self.progress_max = self.graph._root.size
            self.update_progress()

    def run(self, update_callback=None):
        self._init_update(update_callback)

        while not self.is_finished():
            self._run_root()
            self._step()

            self.update_progress()


class Executor(BaseExecutor):
    def __init__(self, graph, quiet=False):
        super().__init__(graph, quiet)
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

                    if successor.batch_size == 1:
                        for d in data:
                            successor._run(d)
                    elif successor.batch_size == float("inf"):
                        successor._run(data)
                    else:
                        it = iter(data)
                        while True:
                            d = tuple(islice(it, successor.batch_size))
                            if not d:
                                break
                            successor._run(d)

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
    def __init__(self, graph, n_threads, quiet=False):
        super().__init__(graph, quiet)

        self.n_threads = n_threads
        self.manager = Manager()

        self.queues = [[] for i in range(n_threads)]
        self.executors = list(range(n_threads))

        self.done_counter = self.manager.Value(ctypes.c_int, 0, lock=False)
        self.counter_lock = self.manager.Lock()

        self.executor = SingleExecRunner(Executor(graph, quiet))

        self._last_res = None

        self.root_closed = False

        self._curr_thread = 0

    def _run_root(self):
        root = self.graph._root

        if root._state == root.STATE_RUNNING:
            for i in range(self.n_threads):
                root._run(None)
        elif root._state == root.STATE_CLOSED:
            self.root_closed = True
        else:
            root.state_transition()

        for parcel in root._output_buffer:
            self.queues[self._curr_thread].append(parcel)

            self._curr_thread += 1
            if self._curr_thread == self.n_threads:
                self._curr_thread = 0

        if len(self.graph._graph[root]) == 0:
            self.print_buffer(root._output_buffer)

        root._output_buffer.clear()

    def is_finished(self):
        if self._last_res and not self._last_res.ready():
            return False

        return self.root_closed and self.pool._taskqueue.empty()

    def _step(self):
        root = self.graph._root

        args = []
        for i in range(self.n_threads):
            q = self.queues[i]

            if len(q) > 0:
                arg = root._state, self.done_counter, self.counter_lock, []
                while len(q) > 0:
                    parcel = q.pop()
                    arg[-1].append(parcel)

                args.append(arg)

        self._last_res = self.pool.starmap_async(self.executor.step, args, error_callback=error_func)

        self.progress_current = self.done_counter.value
        self.update_progress()


    def run(self, update_callback=None):
        self.pool = Pool(processes=self.n_threads)

        super().run(update_callback)

        self.pool.close()
        self.pool.join()

        self.progress_current = self.done_counter.value
        self.update_progress()



class SingleExecRunner(object):
    def __init__(self, executor):
        self.executor = executor
        self.root = executor.graph._root

    def step(self, root_state, done_counter, counter_lock, parcels):
        if parcels:
            for parcel in parcels:
                for successor in self.executor.graph._graph[self.root]:
                    self.executor.send(self.root, successor, parcel)

        self.executor._step()

        if parcels:
            counter_lock.acquire()
            done_counter.value += len(parcels)
            counter_lock.release()

        if root_state == STATE_CLOSING:
            self.executor.graph._root._state = STATE_CLOSING

    def is_finished(self):
        return self.executor.is_finished()

def error_func(value):
    print(type(value), value)
    raise value



def _child_run(queue: multiprocessing.Queue, graph, done_count, quiet):
    executor = Executor(graph, quiet=quiet)
    root = graph._root

    while not executor.is_finished():
        if not queue.empty():
            parcel = queue.get()
            if parcel == "close":
                root.close()
                continue
        else:
            parcel = None

        root.state_transition()

        if parcel:
            for successor in graph._graph[root]:
                executor.send(root, successor, parcel)
            with done_count.get_lock():
                done_count.value += 1

        executor._step()


class ParallelExecutor2(BaseExecutor):
    MAX_QUEUE_SIZE = 100
    def __init__(self, graph, n_threads, quiet=False):
        super().__init__(graph, quiet)
        self.n_threads = n_threads

    def _run_root(self):
        raise Exception("ParallelExecutor2 does not use _run_root or _step. These should not be called")

    def _step(self):
        raise Exception("ParallelExecutor2 does not use _run_root or _step. These should not be called")

    def do_update(self, children):
        total_done = 0
        for i in range(self.n_threads):
            total_done += children[i]["count"].value
        self.progress_current = total_done
        self.update_progress()

    def run(self, update_callback=None):
        self._init_update(update_callback)

        root = self.graph._root

        children = []
        for i in range(self.n_threads):
            q = multiprocessing.Queue(ParallelExecutor2.MAX_QUEUE_SIZE)
            count = multiprocessing.Value(ctypes.c_int, 0, lock=True)
            p = multiprocessing.Process(target=_child_run, args=(q, self.graph, count, self.quiet))
            children.append({"process": p, "queue": q, "count": count})
            children[i]["process"].start()

        t = 0
        while root._state != STATE_CLOSED:
            root.state_transition()
            root._run(None)

            if len(root._output_buffer) > 0:
                for parcel in root._output_buffer:
                    added = False
                    while not added:
                        q = children[t]["queue"]

                        try:
                            q.put(parcel, timeout=1)
                            added = True
                        except queue.Full:
                            pass

                        t += 1
                        if t == self.n_threads:
                            t = 0
            else:
                time.sleep(1)

            self.do_update(children)

            root._output_buffer.clear()


        for i in range(self.n_threads):
            children[i]["queue"].put("close")

        all_done = False
        while not all_done:
            all_done = True

            for i in range(self.n_threads):
                children[i]["process"].join(timeout=1)
                if children[i]["process"].exitcode is None:
                    all_done = False

            self.do_update(children)


