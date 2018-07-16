from collections import defaultdict
from abc import ABC, abstractmethod
import time

import ctypes
from multiprocessing import Pool, Manager

class Pipeline():
    def __init__(self, start, n_threads=1, quiet=False):
        if isinstance(start, Node):
            self.start = [start]
        elif type(start) == list:
            self.start = start
        else:
            raise Exception("Start node is of wrong type")

        self._nodes = self.get_nodes(self.start)

        self.quiet = quiet
        self.n_threads = n_threads
        self.proc_num = 0

        self._is_running = True

        for node in self._nodes:
            node._set_pipeline(self)

        if self.n_threads > 1:
            manager = Manager()
            self.queue = manager.Queue()

            self.node_map = {}
            for node in self._nodes:

                shared_state = None
                lock = None
                running = manager.Value(ctypes.c_bool, True, lock=False)

                if not node.stateless:
                    node_state = node._get_state()

                    lock = manager.Lock()
                    shared_state = manager.dict()
                    for k, v in node_state.items():
                        shared_state[k] = v

                self.node_map[node.name] = [node, shared_state, running, lock]

        self.running = True

    def get_nodes(self, start_set):
        res = start_set.copy()
        for s in start_set:
            res  += self.get_nodes(s._next)
        return res

    def _generate_tasks(self):
        def is_running(s):
            if self.n_threads == 1:
                return s._running
            else:
                return self.node_map[s.name][2].value

        def all_nodes_closed():
            for n in self._nodes:
                if is_running(n):
                    return False

            return True

        while True:
            running = False
            for s in self.start:
                if is_running(s):
                    s._step()
                    running = True

            if not running:
                break


        while not all_nodes_closed():
            time.sleep(0.1)

        self._close()

    def _close(self):
        self.running = False

        if self.n_threads > 1:
            for i in range(self.n_threads):
                self.queue.put(None)

    def _execute_tasks(self, proc_num):
        self.proc_num = proc_num

        while True:
            try:
                item = self.queue.get()
            except:
                continue

            # None means processing is complete
            if item is None:
                return

            # Len 3 messages are close messages
            if len(item) == 3:
                node_name = item[0]

                if not self.node_map[node_name][2].value:
                    continue

                node = self.node_map[node_name][0]
                node.close()
                if self.node_map[node_name][2].value:
                    self.node_map[node_name][2].set(False)

            else:
                node_name, data = item
                node, state, running, lock = self.node_map[node_name]

                if state is not None:
                    lock.acquire()

                node._run(data, state)

                if state is not None:
                    for k, v in node._get_state().items():
                        state[k] = v

                    lock.release()

    def run(self):
        if self.n_threads == 1:
            self._generate_tasks()
        else:
            pool = Pool(processes=self.n_threads)

            def func(value):
                print(type(value), value)
                raise value

            for i in range(self.n_threads):
                pool.apply_async(self._execute_tasks, (i+1,), error_callback=func)

            self._generate_tasks()

            pool.close()
            pool.join()


    def _send_closing(self, frm, to):
        if self.n_threads == 1:
            to.close()
        else:
            if frm in self.start:
                self.node_map[frm.name][2].set(False)

            if self.node_map[to.name][2].value:
                for i in range(self.n_threads):
                    self.queue.put((to.name, "CLOSE", True)) # Put boolean to make close messages different from tasks

    def submit_task(self, node, data):
        if self.n_threads == 1:
            node._run(data=data)
        else:
            self.queue.put((node.name, data))


    def __str__(self):
        return "\n".join([s.graph_to_str() for s in self.start])

    def __repr__(self):
        return self.__str__()

_exclude_from_state = ["batch_size", "name", "_next", "pipeline", "_next_buffers", "stateless"]

class Node(ABC):
    BATCH_SIZE_ALL = -1

    def __init__(self, name, in_streams="*", out_streams="*", **kwargs):
        """

        :param name: Name of the node
        :type name: str

        :param in_name: Name of the input stream this node should expect. If "*" is given, all inputs are accepted
        :type in_name: str or list of str

        :param out_streams: Name of the output streams
        :type out_streams: str or list of str

        :param kwargs: Extra arguments, can be used to specify stateless. All other arguements are passed to setup
        """
        override = {}
        if "batch_size" in kwargs:
            override["batch_size"] = kwargs.get("batch_size")
            kwargs.pop("batch_size")
        else:
            self.batch_size = 1

        self.stateless = True

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


        self._next = []
        self._next_buffers = defaultdict(list)
        self._running = True
        self.setup(**kwargs)

        for k in override:
            self.__setattr__(k, override[k])


    def setup(self, **kwargs):
        pass

    def __str__(self):
        return 'Node<%s ({%s} to {%s})>' % (self.name, ",".join(self.in_streams), ",".join(self.out_streams))

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __lt__(self, other):
        return self.name < other.name

    def graph_to_str(self):
        if len(self._next) == 1:
            return "%s -> %s" %  (str(self), ", ".join([n.graph_to_str() for n in self._next]))
        elif len(self._next) > 1:
            return "%s -> [%s]" %  (str(self), ", ".join([n.graph_to_str() for n in self._next]))
        else:
            return "%s" % str(self)

    def _add_next(self, n):
        # No next nodes have been added. Meaning n is immediately after self
        if len(self._next) == 0:
            if isinstance(n, Node):
                n = [n]

            for _n in n:
                if not isinstance(_n, Node):
                    raise Exception("Can only have node types in pipeline")

                if self.out_streams == "*":
                    pass
                elif _n.in_streams == "*":
                    if len(self.out_streams) == 0:
                        print(self.out_streams)
                        raise Exception("%s accepts all inputs but %s does not output anything" % (_n, self))
                else:
                    if not set(_n.in_streams).issubset(set(self.out_streams)):
                        raise Exception("%s inputs should be a subset of %s outputs" % (_n, self))

                self._next.append(_n)

        else:
            for _n in self._next:
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
        for n in self._next:
            if n.batch_size > 1 or n.batch_size == self.BATCH_SIZE_ALL:
                self._push_buffer(n, force=True)

        for n in self._next:
            self.pipeline._send_closing(self, n)

        self._running = False

    def _get_next_buffer(self, to):
        return self._next_buffers[str(to)]

    def _push_buffer(self, to, force=False):
        next_buffer = self._get_next_buffer(to)

        if (len(next_buffer) >= to.batch_size and to.batch_size != Node.BATCH_SIZE_ALL) or force:
            to._step(next_buffer)
            next_buffer.clear()

    def emit(self, data):
        """Pushes emitted data to all next nodes. Data will be buffered if depending on the batch size
        specified by the next node. If a terminal node emits data, it will be printed"""

        if len(self._next) == 0 and not self.pipeline.quiet:
            print(data)

        if not isinstance(data, list):
            data = [data]

        for n in self._next:
            to_push = []
            if n.in_streams == "*":
                to_push = data
            else:
                if self.out_streams == "*":
                    if len(data) != len(n.in_streams):
                        raise Exception("Node %s emits %i items, but next node (%s) expects %i" % (self, len(data), n, len(n.in_streams)))
                    to_push = data
                else:
                    for k in n.in_streams:
                        to_push.append(data[self.out_streams.index(k)])


            if len(to_push) == 1:
                to_push = to_push[0]

            # if batch size is 1, don't bother saving to buffer
            if n.batch_size == 1:
                n._step(to_push)
            else:
                self._push_buffer(n)
                self._get_next_buffer(n).append(to_push)

    def _get_state(self):
        state = self.__dict__.copy()
        for k in _exclude_from_state:
            state.pop(k)
        return state


    def _step(self, data=None):
        self.pipeline.submit_task(node=self, data=data)

    def _run(self, data, state=None):
        if state is not None:
            for k, v in state.items():
                setattr(self, k, v)

        if not self._running:
            return

        self.run(data)

    @abstractmethod
    def run(self, data):
        raise NotImplementedError("Child classes must override run method")


if __name__ == '__main__':
    from example import *

    gen = Generate("gen", size=10)
    gen1 = Generate("gen1", size=10, reverse=True)
    sq = Square("sq")
    half = Half("half")
    pr = Printer("print", batch_size=1)
    pr1 = Printer("print all")

    p = Pipeline(gen | sq | pr, n_threads=2)
    print(p)

    p.run()
