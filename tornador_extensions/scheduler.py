import math
import collections
from tornado import ioloop, process
from concurrent.futures import ThreadPoolExecutor


class SchedulerTask(object):
    def __init__(self, scheduler, handler, cycles=1, handler_args=None, handler_kwargs=None, **kwargs):
        self._handler = handler
        self._cycles = cycles
        self._handler_args = handler_args
        self._handler_kwargs = handler_kwargs
        self._skipped_cycles = 0
        self._scheduler = scheduler
        self._kwargs = kwargs

        print('cycles', self._cycles)

    def _value(self, val):
        if callable(val):
            return val(self)
        return val

    def _get_args(self):
        return self._value(self._handler_args) or ()

    def _get_kwargs(self):
        return self._value(self._handler_kwargs) or {}

    def _get_handler(self):
        return self._handler

    def __call__(self):
        if self._skipped_cycles < self._cycles - 1:
            self._skipped_cycles += 1
        else:
            self._skipped_cycles = 0
            self._get_handler()(*self._get_args(), **self._get_kwargs())


class SchedulerSubprocessTask(SchedulerTask):
    def _get_handler(self):
        def handler(*args, **kwargs):
            p = process.Subprocess(self._value(self._handler), **kwargs)
            if 'callback' in self._kwargs:
                p.set_exit_callback(self._kwargs['callback'])
        return handler


class SchedulerThreadPoolTask(SchedulerTask):
    def _get_handler(self):
        def handler(*args, **kwargs):
            executor = ThreadPoolExecutor(max_workers=1)
            future = executor.submit(self._handler, *args, **kwargs)
            if 'callback' in self._kwargs:
                future.add_done_callback(self._kwargs['callback'])
        return handler


class Scheduler(object):
    def __init__(self, interval, start_at=None, io_loop=None):
        assert interval > 0
        self.interval = interval
        self._io_loop = io_loop or ioloop.IOLoop.current()
        self._tasks = collections.OrderedDict()
        self._stub = start_at
        self._timeout = None
        self._schedule_next()

    @property
    def running(self):
        return self._timeout is not None

    @property
    def io_loop(self):
        return self._io_loop

    def stop(self):
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def add(self, name, task):
        self._tasks[name] = task

    def remove(self, name):
        self._tasks.pop(name, None)

    def _schedule_next(self):
        if self._stub is None:
            self._io_loop.add_callback(self._run_tasks)
        else:
            now = self._io_loop.time()
            print(now)
            if now < self._stub:
                callat = self._stub
            else:
                callat = (math.floor((now - self._stub) / self.interval) + 1) * self.interval + self._stub
            self._timeout = self._io_loop.call_at(callat, self._run_tasks)

    def _run_tasks(self):
        self._stub = self._io_loop.time()
        self._schedule_next()
        for task in self._tasks.values():
            task()

    def _task_deco(self, task_cls, task_name, **kwargs):
        def deco(handler):
            self._tasks[task_name or handler.__name__] = task_cls(self, handler, **kwargs)
            return handler
        return deco

    def task(self, name=None, **kwargs):
        return self._task_deco(SchedulerTask, name, **kwargs)

    def subprocess_task(self, name=None, **kwargs):
        return self._task_deco(SchedulerSubprocessTask, name, **kwargs)

    def thread_task(self, name=None, **kwargs):
        return self._task_deco(SchedulerThreadPoolTask, name, **kwargs)
