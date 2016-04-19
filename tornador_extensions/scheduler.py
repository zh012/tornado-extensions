import re
import sys
import traceback
import os
import imp
import json
from collections import OrderedDict
import time
import math
from tornado import ioloop, process
from concurrent.futures import ThreadPoolExecutor


########################################
# task scheduler, replace cron or monit
########################################
# basic task class
class SkipCycle(Exception):
    pass


class SchedulerTask(object):
    def __init__(self, scheduler, handler, cycles=1, handler_args=None, handler_kwargs=None, **kwargs):
        self._handler = handler
        self._cycles = cycles
        self._handler_args = handler_args
        self._handler_kwargs = handler_kwargs
        self._skipped_cycles = 0
        self._scheduler = scheduler
        self._kwargs = kwargs

    def _value(self, val):
        if callable(val):
            return val(self)
        return val

    def _get_handler_args(self):
        return self._value(self._handler_args) or ()

    def _get_handler_kwargs(self):
        return self._value(self._handler_kwargs) or {}

    def _get_handler(self):
        return self._handler

    def _run(self):
        return self._get_handler()(*self._get_handler_args(), **self._get_handler_kwargs())

    def get_conf(self, key, default=None):
        return self._kwargs.get(key, default)

    def __call__(self):
        if self._skipped_cycles < self._cycles - 1:
            self._skipped_cycles += 1
            raise SkipCycle()
        else:
            self._skipped_cycles = 0
            return self._run()


# task running in subprocess
class SchedulerSubprocessTask(SchedulerTask):
    def _get_handler(self):
        def handler(*args, **kwargs):
            p = process.Subprocess(self._value(self._handler), **kwargs)
            if 'callback' in self._kwargs:
                p.set_exit_callback(self._kwargs['callback'])
        return handler


# task running in thread
class SchedulerThreadPoolTask(SchedulerTask):
    def _get_handler(self):
        def handler(*args, **kwargs):
            executor = ThreadPoolExecutor(max_workers=1)
            future = executor.submit(self._handler, *args, **kwargs)
            if 'callback' in self._kwargs:
                future.add_done_callback(self._kwargs['callback'])
        return handler


# task scheduler
class Scheduler(object):
    def __init__(self, interval, start_at=0., io_loop=None, logfile=''):
        assert interval > 0
        self.interval = interval
        self._io_loop = io_loop or ioloop.IOLoop.current()
        self._tasks = OrderedDict()
        self._stub = start_at
        self._timeout = None
        self._schedule_next()
        self._logfile = logfile

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

    def log(self, msg):
        if not self._logfile:
            print(msg)
        else:
            open(self._logfile, 'a').write(msg + '\n')

    def _schedule_next(self):
        if self._stub is None:
            self._io_loop.add_callback(self._run_tasks)
        else:
            now = self._io_loop.time()
            if now < self._stub:
                callat = self._stub
            else:
                callat = (math.floor((now - self._stub) / self.interval) + 1) * self.interval + self._stub
            self._timeout = self._io_loop.call_at(callat, self._run_tasks)

    def _run_tasks(self):
        scheduled_at = time.time()
        if self._stub is None:
            self._stub = self._io_loop.time()
        self._schedule_next()

        for name, task in self._tasks.items():
            logdata = {
                'scheduled_at': scheduled_at,
                'started_at': time.time(),
                'task_failed': False,
                'task_name': name,
                'task_file': task.get_conf('location'),
            }

            try:
                ret = task()
            except SkipCycle:
                continue
            except:
                ret = traceback.format_exc()
                logdata['task_failed'] = True

            logdata['used_time'] = time.time() - logdata['started_at']

            try:
                json.dumps(ret)
                logdata['task_data'] = ret
            except:
                logdata['task_data'] = str(ret)

            self.log(json.dumps(logdata))

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


# task decorator
def task(name, **kwargs):
    def deco(func):
        if not hasattr(func, '__schedule__'):
            func.__schedule__ = {}
        func.__schedule__[name] = kwargs
        return func
    return deco


# load task to scheduler
def register_task(scheduler, taskfunc):
    schedule = getattr(taskfunc, '__schedule__', None)
    if schedule:
        for name, params in schedule.items():
            params_copy = params.copy()
            task_cls = params_copy.pop('task_cls', None) or SchedulerTask
            params_copy.setdefault('cycles', 1)
            params_copy['location'] = getattr(taskfunc, '__location__', None)
            scheduler._task_deco(task_cls, name, **params_copy)(taskfunc)


# load tasks
def load_tasks(path):

    def _load_task(pa, ta):
        if os.path.isfile(pa) or (os.path.isdir(pa) and os.path.isfile(os.path.join(pa, '__init__.py'))):
            mod = imp.load_source('_tmp_task_module', pa)
            for name in dir(mod):
                task = getattr(mod, name)
                if callable(task) and hasattr(task, '__schedule__'):
                    task.__location__ = pa
                    ta.append(task)
        elif os.path.isdir(pa):
            for fn in os.listdir(pa):
                if re.match('^task_[^.]+(\.py)?$', fn):
                    _load_task(os.path.join(pa, fn), ta)
        else:
            raise RuntimeError('Failed to load task from: {}'.format(pa))

    tasks = []
    _load_task(path, tasks)
    return tasks


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', default=1, help='The interval of one tick. Unit is second.')
    parser.add_argument('--task', action='append', help='The path where the tasks definition located. If folder is provided, all the python files starts with "task_" under the folder will be loaded.')
    parser.add_argument('--logfile', default='', help='Log file path. If not provided, the logs are printed to stdout.')
    args = parser.parse_args()

    if not args.task:
        parser.print_help()
        sys.exit(1)

    scheduler = Scheduler(args.interval, logfile=args.logfile)

    for path in args.task:
        for t in load_tasks(path):
            register_task(scheduler, t)

    try:
        ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        ioloop.IOLoop.current().stop()
