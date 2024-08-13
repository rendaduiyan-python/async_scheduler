import asyncio
import abc
import os
import sys
import time
import traceback
import inspect
from dataclasses import dataclass, field, asdict
from typing import (
    Dict,
    ParamSpec,
    TypeVar,
    Tuple,
    Optional,
    Union,
    AsyncGenerator,
    Any)
import logging as log
from binascii import hexlify
from uuid import UUID
from datetime import datetime, timezone
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import partial
from contextlib import asynccontextmanager
from threading import Lock


class TaskState(Enum):
    NEW = 0
    SUBMITTED = 1
    SCHEDULED = 2
    STARTED = 3
    TIMED_OUT = 4
    FAILED = 5
    DONE = 6
    CANCELED = 7
    RUNNING = 8
    FINISHED = 9
    UNKONWN = 99


class TaskType(Enum):
    ASYNC = 1
    SYNC = 2


def handle_exc() -> None:
    _, _, tb = sys.exc_info()
    traceback.print_tb(tb)
    tb_info = traceback.extract_tb(tb)
    filename, line, func, text = tb_info[-1]
    log.error(f'An error occurred on line {line} in statement {text}')


@dataclass
class TaskData:
    name: str
    task_type: TaskType
    timeout: int = -1
    result: Any = None


@dataclass
class TaskTS:
    # all ts in nano seconds
    submit_ts: int = -1
    start_ts: int = -1
    timeout_ts: int = -1
    error_ts: int = -1
    done_ts: int = -1
    cancel_ts: int = -1

    @staticmethod
    def delta_seconds(start: int, end: int) -> int:
        return round((end - start) / 1e9)

    @property
    def state(self) -> TaskState:
        check_list = [
            self.submit_ts,
            self.start_ts,
            self.timeout_ts,
            self.error_ts,
            self.done_ts,
            self.cancel_ts,
        ]
        if all(ts == -1 for ts in check_list):
            return TaskState.NEW
        if all(ts == -1 for ts in check_list[1:]):
            return TaskState.SUBMITTED
        if self.start_ts != -1 and all(ts == -1 for ts in check_list[2:]):
            return TaskState.SCHEDULED
        if self.timeout_ts != -1:
            return TaskState.TIMED_OUT
        if self.error_ts != -1:
            return TaskState.FAILED
        if self.done_ts != -1:
            return TaskState.DONE
        if self.cancel_ts != -1:
            return TaskState.CANCELED

    @property
    def is_running(self) -> bool:
        return self.state == TaskState.SCHEDULED

    @property
    def is_finished(self) -> bool:
        return self.state in [
            TaskState.TIMED_OUT,
            TaskState.FAILED,
            TaskState.DONE,
            TaskState.CANCELED,
        ]

    def max_field(self) -> str:
        dTS = asdict(self)
        return sorted(dTS, key=lambda x: dTS[x])[-1]

    def display_names(self) -> dict:
        dTS = asdict(self)
        dis_names = ['submitted', 'started', 'timedout',
                     'failed', 'done', 'cancelled']
        return dict(zip(list(dTS.keys()), dis_names))


P = ParamSpec('P')
T = TypeVar('T')


class Task(abc.ABC):
    def __init__(self, name: str, task_type: TaskType,
                 timeout: int = -1) -> None:
        self._data = TaskData(name=name, task_type=task_type, timeout=timeout)        
    
    @property
    def result(self) -> Any:
        return self._data.result
    
    @abc.abstractmethod
    def run_sync(self, *args: P.args, **kwargs: P.kwargs) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    async def run_async(self, *args: P.args, **kwargs: P.kwargs) -> T:
        raise NotImplementedError

    def whoami(self) -> str:
        frame = inspect.currentframe()
        return inspect.getframeinfo(frame).function

    def start_cb(self, data: TaskTS) -> None:
        log.debug(f'{self.whoami()} is called with {data}')

    def timeout_cb(self, data: TaskTS) -> None:
        log.debug(f'{self.whoami()} is called with {data}')

    def done_cb(self, data: TaskTS, res_err: T) -> None:
        log.debug(f'{self.whoami()} is called with {data}')

    def error_cb(self, data: TaskTS, res_err: Exception) -> None:
        log.debug(f'{self.whoami()} is called with {data}')

    def cancle_cb(self, data: TaskTS) -> None:
        log.debug(f'{self.whoami()} is called with {data}')

    @property
    def data(self) -> 'TaskData':
        return self._data


class AsyncTask(Task):
    def __init__(self, name: str, timeout: int = -1) -> None:
        super().__init__(name, TaskType.ASYNC, timeout)

    def run_sync(self, *args: P.args, **kwargs: P.kwargs) -> T:
        raise RuntimeError(f'Invalid for {TaskType.ASYNC.name} tasks')

    async def run_async(self, *args: P.args, **kwargs: P.kwargs) -> T:
        pass


class SyncTask(Task):
    def __init__(self, name: str, timeout: int = -1) -> None:
        super().__init__(name, TaskType.SYNC, timeout)

    def run_sync(self, *args: P.args, **kwargs: P.kwargs) -> T:
        pass

    async def run_async(self, *args: P.args, **kwargs: P.kwargs) -> T:
        raise RuntimeError(f'Invalid for {TaskType.SYNC.name} tasks')


@dataclass
class CallerData:
    task: Task
    args: list
    kwargs: dict
    task_ts: TaskTS = field(default_factory=lambda: TaskTS())
    lock: Lock = Lock()
    real_task: asyncio.Task = None
    scheduled: bool = False


@asynccontextmanager
async def async_lock(
    ioloop: 'asyncio.AbstractEventLoop',
    executor: 'ThreadPoolExecutor',
    lock: 'asyncio.Lock',
) -> AsyncGenerator[None, None]:
    await ioloop.run_in_executor(executor, lock.acquire)
    try:
        yield
    finally:
        lock.release()


class AsyncScheduler:
    _TFMT: str = '%Y-%m-%d %H:%M:%S.%f'
    _MAX_WORKER: int = 20

    def __init__(self,
                 ioloop: Optional['asyncio.AbstractEventLoop'] = None) -> None:
        self._task_queue: asyncio.Queue = asyncio.Queue()
        self._tasks: Dict[str, Tuple[Task, list, dict, asyncio.Lock]] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=AsyncScheduler._MAX_WORKER)
        self._loop: asyncio.AbstractEventLoop = ioloop
        self._running: bool = False

    @staticmethod
    def new_uuid() -> str:
        return str(UUID(hex=hexlify(os.urandom(16)).decode()))

    @staticmethod
    def fmt_ts(ts: int) -> datetime:
        return datetime.fromtimestamp(ts / 1e9).astimezone(timezone.utc)

    @staticmethod
    def _default_done_cb(task: asyncio.Task) -> None:
        try:
            res = task.result()
            log.debug(f'task {task.get_name()} returned: {res}')
        except Exception as err:
            log.warning(f'task {task.get_name()} results in error: {err}')
            log.warning(traceback.format_exc())

    @property
    def ioloop(self) -> 'asyncio.AbstractEventLoop':
        return self._loop

    def submit_task(self, task: 'Task',
                    *args: P.args, **kwargs: P.kwargs) -> str:
        task_uuid = self.new_uuid()
        try:
            self._task_queue.put_nowait(task_uuid)
            cd = self._tasks[task_uuid] = CallerData(task, args, kwargs)
            cd.task_ts.submit_ts = time.time_ns()
            log.debug(f'current queue has {self._task_queue.qsize()} task(s)')
        except asyncio.QueueFull:
            return None

        return task_uuid

    async def task_state(self, task_uuid: str) -> TaskState:
        if task_uuid not in self._tasks:
            log.debug(f'given task uuid {task_uuid} does not exist')
            return TaskState.UNKNOWN
        cd = self._tasks[task_uuid]
        log.debug(f'task data: {cd}')
        async with async_lock(self._loop, self._executor, cd.lock):
            return cd.task_ts.state

    async def _wait_for(self, cd: CallerData, check_intv: float) -> None:
        while True:
            async with async_lock(self._loop, self._executor, cd.lock):
                if cd.task_ts.is_finished:
                    break
            await asyncio.sleep(check_intv)

    async def wait_task(
        self,
        task_uuid: str,
        check_intv: float = 0.1,
        timeout: Union[None, int, float] = None,
    ) -> bool:
        if task_uuid not in self._tasks:
            log.debug(f'given task uuid {task_uuid} does not exist')
            return
        cd = self._tasks[task_uuid]
        log.debug(f'task data: {cd}')
        to = None
        if cd.task.data.timeout > 0:
            if timeout is not None:
                to = min(cd.task.data.timeout, timeout)
            else:
                to = cd.task.data.timeout
        elif timeout is not None:
            to = timeout
        at = self._loop.create_task(
            self._wait_for(cd, check_intv), name=f'wait_task_{task_uuid}'
        )
        try:
            await asyncio.wait_for(at, to)
            return True
        except asyncio.TimeoutError:
            log.debug(f"task {task_uuid} didn't finshed in "
                      f"{timeout} second(s)")
            return False

    async def cancel_task(self, task_uuid: str) -> bool:
        if task_uuid not in self._tasks:
            log.debug(f'given task uuid {task_uuid} does not exist')
            return False
        cd = self._tasks[task_uuid]
        log.debug(f'task data: {cd}')
        async with async_lock(self._loop, self._executor, cd.lock):
            assert cd.task_ts.submit_ts != -1
            if cd.task_ts.start_ts == -1:
                log.debug(f'task {cd.task.data.name} is not yet started')
                cd.task_ts.cancel_ts = time.time_ns()
                self._callback(task_uuid, 'cancel_cb')
                return True
            if cd.task_ts.timeout_ts != -1:
                log.debug(
                    f'task {cd.task.data.name} timed out at: '
                    f'{self.fmt_ts(cd.task_ts.timeout_ts)}'
                )
                return False
            if cd.task_ts.done_ts != -1:
                log.debug(
                    f'task {cd.task.data.name} was done at: '
                    f'{self.fmt_ts(cd.task_ts.done_ts)}'
                )
                return False
            if cd.task_ts.error_ts != -1:
                log.debug(
                    f'task {cd.task.data.name} failed out at: '
                    f'{self.fmt_ts(cd.task_ts.error_ts)}'
                )
                return False
            if cd.task_ts.cancel_ts != -1:
                log.debug(
                    f'task {cd.task.data.name} was cancelled at: '
                    f'{self.fmt_ts(cd.task_ts.cancel_ts)}'
                )
                return False
            if cd.task_ts.start_ts != -1 and cd.real_task:
                if cd.real_task.done():
                    if cd.task_ts.done_ts == -1:
                        cd.task_ts.done_ts = time.time_ns()
                    log.debug(
                        f'task {cd.task.data.name} was done at: '
                        f'{self.fmt_ts(cd.task_ts.done_ts)}'
                    )
                    return False
                else:
                    cd.real_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await cd.real_task
                    cd.task_ts.cancel_ts = time.time_ns()
                    self._callback(task_uuid, 'cancel_cb')
                    return True
        return False

    def start(self) -> None:
        if not self._running:
            self._running = True
            if not self._loop:
                self._loop = asyncio.get_running_loop()
            self._task = self._loop.create_task(
                self._run(), name='async_scheduler')
            self._task.add_done_callback(self._default_done_cb)
            log.debug('AsyncScheduler started')

    async def stop(self) -> None:
        if self._running:
            log.info('stopping scheduler')
            self._running = False
            await asyncio.sleep(0.5)
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

            log.info('scheduler stopped')

    async def _run(self) -> None:
        while self._running:
            try:
                task_uuid = self._task_queue.get_nowait()
                log.debug(f'got {task_uuid} from queue')
                self._task_queue.task_done()
                await self._schedule(task_uuid)
            except asyncio.CancelledError:
                break
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)

    async def _schedule(self, task_uuid: str) -> None:
        cd = self._tasks[task_uuid]
        async with async_lock(self._loop, self._executor, cd.lock):
            if not cd.scheduled:
                task_name = f'wrapper_{cd.task.data.name}'
                log.debug(f'about to schedule task {cd.task.data.name}')
                cd.real_task = self._loop.create_task(
                    self._run_task(task_uuid), name=task_name
                )
                cd.scheduled = True

    def _callback(self, task_uuid: str, cb_name: str, *args: P.args) -> None:
        cd = self._tasks[task_uuid]
        check_cb = hasattr(cd.task, cb_name) and \
            callable(getattr(cd.task, cb_name))
        if check_cb:
            cb = getattr(cd.task, cb_name)
            log.debug(f'callback: {cb_name}, {cb}')
            cb(cd.task_ts, *args)
        else:
            log.info(f'no such callback {cb_name} or not callable')

    async def _run_task(self, task_uuid: str) -> None:
        cd = self._tasks[task_uuid]
        log.debug(f'entry: {cd.task.data.name}')
        assert cd.task_ts.cancel_ts == -1
        assert cd.task_ts.timeout_ts == -1
        assert cd.task_ts.done_ts == -1
        assert cd.task_ts.error_ts == -1
        assert cd.task_ts.start_ts == -1
        if cd.task.data.task_type == TaskType.ASYNC:
            log.debug('running async task')
            at = self._loop.create_task(
                cd.task.run_async(*cd.args, **cd.kwargs),
                name=cd.task.data.name
            )
        else:
            log.debug('running sync task in an executor')
            at = self._loop.run_in_executor(
                self._executor,
                partial(cd.task.run_sync, *cd.args, **cd.kwargs)
            )
        async with async_lock(self._loop, self._executor, cd.lock):
            cd.task_ts.start_ts = time.time_ns()

        self._callback(task_uuid, 'start_cb')
        try:
            to = cd.task.data.timeout if cd.task.data.timeout > 0 else None
            if to is not None:
                log.debug(f'scheduler: wait {cd.task.data.name} '
                          f'for {to} second(s)')
            else:
                log.debug(f'scheduler: wait {cd.task.data.name} to be done')
            await asyncio.wait_for(at, to)
            try:
                res = at.result()
            except Exception as err:
                log.debug(f'result error: {err}')
            async with async_lock(self._loop, self._executor, cd.lock):
                cd.task_ts.done_ts = time.time_ns()
            self._callback(task_uuid, 'done_cb', res)
        except asyncio.TimeoutError:
            cd.task_ts.timeout_ts = time.time_ns()
            to = (cd.task_ts.timeout_ts - cd.task_ts.start_ts) / 1e9
            log.debug(
                f'task {cd.task.data.name} timed out '
                f'after {to: .2f} second(s) at: '
                f'{self.fmt_ts(cd.task_ts.timeout_ts)}'
            )
            self._callback(task_uuid, 'timeout_cb')
        except Exception as err:
            log.warning(f'task {cd.task.data.name} failed with error: {err}')
            log.warning(traceback.format_exc())
            cd.task_ts.error_ts = time.time_ns()
            log.debug(
                f'task {cd.task.data.name} failed at: '
                f'{self.fmt_ts(cd.task_ts.error_ts)}'
            )
            self._callback(task_uuid, 'error_cb', err)
        log.debug(f'exit: {cd.task.data.name}')

    def current_task(self, ioloop: Optional['asyncio.AbstractEventLoop']
                     = None) -> str:
        if ioloop:
            task = asyncio.current_task(ioloop)
        else:
            task = asyncio.current_task(self._loop)
        if not task:
            return 'no task in the current loop'
        ss = ''
        s = 'name'
        ss += f'{s: >15} : {task.get_name()}\n'
        s = 'coro'
        ss += f'{s: >15} : {task.get_coro()}\n'
        s = 'stack'
        ss += f'{s: >15} : {task.get_stack()}\n'
        return ss

    def task_info(self, task_uuid: str) -> Tuple[Task, list, dict]:
        if task_uuid in self._tasks:
            cd = self._tasks[task_uuid]
            return cd.task, cd.args, cd.kwargs
        else:
            raise RuntimeError(f'no such task: {task_uuid}')

    async def dump_task(self, task_uuid: str = 'all') -> str:
        cds = []
        if task_uuid == 'all':
            cds = [cd for cd in self._tasks.values()]
        else:
            cds = [self._tasks[task_uuid]]

        ss = ''
        s = 'Name'
        ss += f'{s: >20}'
        s = 'State'
        ss += f'{s: >40}'
        s = 'Time'
        ss += f'{s: >60}'
        ss += '\n'
        for cd in cds:
            ss += f'{cd.task.data.name: >20}'
            async with async_lock(self._loop, self._executor, cd.lock):
                max_ts = cd.task_ts.max_field()
                log.debug(f'task {cd.task.data.name} has max field: {max_ts}')
                if max_ts == 'start_ts':
                    s = 'running'
                    ts = time.time_ns()
                elif max_ts == 'submit_ts':
                    s = 'queue'
                    ts = 0
                else:
                    s = cd.task_ts.display_names()[max_ts]
                    ts = getattr(cd.task_ts, max_ts)
                ss += f'{s: >40}'
                if ts > 0:
                    s = TaskTS.delta_seconds(cd.task_ts.start_ts, ts)
                else:
                    s = 0
                ss += f'{s: >60}'
            ss += '\n'

        return ss

    async def slowest(self) -> str:
        stats = {}
        for uuid, cd in self._tasks.items():
            if cd.task_ts.start_ts == -1:
                continue
            max_ts = cd.task_ts.max_field()
            if max_ts != 'start_ts':
                ts = getattr(cd.task_ts, max_ts)
                d = TaskTS.delta_seconds(cd.task_ts.start_ts, ts)
            else:
                d = TaskTS.delta_seconds(cd.task_ts.start_ts, time.time_ns())
            stats.update({uuid: d})
        if stats:
            candidate = sorted(stats, key=lambda x: stats[x])[-1]
            return await self.dump_task(candidate)
        else:
            return ''
