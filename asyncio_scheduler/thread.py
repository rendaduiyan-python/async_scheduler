import asyncio

from typing import ParamSpec, TypeVar, Tuple, Coroutine, Callable, Union, Any
from threading import current_thread, Thread, main_thread
from functools import partial
from .scheduler import AsyncScheduler, Task, handle_exc
import logging as log

P = ParamSpec('P')
T = TypeVar('T')


class SchedulerThread(Thread):
    def __init__(
        self, name: str,
        caller_loop: 'asyncio.AbstractEventLoop',
        max_worker: int = 20
    ) -> None:
        super().__init__(name=name)
        assert caller_loop
        self._caller_loop: 'asyncio.AbstractEventLoop' = caller_loop
        self._loop: 'asyncio.AbstractEventLoop' = asyncio.new_event_loop()
        AsyncScheduler._MAX_WORKER = max_worker
        self._scheduler: 'AsyncScheduler' = AsyncScheduler(self._loop)
        if caller_loop is self._loop:
            log.error(
                'event loop for caller is exactly the same for current '
                'thread; please use AsyncScheduler instead'
            )
        assert caller_loop is not self._loop

    @property
    def ioloop(self) -> 'asyncio.AbstractEventLoop':
        return self._loop

    @property
    def scheduler(self) -> 'AsyncScheduler':
        return self._scheduler

    def run(self) -> None:
        log.info('scheduler thread started')
        assert current_thread() is not main_thread()
        try:
            asyncio.events.set_event_loop(self._loop)
            self._loop.call_soon(self._scheduler.start)
            self._loop.run_forever()
        finally:
            self.at_exit()
        log.info('scheduler thread quit')

    def at_exit(self) -> None:
        log.info('exiting...')
        self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        self._loop.stop()

    async def stop(self) -> None:
        log.info('scheduler thread stopping')
        await self._execute(1, self._scheduler.stop)
        self._loop.call_soon_threadsafe(self._loop.stop)

    def _task_in_another_thread(
        self,
        timeout: Union[float, int, None],
        coro: Callable[P, Coroutine],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Any:
        fut = asyncio.run_coroutine_threadsafe(
            coro(*args, **kwargs), self._loop)
        result = None
        try:
            result = fut.result(timeout)
        except TimeoutError:
            log.warning(f'the coroutine took longer than {timeout}'
                        f', cancelling ...')
            fut.cancel()
        except AssertionError:
            handle_exc()
        except Exception as err:
            log.warning(f'threadsafe failure: {err!r}')
        else:
            log.debug(f'the coroutine returned: \n{result}')
        return result

    async def _execute(
        self,
        timeout: Union[float, int, None],
        coro: Callable[P, Coroutine],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Any:
        return await self._caller_loop.run_in_executor(
            self._scheduler._executor,
            partial(self._task_in_another_thread, timeout,
                    coro, *args, **kwargs),
        )

    def submit_task(self, task: 'Task',
                    *args: P.args, **kwargs: P.kwargs) -> str:
        log.info('submitting a task')
        return self._scheduler.submit_task(task, *args, **kwargs)

    async def wait_task(
        self,
        task_uuid: str,
        check_intv: float = 0.1,
        timeout: Union[float, int, None] = None,
    ) -> None:
        log.info('waiting a task')
        return await self._execute(
            timeout + 0.1, self._scheduler.wait_task,
            task_uuid, check_intv, timeout
        )

    async def cancel_task(
        self, timeout: Union[float, int, None], task_uuid: str
    ) -> bool:
        log.info('canceling a task')
        return await self._execute(
            timeout, self._scheduler.cancel_task, task_uuid)

    def current_task(self, timeout: Union[float, int, None] = None) -> str:
        return self._scheduler.current_task(self.ioloop)

    def task_info(self, task_uuid: str) -> Tuple[Task, list, dict]:
        return self._scheduler.task_info(task_uuid)

    async def dump_task(
        self, task_uuid: str = 'all', timeout: Union[float, int, None] = None
    ) -> str:
        return await self._execute(
            timeout, self._scheduler.dump_task, task_uuid)

    async def slowest(self, timeout: Union[float, int, None] = None) -> str:
        return await self._execute(timeout, self._scheduler.slowest)
