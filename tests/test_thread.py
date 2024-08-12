import asyncio
import pytest
from unittest.mock import MagicMock
from typing import Tuple, List
from async_scheduler.scheduler import AsyncScheduler
from .common import (
    log,
    EmptyAsyncTask,
    AsyncTaskTimeoutCB,
    AsyncTaskErrorCB,
    AsyncTaskCancelCB,
    AsyncTaskDoneCB,
    DummyAsyncTask,
    AsyncTaskWithSyncSleep,
    EmptySyncTask,
)


@pytest.mark.asyncio
async def test_async_task_thread(scheduler_thread):
    loop = asyncio.get_running_loop()
    log.info(f'testing scheduler in a thread in {loop=}')
    assert scheduler_thread.ioloop
    assert scheduler_thread.ioloop is not loop
    task = EmptyAsyncTask(name='empty', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    scheduler_thread.start()
    await asyncio.sleep(1)
    assert scheduler_thread.is_alive()
    assert scheduler_thread.ioloop.is_running()
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    assert task_uuid
    await asyncio.sleep(3)
    curr_task_str = f'{scheduler_thread.current_task()}'
    log.info(f'current task: {curr_task_str}')
    assert 'no task in the current loop' == curr_task_str
    await asyncio.sleep(6)
    assert task.run_async.called

    log.info('testing async task with some args')
    task = EmptyAsyncTask(name='empty_with_args', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task_uuid = scheduler_thread.submit_task(task, 3)
    log.debug(f'task uuid: {task_uuid}')
    task_alias, args, kwargs = scheduler_thread.task_info(task_uuid)
    assert task_alias is task
    assert len(args) == 1
    assert args[0] == 3
    await asyncio.sleep(9)
    assert task.run_async.called

    log.info('testing sync task without any callbacks or args')
    task = EmptySyncTask(name='empty_sync', timeout=7)
    task.run_sync = MagicMock(side_effect=task.run_sync)
    task.start_cb = MagicMock(side_effect=task.start_cb)
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await scheduler_thread.wait_task(task_uuid, timeout=2)
    while not task.start_cb.called:
        curr_task_str = f'{scheduler_thread.current_task()}'
        log.info(f'current task: {curr_task_str}')
        await asyncio.sleep(1)
    # assert 'no task in the current loop' != curr_task_str
    await scheduler_thread.wait_task(task_uuid, timeout=3)
    assert task.run_sync.called
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_task_timeout(scheduler_thread):
    log.info('testing async task with time_cb')
    task = AsyncTaskTimeoutCB(name='timeout', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.timeout_cb = MagicMock(side_effect=task.timeout_cb)
    scheduler_thread.start()
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await asyncio.sleep(9)
    assert task.run_async.called
    assert not task.timeout_cb.called

    task = AsyncTaskTimeoutCB(name='timeout', timeout=4)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.timeout_cb = MagicMock(side_effect=task.timeout_cb)
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.timeout_cb.called
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_task_error(scheduler_thread):
    log.info('testing async task with error_cb')
    task = AsyncTaskErrorCB(name='error', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.error_cb = MagicMock(side_effect=task.error_cb)
    scheduler_thread.start()
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.error_cb.called
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_started(scheduler_thread):
    log.info('testing async task with cancel_cb')
    log.info('cancel running task')
    task = AsyncTaskCancelCB(name='cancel', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    scheduler_thread.start()
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await asyncio.sleep(2)
    assert await scheduler_thread.cancel_task(9, task_uuid)
    await asyncio.sleep(7)
    assert task.run_async.called
    assert task.cancel_cb.called
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_task_done(scheduler_thread):
    log.info('testing async task with done_cb')
    task = AsyncTaskDoneCB(name='done', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    scheduler_thread.start()
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.done_cb.called

    task = AsyncTaskDoneCB(name='done', timeout=3)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    await asyncio.sleep(9)
    assert task.run_async.called
    assert not task.done_cb.called
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_dummy_task_one(scheduler_thread):
    async_period = 2
    sync_period = 1
    slots = 5
    timeout = (async_period + sync_period) * slots
    loop_period = timeout + 1
    log.info('testing one task with different blocking periods')
    task = DummyAsyncTask('dummy_one',
                          timeout=timeout,
                          async_period=async_period,
                          sync_period=sync_period,
                          slots=slots)
    scheduler_thread.start()
    bt, nbt = task.sleep_time()
    log.debug(f'totally sleep time for {(bt + nbt): .2f} second(s)')
    task.run_async = MagicMock(side_effect=task.run_async)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    task_uuid = scheduler_thread.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    assert task_uuid
    await asyncio.sleep(loop_period)
    assert task.run_async.called
    assert task.done_cb.called
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_dummy_task_multiple(scheduler_thread):
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 5

    loop_thread = scheduler_thread.ioloop
    loop_thread.set_debug(True)
    loop_thread.slow_callback_duration = 5
    scheduler_thread.start()

    tasks, loop_period = await _create_multiple_tasks(
        scheduler_thread.scheduler,
        task_num=10,
        async_period=2,
        sync_period=1,
        slots=5)
    log.info(f'waiting loop for {loop_period} second(s)')
    await asyncio.sleep(loop_period / 4)
    log.info(f'all tasks:\n{await scheduler_thread.dump_task()}')
    log.info(f'slowest task:\n{await scheduler_thread.slowest()}')
    await asyncio.sleep(3 * loop_period / 4 + 1)
    func_called = [task.run_async.called for task in tasks]
    assert all(func_called)
    func_called = [task.done_cb.called for task in tasks]
    assert all(func_called)
    await scheduler_thread.stop()


@pytest.mark.asyncio
async def test_async_task_not_blocked_by_sync(scheduler_thread):
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 5

    loop_thread = scheduler_thread.ioloop
    loop_thread.set_debug(True)
    loop_thread.slow_callback_duration = 5

    scheduler_thread.start()
    slow_sync_task = AsyncTaskWithSyncSleep(name='dummy_slow_sync',
                                            sleep_time=5)
    slow_sync_task.done_cb = MagicMock(side_effect=slow_sync_task.done_cb)
    uuid_1 = scheduler_thread.submit_task(slow_sync_task)
    async_task = DummyAsyncTask(name='dummy_async_task',
                                timeout=10,
                                async_period=2,
                                sync_period=1,
                                slots=5)
    async_task.done_cb = MagicMock(side_effect=async_task.done_cb)
    uuid_2 = scheduler_thread.submit_task(async_task)
    log.debug(f'task uuids: {uuid_1}, {uuid_2}')
    await asyncio.sleep(15)
    assert slow_sync_task.done_cb.called
    assert async_task.done_cb.called
    await scheduler_thread.stop()


async def _create_multiple_tasks(scheduler: AsyncScheduler,
                                 task_num: int,
                                 async_period: int,
                                 sync_period: int,
                                 slots: int,
                                 name_prefix: str = 'dummy_'
                                 ) -> Tuple[List[DummyAsyncTask], int]:
    # initial big timeout
    timeout = (async_period + sync_period) * slots
    log.info(f'testing {task_num} task with different blocking periods')
    tasks: List[DummyAsyncTask] = []
    bt, nbt = 0.0, 0.0
    for i in range(task_num):
        task = DummyAsyncTask(f'{name_prefix}{i}',
                              timeout=timeout,
                              async_period=async_period,
                              sync_period=sync_period,
                              slots=slots)

        b1, n1 = task.sleep_time()
        log.debug(f'sleep time for {task.data.name}: {b1: .2f}, {n1: .2f}')
        bt += b1
        nbt += n1
        task.run_async = MagicMock(side_effect=task.run_async)
        task.done_cb = MagicMock(side_effect=task.done_cb)
        tasks.append(task)

    log.info(f'total sleep time: {bt: .2f}, {nbt: .2f}')
    log.info('set task timeout to total blocking time + '
             'individual non-blocking')
    loop_period = 0.0
    # optimize timeout for tasks
    for t in tasks:
        _, n1 = t.sleep_time()
        t.set_timeout(bt + n1)
        if bt + n1 > loop_period:
            loop_period = (bt + n1)

    for t in tasks:
        uuid = scheduler.submit_task(t)
        log.debug(f'task uuid: {uuid}')
        assert uuid

    loop_period = round(loop_period + 0.5)
    return (tasks, loop_period)


@pytest.mark.asyncio
async def test_two_scheduler_two_threads(two_threaded_scheduler):
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 5

    sch_th_1, sch_th_2 = two_threaded_scheduler
    loop_thread = sch_th_1.ioloop
    loop_thread.set_debug(True)
    loop_thread.slow_callback_duration = 5
    loop_thread = sch_th_2.ioloop
    loop_thread.set_debug(True)
    loop_thread.slow_callback_duration = 5

    sch_th_1.start()
    sch_th_2.start()

    task_group1, period1 = await _create_multiple_tasks(
        sch_th_1.scheduler,
        task_num=10,
        async_period=2,
        sync_period=1,
        slots=5,
        name_prefix='dummy_th_1_')
    task_group2, period2 = await _create_multiple_tasks(
        sch_th_2.scheduler,
        task_num=10,
        async_period=2,
        sync_period=1,
        slots=5,
        name_prefix='dummy_th_2_')
    loop_period = max(period1, period2)
    log.info(f'waiting loop for {loop_period} second(s)')
    await asyncio.sleep(loop_period / 4)
    log.info(f'all tasks in thread 1:\n{await sch_th_1.dump_task()}')
    log.info(f'slowest task in thread 1:\n{await sch_th_1.slowest()}')
    log.info(f'all tasks in thread 2:\n{await sch_th_2.dump_task()}')
    log.info(f'slowest task in thread 2:\n{await sch_th_2.slowest()}')
    await asyncio.sleep(3 * loop_period / 4 + 1)
    func_called = [task.run_async.called for task in task_group1]
    assert all(func_called)
    func_called = [task.done_cb.called for task in task_group1]
    assert all(func_called)

    await sch_th_1.stop()
    await sch_th_2.stop()
