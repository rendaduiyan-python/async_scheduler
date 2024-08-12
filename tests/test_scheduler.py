import asyncio
import pytest
from unittest.mock import MagicMock
from .common import (
    log,
    EmptyAsyncTask,
    EmptySyncTask,
    AsyncTaskStartCB,
    AsyncTaskTimeoutCB,
    AsyncTaskDoneCB,
    AsyncTaskErrorCB,
    AsyncTaskCancelCB,
    AsyncTaskCancelCBQueue,
    AsyncTaskCancelCBTimeout,
    AsyncTaskCancelCBDone,
    AsyncTaskCancelError,
    DummyAsyncTask,
)


@pytest.mark.asyncio
async def test_task_basic(scheduler):
    log.info('testing async task without any callbacks or args')
    task = EmptyAsyncTask(name='empty', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    log.info(f'current task: {scheduler.current_task()}')
    await asyncio.sleep(9)
    assert task.run_async.called
    await scheduler.stop()

    log.info('testing async task with some args')
    task = EmptyAsyncTask(name='empty_with_args', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task_uuid = scheduler.submit_task(task, 3)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    task_alias, args, kwargs = scheduler.task_info(task_uuid)
    assert task_alias is task
    assert len(args) == 1
    assert args[0] == 3
    await asyncio.sleep(9)
    assert task.run_async.called
    await scheduler.stop()

    log.info('testing async task with some kwargs')
    task = EmptyAsyncTask(name='empty_with_kwargs', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task_uuid = scheduler.submit_task(task, duration=3)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    task_alias, args, kwargs = scheduler.task_info(task_uuid)
    assert task_alias is task
    assert len(args) == 0
    assert 'duration' in kwargs
    assert kwargs.get('duration') == 3
    await asyncio.sleep(9)
    assert task.run_async.called
    await scheduler.stop()

    log.info('testing sync task without any callbacks or args')
    task = EmptySyncTask(name='empty', timeout=7)
    task.run_sync = MagicMock(side_effect=task.run_sync)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    log.info(f'current task: {scheduler.current_task()}\n')
    await asyncio.sleep(9)
    assert task.run_sync.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_start(scheduler):
    log.info('testing async task with start_cb')
    task = AsyncTaskStartCB(name='start', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.start_cb = MagicMock(side_effect=task.start_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.start_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_timeout(scheduler):
    log.info('testing async task with time_cb')
    task = AsyncTaskTimeoutCB(name='timeout', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.timeout_cb = MagicMock(side_effect=task.timeout_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(9)
    assert task.run_async.called
    assert not task.timeout_cb.called
    await scheduler.stop()

    task = AsyncTaskTimeoutCB(name='timeout', timeout=4)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.timeout_cb = MagicMock(side_effect=task.timeout_cb)
    task_uuid = scheduler.submit_task(task, 5)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.timeout_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_done(scheduler):
    log.info('testing async task with done_cb')
    task = AsyncTaskDoneCB(name='done', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.done_cb.called
    await scheduler.stop()

    task = AsyncTaskDoneCB(name='done', timeout=3)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    task_uuid = scheduler.submit_task(task, 4)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(9)
    assert task.run_async.called
    assert not task.done_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_error(scheduler):
    log.info('testing async task with error_cb')
    task = AsyncTaskErrorCB(name='error', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.error_cb = MagicMock(side_effect=task.error_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.error_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_started(scheduler):
    log.info('testing async task with cancel_cb')
    log.info('cancel running task')
    task = AsyncTaskCancelCB(name='cancel', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(2)
    assert await scheduler.cancel_task(task_uuid)
    await asyncio.sleep(7)
    assert task.run_async.called
    assert task.cancel_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_queue(scheduler):
    log.info('cancel task in the queue')
    task = AsyncTaskCancelCBQueue(name='cancel', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    assert task_uuid
    await asyncio.sleep(2)
    assert await scheduler.cancel_task(task_uuid)
    await asyncio.sleep(7)
    assert not task.run_async.called
    assert task.cancel_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_timeout(scheduler):
    log.info('cancel task that times out already')
    task = AsyncTaskCancelCBTimeout(name='cancel', timeout=3)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.timeout_cb = MagicMock(side_effect=task.timeout_cb)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    task_uuid = scheduler.submit_task(task, 4)
    log.debug(f'task uuid: {task_uuid}')
    assert task_uuid
    scheduler.start()
    await asyncio.sleep(4)
    assert not await scheduler.cancel_task(task_uuid)
    assert task.run_async.called
    assert task.timeout_cb.called
    assert not task.cancel_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_done(scheduler):
    log.info('cancel task that is done already')
    task = AsyncTaskCancelCBDone(name='cancel', timeout=3)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    task_uuid = scheduler.submit_task(task, 1)
    log.debug(f'task uuid: {task_uuid}')
    assert task_uuid
    scheduler.start()
    await asyncio.sleep(4)
    assert not await scheduler.cancel_task(task_uuid)
    assert task.run_async.called
    assert task.done_cb.called
    assert not task.cancel_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_error(scheduler):
    log.info('cancel task that failed')
    task = AsyncTaskCancelError(name='cancel', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    task.error_cb = MagicMock(side_effect=task.error_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(1)
    assert not await scheduler.cancel_task(task_uuid)
    await asyncio.sleep(9)
    assert task.run_async.called
    assert task.error_cb.called
    assert not task.cancel_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_task_cancel_cancel(scheduler):
    log.info('cancel task that is already cancelled')
    log.info('firstly, cancel running task')
    task = AsyncTaskCancelCB(name='cancel', timeout=7)
    task.run_async = MagicMock(side_effect=task.run_async)
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(2)
    assert await scheduler.cancel_task(task_uuid)
    await asyncio.sleep(7)
    assert task.run_async.called
    assert task.cancel_cb.called
    log.info('secondly, cancel the exact same task')
    task.cancel_cb = MagicMock(side_effect=task.cancel_cb)
    assert not await scheduler.cancel_task(task_uuid)
    assert not task.cancel_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_dummy_task_one(scheduler):
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
    bt, nbt = task.sleep_time()
    log.debug(f'totally sleep time for {(bt + nbt): .2f} second(s)')
    task.run_async = MagicMock(side_effect=task.run_async)
    task.done_cb = MagicMock(side_effect=task.done_cb)
    task_uuid = scheduler.submit_task(task)
    log.debug(f'task uuid: {task_uuid}')
    scheduler.start()
    assert task_uuid
    await asyncio.sleep(loop_period)
    assert task.run_async.called
    assert task.done_cb.called
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_dummy_task_multiple(scheduler):
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 5

    async_period = 2
    sync_period = 1
    slots = 5
    timeout = (async_period + sync_period) * slots
    task_num = 10
    log.info(f'testing {task_num} task with different blocking periods')
    tasks = []
    bt, nbt = 0.0, 0.0
    for i in range(task_num):
        task = DummyAsyncTask(f'dummy_{i}',
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
    scheduler.start()
    log.info(f'waiting loop for {loop_period} second(s)')
    await asyncio.sleep(loop_period / 4)
    log.info(f'all task:\n{await scheduler.dump_task()}')
    log.info(f'slowest task:\n{await scheduler.slowest()}')
    await asyncio.sleep(3 * loop_period / 4 + 1)
    func_called = [task.run_async.called for task in tasks]
    assert all(func_called)
    func_called = [task.done_cb.called for task in tasks]
    assert all(func_called)
    await scheduler.stop()


@pytest.mark.asyncio
async def test_async_dummy_task_slowest(scheduler):
    task_to = 31
    task1 = EmptyAsyncTask(name='long', timeout=task_to)
    task1.run_async = MagicMock(side_effect=task1.run_async)
    task_uuid = scheduler.submit_task(task1, duraiton=(task_to - 1))
    assert task_uuid
    log.debug(f'task uuid: {task_uuid}')
    task2 = EmptyAsyncTask(name='short', timeout=task_to)
    task2.run_async = MagicMock(side_effect=task2.run_async)
    task_uuid = scheduler.submit_task(task2, duraiton=1)
    assert task_uuid
    log.debug(f'task uuid: {task_uuid}')
    slowest_s = await scheduler.slowest()
    assert slowest_s == ''
    interval = 10
    scheduler.start()
    for x in range(10, 31, interval):
        await asyncio.sleep(interval)
        slowest_s = await scheduler.slowest()
        assert slowest_s != ''
        assert 'long' in slowest_s
        assert 'short' not in slowest_s
        check_str = ''
        s = 'long'
        check_str += f'{s: >20}'
        if x == 30:
            s = 'done'
        else:
            s = 'running'
        check_str += f'{s: >40}'
        check_str += f'{x: >60}'
        log.debug(f'expected string:\n\'{check_str}\'')
        log.debug(f'slowest string:\n\'{slowest_s}\'')
        assert slowest_s != ''
        assert 'long' in slowest_s
        assert 'running' in slowest_s
    await asyncio.sleep(1)
    assert task1.run_async.called
    assert task2.run_async.called
    await scheduler.stop()
