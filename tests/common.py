import logging
import random
import time
import asyncio

from asyncio_scheduler.scheduler import AsyncTask, SyncTask

logging.basicConfig(format='%(levelname)s:%(message)s',
                    level=logging.DEBUG)
logging.Formatter.converter = time.gmtime
log = logging.getLogger('scheduler')


class EmptyAsyncTask(AsyncTask):
    '''
    Dummy AsyncTask to test scheduling and executing.
    '''
    _DURATION = 4

    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    async def run_async(self, *args, **kwargs):
        if len(args) > 0:
            duration = args[0]
        else:
            if 'duraiton' in kwargs:
                duration = kwargs['duraiton']
            else:
                duration = self._DURATION
        log.info(f'about to yield for {duration} second(s)')
        await asyncio.sleep(duration)
        log.info(f'{type(self).__name__} method is called')


class EmptySyncTask(SyncTask):
    '''
    Dummy SyncTask to test scheduling and executing.
    '''
    _DURATION = 4

    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def run_sync(self, *args, **kwargs):
        if len(args) > 0:
            duration = args[0]
        else:
            if 'duraiton' in kwargs:
                duration = kwargs['duraiton']
            else:
                duration = self._DURATION
        log.info(f'about to block for {duration} second(s)')
        time.sleep(duration)
        log.info(f'{type(self).__name__} method is called')

    def start_cb(self, data):
        log.info(f'start_cb is called: {data}')
        assert data.start_ts != -1


class AsyncTaskStartCB(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def start_cb(self, data):
        log.info(f'start_cb is called: {data}')
        assert data.start_ts != -1


class AsyncTaskTimeoutCB(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def timeout_cb(self, data):
        log.info(f'timeout_cb is called: {data}')
        assert data.start_ts != -1
        assert data.timeout_ts != -1
        to = int((data.timeout_ts - data.start_ts)/1e9)
        log.info(f'{self.data.name} timed out after {to} second(s)')
        assert (data.timeout_ts - data.start_ts) >= self.data.timeout


class AsyncTaskDoneCB(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def done_cb(self, data, res_err):
        log.info(f'done_cb is called: {data} {res_err}')
        assert data.start_ts != -1
        assert data.done_ts != -1


class AsyncTaskErrorCB(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    async def run_async(self, *args, **kwargs):
        raise RuntimeError('dummy error')

    def error_cb(self, data, res_err):
        log.info(f'error_cb is called: {data}, {res_err}')
        assert data.start_ts != -1
        assert data.error_ts != -1


class AsyncTaskCancelCB(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def cancel_cb(self, data):
        log.info(f'cancel_cb is called: {data}')
        assert data.start_ts != -1
        assert data.cancel_ts != -1


class AsyncTaskCancelCBQueue(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def cancel_cb(self, data):
        log.info(f'cancel_cb is called: {data}')
        assert data.start_ts == -1
        assert data.cancel_ts != -1


class AsyncTaskCancelCBTimeout(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def timeout_cb(self, data):
        log.info(f'timeout_cb is called: {data}')
        assert data.start_ts != -1
        assert data.timeout_ts != -1

    def cancel_cb(self, data):
        log.info(f'cancel_cb is called: {data}')
        assert data.start_ts != -1
        assert data.cancel_ts != -1


class AsyncTaskCancelCBDone(EmptyAsyncTask):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    def done_cb(self, data, res_err):
        log.info(f'done_cb is called: {data} {res_err}')
        assert data.start_ts != -1
        assert data.done_ts != -1

    def cancel_cb(self, data):
        log.info(f'cancel_cb is called: {data}')
        assert data.start_ts != -1
        assert data.cancel_ts != -1


class AsyncTaskCancelError(AsyncTaskErrorCB):
    def __init__(self, name: str, timeout: int = -1):
        super().__init__(name, timeout)

    async def run_async(self, *args, **kwargs):
        raise RuntimeError('dummy error')

    def error_cb(self, data, res_err):
        log.info(f'error_cb is called: {data}, {res_err}')
        assert data.start_ts != -1
        assert data.error_ts != -1

    def cancel_cb(self, data):
        log.info(f'cancel_cb is called: {data}')
        assert data.error_ts != -1
        assert data.cancel_ts != -1


class DummyAsyncTask(AsyncTask):
    def __init__(self, name, timeout, async_period, sync_period, slots=10):
        super().__init__(name, timeout)
        random.seed()
        self.time_slots = []
        self.slots = slots
        for _ in range(slots):
            if bool(random.getrandbits(1)):
                self.time_slots.append(('async',
                                        random.random() * sync_period))
            else:
                self.time_slots.append(('sync',
                                        random.random() * async_period))

    def set_timeout(self, to):
        log.debug(f'set new timeout to {to: .2f} for {self.data.name}')
        self.data.timeout = to

    def sleep_time(self):
        bt, nbt = 0.0, 0.0
        for i in range(self.slots):
            sleep_type, period = self.time_slots[i]
            if sleep_type == 'sync':
                bt += period
            else:
                nbt += period
        return bt, nbt

    async def run_async(self):
        log.info(f'{self.data.name}: will time out '
                 f'in {self.data.timeout: .2f} second(s)')
        for i in range(self.slots):
            sleep_type, period = self.time_slots[i]
            if sleep_type == 'sync':
                log.info(f'{self.data.name}: slot {i} blocking for '
                         f'{period: .2f} second(s)')
                time.sleep(period)
            else:
                log.info(f'{self.data.name}: slot {i} yielding for '
                         f'{period: .2f} second(s)')
                await asyncio.sleep(period)

        log.info(f'{self.data.name}: run_async is called')

    def done_cb(self, data, res_err):
        log.info(f'{self.data.name}: done_cb is called: {data} {res_err}')
        assert data.start_ts != -1
        assert data.done_ts != -1

    def timeout_cb(self, data):
        log.info(f'timeout_cb is called: {data}')
        assert data.start_ts != -1
        assert data.timeout_ts != -1
        to = int((data.timeout_ts - data.start_ts)/1e9)
        log.info(f'{self.data.name} timed out after {to} second(s)')
        assert (data.timeout_ts - data.start_ts) >= self.data.timeout


class AsyncTaskWithSyncSleep(AsyncTask):
    def __init__(self, name: str, sleep_time: int) -> None:
        super().__init__(name, sleep_time + 1)
        self._sleep_time = sleep_time

    async def run_async(self) -> None:
        # await asyncio.sleep(1)
        time.sleep(self._sleep_time)
        log.info(f'{self.data.name}: run_async is called')

    def done_cb(self, data, res_err):
        log.info(f'{self.data.name}: done_cb is called: {data} {res_err}')
        assert data.start_ts != -1
        assert data.done_ts != -1
