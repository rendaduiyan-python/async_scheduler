import pytest_asyncio
import asyncio
from asyncio_scheduler.scheduler import AsyncScheduler
from asyncio_scheduler.thread import SchedulerThread


@pytest_asyncio.fixture()
async def scheduler():
    s = AsyncScheduler()
    yield s


@pytest_asyncio.fixture()
async def scheduler_thread():
    loop = asyncio.get_running_loop()
    st = SchedulerThread('test', loop)
    yield st


@pytest_asyncio.fixture()
async def two_threaded_scheduler():
    loop = asyncio.get_running_loop()
    st1 = SchedulerThread('test1', loop)
    st2 = SchedulerThread('test2', loop)
    yield (st1, st2)
