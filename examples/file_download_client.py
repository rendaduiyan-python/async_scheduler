import asyncio
from tornado.httpclient import AsyncHTTPClient
from asyncio_scheduler.scheduler import AsyncScheduler
from tests.common import log

http_client = AsyncHTTPClient()
async def download_one_file(
    index: int,
    stats: dict,
    http_client: AsyncHTTPClient
) -> None:
    port = 20024
    uuid = AsyncScheduler.new_uuid()
    url = f'http://127.0.0.1:{port}/files?uuid={uuid}&index={index}'
    log.info(f'{url=}')
    try:
        content = (await http_client.fetch(url)).body.decode()
        log.debug(f'{content=}')
        if content == str(uuid * (index + 1)):
            stats.update({index: True})
        else:
            stats.update({index: False})
    except Exception as err:
        log.warning(f'At {index}, downloading failed: {err}')
        stats.update({index: False})
    
def default_done_cb(task: asyncio.Task):
    try:
        task.result()
    except Exception as err:
        log.error(f'task {task.get_name()} failed: {err}')

async def main():
    cnt: int = 100
    stats: dict = {}
    http_client = AsyncHTTPClient(max_clients=500, defaults={'request_timeout': 120})
    async with asyncio.TaskGroup() as tg:
        for i in range(0, cnt):
            task = tg.create_task(download_one_file(i, stats, http_client))
            task.add_done_callback(default_done_cb)
    print(f'all files are downdloaded: {stats}')
    assert all(list(stats.values()))

asyncio.run(main())