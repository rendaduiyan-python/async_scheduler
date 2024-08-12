import asyncio
import time
import random

from tornado.web import RequestHandler, Application
from tornado.httputil import HTTPServerRequest
from typing import Any
from tests.common import log
from async_scheduler.scheduler import (
    AsyncTask, 
    AsyncScheduler, 
    AsyncTask,
    TaskTS,
    T)


class AuthMixin:
    async def _authenticate(self, headers: dict) -> bool:
        # TO-DO: add real code
        log.info(f"token is valid")
        return True
    
class file_task(AsyncTask):
    _MAX_DURATION: int = 30
    def __init__(
            self,
            name: str,
            index: int,
            stats: dict,
            handler: RequestHandler,
            timeout: int = -1) -> None:
        super().__init__(name, timeout)
        self._uuid = name
        self._index = index
        self._stats = stats
        self._handler = handler
        random.seed()

    async def run_async(self) -> None:
        await asyncio.sleep(random.random() * self._MAX_DURATION)        
        self._data.result = str(self._uuid * (self._index + 1))
    
    def timeout_cb(self, data: TaskTS, res_err: T) -> None:
        super().timeout_cb(data, res_err)
        self._stats.update({
            self._uuid: {
                'timeout': True,                
            }
        })

    def done_cb(self, data: TaskTS, res_err: T) -> None:
        super().done_cb(data, res_err)        
        duration = TaskTS.delta_seconds(data.start_ts, data.done_ts)
        log.info(f'file generated in {duration} second(s)')
        self._stats.update({
            self._uuid: {
                'done': duration,
                'index': self._index,
            }
        })

class file_handler(AuthMixin, RequestHandler):
    def __init__(
        self,
        application: Application,
        request: HTTPServerRequest,
        **kwargs: Any
    ) -> None:        
        RequestHandler.__init__(self, application, request, **kwargs)         
        
    async def prepare(self) -> None:
        log.debug(f'{self.request=}')
        if not await self._authenticate(self.request.headers):
            log.warning("authentication failed")
            self.set_status(401)
    
    async def get(self) -> None:
        uuid = self.get_argument('uuid', None)
        index = self.get_argument('index', None)
        if uuid and index:
            index = int(index)
            log.info(f'adding task {uuid}, {index}')
            t = file_task(uuid, index, self._stats, self)
            task_uuid = self._scheduler.submit_task(t)            
            success = await self._scheduler.wait_task(task_uuid)
            if success:
                log.debug(f'result = {t.result}')
                self.write(t.result)            
        else:
            log.warning('ignore one request')
            if uuid:
                self._stats.update({
                    uuid: {'ignored': True}
                })

    def initialize(
            self,            
            stats: dict,
            scheduler: AsyncScheduler
        ) -> None:        
        self._stats = stats
        self._scheduler = scheduler

class file_app(Application):
    def __init__(self, port: int) -> None:
        self._port = port
        self._stats = {}
        self._scheduler = AsyncScheduler()
        handlers = [
            ("/files", file_handler, {'stats': self._stats, 'scheduler': self._scheduler})
        ]
        settings = {}
        super().__init__(handlers, **settings)        
    
    def start(self) -> None:
        # TO-DO: add ssl options        
        self.rpc_https_server = super().listen(self._port)
        self._scheduler.start()
        log.info(f"file service listening on {self._port}")

    def stop(self) -> None:
        task_stop = asyncio.create_task(self._scheduler.stop())
        task_stop.add_done_callback(AsyncScheduler._default_done_cb1)
        log.info(f'wait a while to stop scheduler')
        time.sleep(1)
        self.rpc_https_server.stop()


class file_service:
    def __init__(self) -> None:
        self._port = 20024
        self._app = file_app(self._port)

    async def start(self) -> None:
        self._app.start()
        await asyncio.Event().wait()
    
    def stop(self) -> None:
        self._app.stop()
        log.info('file service stopped')
    
if __name__ == '__main__':
    svc = file_service()
    asyncio.run(svc.start())