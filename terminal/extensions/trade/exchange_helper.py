import asyncio
import json
import logging
import time
from typing import Optional, Dict, Set

import aiohttp
from aiohttp import ClientWebSocketResponse

logger = logging.getLogger(__name__)


class ExchangeHelper:
    def __init__(self):
        self.is_initialized = False
        self.stream_set: Dict[str, Set[str]] = {}
        self.stream_substitute: Dict[str, str] = {}
        self.portal = lambda data: NotImplementedError
        self.background_task = set()

    @staticmethod
    def get_request_id():
        """
        Use timestamp in millisecond as request id.
        """
        return int(time.time() * 1000)

    @staticmethod
    def parse_stream(stream: str):
        """
        Currently, there are four kind of streams;

        <base>_<quote>@trade: return symbol's trade

        <base>_<quote>@book: return symbol's book

        <base>_<quote>@kline: return symbol's kline of 1 minute.

        user@?: return user account related like transaction detail.
        """
        arr = stream.split("@")
        stream_symbol = arr[0]
        stream_type = arr[1]
        return stream_symbol.split("_"), stream_type.split("_")

    async def subscribe(self, uid: str, stream: str):
        raise NotImplementedError

    async def unsubscribe(self, uid: str, stream: str):
        raise NotImplementedError

    async def __call__(self):
        raise NotImplementedError


class ExchangeRawHelper(ExchangeHelper):
    def __init__(self, http_url: str, ws_url: str, proxy=None, websocket_send_interval=0.5, max_connect_retry_times=10):
        super().__init__()
        self.http_url = http_url
        self.ws_url = ws_url
        self.proxy = proxy
        self.websocket_send_interval = websocket_send_interval
        self.max_connect_retry_times = max_connect_retry_times
        self.message_buffer: list[str] = []
        self.session: Optional[aiohttp.ClientSession] = None
        self.websocket: Optional[ClientWebSocketResponse] = None
        self.connect_retry_times = 10
        self.websocket_send_time = time.time()

    def format_stream_name(self, stream: str):
        pass

    async def websocket_send_control(self):
        while True:
            diff = time.time() - self.websocket_send_time
            if diff < self.websocket_send_interval:
                await asyncio.sleep(self.websocket_send_interval - diff)
            else:
                break
        self.websocket_send_time = time.time()

    async def websocket_send(self, message):
        await self.websocket_send_control()
        try:
            await self.websocket.send_str(message)
        except ConnectionResetError as e:
            logger.warning(f"connection reset error: {e}")
            self.message_buffer.append(message)

    async def websocket_reconnect_control(self):
        if self.connect_retry_times > self.max_connect_retry_times:
            return False
        await asyncio.sleep(self.connect_retry_times * 10)
        self.connect_retry_times += 1
        return True

    async def connect(self):
        self.websocket = await self.session.ws_connect(
            self.ws_url,
            timeout=1e8,
            proxy=self.proxy,
            compress=12
        )

    async def set_up(self):
        self.connect_retry_times = 0
        self.is_initialized = True
        logger.info("helper is initialized")
        buffer, self.message_buffer = self.message_buffer, []
        for message in buffer:
            await self.websocket_send(message)

    async def preprocess(self, data: dict):
        return data

    async def handler(self):
        async for message in self.websocket:
            try:
                data: dict = json.loads(message.data)
                assert isinstance(data, dict), "data type not dict"
            except Exception as e:
                logger.warning(f"loading message failed: {str(e)}")
                continue
            data = await self.preprocess(data)
            if len(data):
                self.portal(data)

    def clean_up(self):
        self.is_initialized = False

    async def wait_clean_up(self):
        await self.websocket.close()

    async def __call__(self):
        self.session = aiohttp.ClientSession()
        while True:
            try:
                try:
                    await self.connect()
                    await self.set_up()
                    await self.handler()
                finally:
                    self.clean_up()
                    await self.wait_clean_up()
                    await self.websocket.close()
            except asyncio.CancelledError:
                logger.info("websocket exit")
                break
            except Exception as e:
                logger.warning(f"connection closed: {e}")
                reconnect_control = await self.websocket_reconnect_control()
                if not reconnect_control:
                    break
        await self.session.close()
