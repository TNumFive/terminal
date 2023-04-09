import asyncio
import json
import logging
import time
from typing import Dict, Optional

from websockets.exceptions import ConnectionClosed

from core import Client
from core.utils import get_error_line
from .binance_helper import BinanceRawHelper as BinanceHelper

logger = logging.getLogger(__name__)


class TradeClient(Client):

    def load_content(self, content: str):
        try:
            data = json.loads(content)
            if "method" in data:
                # this packet is a reqeust packet
                assert len(data) == 3
                assert isinstance(data["method"], str)
                assert isinstance(data["id"], int)
                assert "params" in data
                return data
            elif "result" in data:
                # this packet is a response packet
                assert len(data) == 2
                assert isinstance(data["result"], str)
                assert isinstance(data["id"], int)
                return data
            else:
                # this packet is a public data stream
                return data
        except (KeyError, AssertionError):
            logger.warning(f"client:{self.uid} loading content failed: {get_error_line()}")
            return {}
        except Exception as e:
            logger.warning(f"client:{self.uid} loading content failed: {str(e)}")
            return {}

    @staticmethod
    def get_request_id():
        return int(time.time() * 1000)

    async def subscribe(self, uid: str, stream: str):
        raise NotImplementedError

    async def unsubscribe(self, uid: str, stream: str):
        raise NotImplementedError


class ExchangeClient(TradeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.is_initialized = False

    async def check_alive(self, uid: str, request_id: int):
        response = {"id": request_id, "result": True}
        await self.send([uid], json.dumps(response))

    async def check_initialized(self, uid: str, request_id: int):
        response = {"id": request_id, "result": self.is_initialized}
        await self.send([uid], json.dumps(response))

    async def react(self, packet: dict):
        data = self.load_content(packet["content"])
        method = data.get("method", None)
        if method == "check_alive":
            await self.check_alive(packet["source"], data["id"])
        elif method == "check_initialized":
            await self.check_initialized(packet["source"], data["id"])
        if self.is_initialized:
            if method == "subscribe":
                await self.subscribe(packet["source"], data["params"])
            elif method == "unsubscribe":
                await self.unsubscribe(packet["source"], data["params"])


class BinanceExchangeClient(ExchangeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid},
                 binance_helper=BinanceHelper()):
        super().__init__(uid, uri, auth_func)
        self.packet_buffer = []
        self.binance_task: Optional[asyncio.Task] = None
        self.binance_helper = binance_helper
        self.stream_dict: Dict[str, list] = self.binance_helper.stream_dict
        self.binance_helper.portal = lambda data: self.helper_portal(data)

    @property
    def is_initialized(self):
        return self.binance_helper.is_initialized

    @is_initialized.setter
    def is_initialized(self, _):
        pass

    async def send(self, dest: list[str], content: str):
        """
        Buffer data first in case of lost connection.

        The exception to inner ws shall not affect the outside ws, and wait until reconnect then re-send.
        """
        try:
            await super().send(dest, content)
        except ConnectionClosed:
            # Eat the exception and buffer the data.
            self.packet_buffer.append((dest, content))

    async def handle_binance(self, data: dict):
        stream = data.get("stream", None)
        if stream and isinstance(stream, str):
            dest = self.stream_dict[stream]
            if len(dest):
                await self.send(dest, json.dumps(data))
        else:
            logger.warning(f"client:{self.uid} unknown data: {data}")

    def helper_portal(self, data):
        task = asyncio.create_task(self.handle_binance(data))
        self.background_task.add(task)
        task.add_done_callback(self.background_task.discard)

    async def set_up(self):
        await super().set_up()
        if not self.binance_task or self.binance_task.done():
            self.binance_task = asyncio.create_task(self.binance_helper())
        if len(self.packet_buffer):
            packet_buffer, self.packet_buffer = self.packet_buffer, []
            for packet in packet_buffer:
                await self.send(packet[0], packet[1])

    async def subscribe(self, uid: str, stream: str):
        await self.binance_helper.subscribe(uid, stream)

    async def unsubscribe(self, uid: str, stream: str):
        await self.binance_helper.unsubscribe(uid, stream)

    async def __call__(self):
        await super().__call__()
        # Stop binance_helper when exited.
        if self.binance_task and not self.binance_task.done():
            await self.binance_helper.stop()
            await self.binance_task
        if len(self.background_task):
            await asyncio.gather(*self.background_task)


class StrategyClient(TradeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.request_dict: Dict[int, asyncio.Future] = {}

    async def check_alive(self, uid: str):
        request_id = self.get_request_id()
        content = {
            "method": "check_alive",
            "params": "",
            "id": request_id,
        }
        await self.send([uid], json.dumps(content))
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def check_initialized(self, uid: str):
        request_id = self.get_request_id()
        content = {
            "method": "check_initialized",
            "params": "",
            "id": request_id,
        }
        await self.send([uid], json.dumps(content))
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def subscribe(self, uid: str, stream: str):
        request_id = self.get_request_id()
        content = {
            "method": "subscribe",
            "params": stream,
            "id": request_id,
        }
        await self.send([uid], json.dumps(content))
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def unsubscribe(self, uid: str, stream: str):
        request_id = self.get_request_id()
        content = {
            "method": "unsubscribe",
            "params": stream,
            "id": request_id,
        }
        await self.send([uid], json.dumps(content))
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def react(self, packet: dict):
        data = self.load_content(packet["content"])
        if "method" in data:
            # Strategy client usually doesn't care about request
            pass
        elif "result" in data and data["id"] in self.request_dict:
            # requested response has come back.
            request_id: int = data["id"]
            self.request_dict[request_id].set_result(data["result"])
