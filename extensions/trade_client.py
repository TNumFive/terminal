import asyncio
import json
import logging
import time
from typing import Optional, Dict

from websockets.exceptions import ConnectionClosedError
from websockets.legacy import client as ws_client
from websockets.legacy.server import WebSocketServerProtocol

from core import Client
from core.utils import get_error_line

logger = logging.getLogger(__name__)


class TradeClient(Client):

    def load_content(self, content: str):
        try:
            data = json.loads(content)
            if "method" not in data:
                # this packet is a response packet
                return data
            # this packet is a reqeust packet
            assert len(data) == 3
            assert isinstance(data["method"], str)
            assert isinstance(data["id"], int)
            assert "params" in data
            return data
        except (KeyError, AssertionError):
            logger.warning(f"client:{self.uid} loading content failed: {get_error_line()}")
            return {}
        except Exception as e:
            logger.warning(f"client:{self.uid} loading content failed: {str(e)}")
            return {}

    async def subscribe(self, uid: str, stream: str):
        raise NotImplementedError

    async def unsubscribe(self, uid: str, stream: str):
        raise NotImplementedError


class ExchangeClient(TradeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.is_initialized = False

    async def check_alive(self, uid: str, request_id: int):
        response = {"id": request_id}
        await self.send([uid], json.dumps(response))

    async def check_initialized(self, uid: str, request_id: int):
        response = {"id": request_id, "result": self.is_initialized}
        await self.send([uid], json.dumps(response))

    async def react(self, packet: dict):
        data = self.load_content(packet["content"])
        if "method" not in data:
            return
        method = data["method"]
        if method == "check_alive":
            await self.check_alive(packet["uid"], data["id"])
        elif method == "check_initialized":
            await self.check_initialized(packet["uid"], data["id"])
        if not self.is_initialized:
            return
        if method == "subscribe":
            await self.subscribe(packet["source"], data["params"])
        elif method == "unsubscribe":
            await self.unsubscribe(packet["source"], data["params"])


class BinanceExchangeClient(ExchangeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid},
                 binance_ws_url="wss://stream.binance.com", init_stream="btcusdt@kline_1m"):
        super().__init__(uid, uri, auth_func)
        self.packet_buffer = []
        self.binance_task: Optional[asyncio.Task] = None
        self.binance_ws_url = binance_ws_url
        self.init_stream = init_stream
        self.binance_websocket: Optional[WebSocketServerProtocol] = None
        self.stream_dict: Dict[str, list] = {}

    async def send(self, dest: list[str], content: str):
        """
        Buffer data first in case of lost connection.

        The exception to inner ws shall not affect the outside ws, and wait until reconnect then re-send.
        """
        self.packet_buffer.append((dest, content))
        while len(self.packet_buffer):
            head = self.packet_buffer[0]
            try:
                await super().send(head[0], head[1])
                self.packet_buffer.pop(0)
            except ConnectionClosedError:
                # Eat the exception as it will be re-raised when try recv next message.
                break

    async def resubscribe(self):
        stream_list = list(self.stream_dict.keys())
        stream_list.remove(self.init_stream)
        if not len(stream_list):
            return
        request = {
            "method": "SUBSCRIBE",
            "params": stream_list,
            "id": int(time.time() * 1000)
        }
        await self.binance_websocket.send(json.dumps(request))

    async def handler_binance(self):
        await self.resubscribe()
        async for message in self.binance_websocket:
            try:
                data: dict = json.loads(message)
                assert isinstance(data, dict), "data type not dict"
            except Exception as e:
                logger.warning(f"client:{self.uid} load binance message failed: {str(e)}")
                continue
            stream = data.get("stream", None)
            if stream and isinstance(stream, str):
                dest = self.stream_dict[stream]
                if len(dest):
                    await self.send(dest, message)
            else:
                logger.warning(f"client:{self.uid} unknown data: {data}")

    async def connect_binance(self):
        ws_url = self.binance_ws_url + "/stream?streams=" + self.init_stream
        self.stream_dict[self.init_stream] = []
        async for websocket in ws_client.connect(ws_url):
            try:
                self.binance_websocket = websocket
                self.is_initialized = True
                await self.handler_binance()
            except asyncio.CancelledError:
                logger.warning(f"client:{self.uid} binance websocket exit")
                return
            except Exception as e:
                logger.warning(f"client:{self.uid} binance websocket closed: {str(e)}")
            finally:
                self.is_initialized = False

    async def set_up(self):
        if not self.binance_task or self.binance_task.done():
            self.binance_task = asyncio.create_task(self.connect_binance())

    async def subscribe(self, uid: str, stream: str):
        if stream not in self.stream_dict:
            self.stream_dict[stream] = [uid]
            request = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": int(time.time() * 1000)
            }
            await self.binance_websocket.send(json.dumps(request))
        dest = self.stream_dict[stream]
        if uid not in dest:
            dest.append(uid)

    async def unsubscribe(self, uid: str, stream: str):
        if stream not in self.stream_dict:
            return
        dest = self.stream_dict[stream]
        if uid in dest:
            dest.remove(uid)
        if not len(dest):
            request = {
                "method": "UNSUBSCRIBE",
                "params": [stream],
                "id": int(time.time() * 1000)
            }
            await self.binance_websocket.send(json.dumps(request))
            self.stream_dict.pop(stream)

    async def __call__(self):
        await super().__call__()
        if self.binance_task and not self.binance_task.done():
            await self.binance_task


class StrategyClient(TradeClient):

    async def check_alive(self, uid: str):
        content = {
            "method": "check_alive",
            "params": "",
            "id": int(time.time() * 1000),
        }
        await self.send([uid], json.dumps(content))

    async def check_initialized(self, uid: str):
        content = {
            "method": "check_initialized",
            "params": "",
            "id": int(time.time() * 1000),
        }
        await self.send([uid], json.dumps(content))

    async def subscribe(self, uid: str, stream: str):
        content = {
            "method": "subscribe",
            "params": stream,
            "id": int(time.time() * 1000),
        }
        await self.send([uid], json.dumps(content))

    async def unsubscribe(self, uid: str, stream: str):
        content = {
            "method": "unsubscribe",
            "params": stream,
            "id": int(time.time() * 1000),
        }
        await self.send([uid], json.dumps(content))
