import asyncio
import json
import logging
import time
from typing import Optional, Dict

from websockets.exceptions import ConnectionClosedError
from websockets.legacy.server import WebSocketServerProtocol
from websockets.legacy import client as ws_client

from core import Client
from core.utils import get_error_line

logger = logging.getLogger(__name__)


class TradeClient(Client):

    async def initialize(self):
        pass

    async def login(self):
        await super().login()
        await self.initialize()

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

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}, debug=False):
        super().__init__(uid, uri, auth_func)
        self.debug = debug
        self.is_initialized = False

    async def check_alive(self, uid: str, request_id: int):
        """
        There are two kinds of request,

        One like this check_alive, which will return {"method":result_obj}. The result_obj must be a dict with key "id".

        And the other like sub/unsub, which will return public data in binance ws data format.
        """
        response = {"check_alive": {"id": request_id}}
        await self.send([uid], json.dumps(response))

    async def check_initialized(self, uid: str, request_id: int):
        response = {"check_initialized": {"id": request_id, "status": self.is_initialized}}
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
        self.binance_ws_url = binance_ws_url
        self.init_stream = init_stream
        self.binance_websocket: Optional[WebSocketServerProtocol] = None
        self.stream_dict: Dict[str, list] = {init_stream: []}
        self.background_task = set()

    async def resubscribe(self):
        stream_list = list(self.stream_dict.keys())
        stream_list.remove(self.init_stream)
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
                assert isinstance(data, dict)
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
        async for websocket in ws_client.connect(ws_url):
            try:
                self.binance_websocket = websocket
                self.is_initialized = True
                await self.handler_binance()
            except ConnectionClosedError as e:
                logger.warning(f"client:{self.uid} connection error: {str(e)}")
            finally:
                self.websocket = None
                if self.debug:
                    # prevent error connecting
                    return

    async def initialize(self):
        if not self.is_initialized:
            task = asyncio.create_task(self.connect_binance())
            self.background_task.add(task)
            task.add_done_callback(self.background_task.discard)

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
