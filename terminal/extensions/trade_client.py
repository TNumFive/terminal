import asyncio
import json
import logging
import time
from typing import Dict

from terminal.core import Client
from terminal.core.utils import get_error_line

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
        if not self.is_initialized:
            return
        if method == "subscribe":
            await self.subscribe(packet["source"], data["params"])
        elif method == "unsubscribe":
            await self.unsubscribe(packet["source"], data["params"])


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
        return data
