import asyncio
import logging
import time
from typing import Dict, Optional, Set

from websockets.exceptions import ConnectionClosed

from terminal.core import Client, Packet, get_timestamp
from .exchange_helper import ExchangeHelper
from .trade_content import TradeContent, RequestContent, ResponseContent, StreamContent

logger = logging.getLogger(__name__)


class TradeClient(Client):

    @staticmethod
    def load_content(content: str):
        trade_content = TradeContent.check_content(content)
        if trade_content.klass == "request":
            return RequestContent.from_content(trade_content.content)
        elif trade_content.klass == "response":
            return ResponseContent.from_content(trade_content.content)
        elif trade_content.klass == "stream":
            return StreamContent.from_content(trade_content.content)
        assert False, "content klass error"

    @staticmethod
    def get_request_id():
        return int(time.time() * 1000)

    async def subscribe(self, uid: str, stream: str):
        raise NotImplementedError

    async def unsubscribe(self, uid: str, stream: str):
        raise NotImplementedError


class ExchangeClient(TradeClient):

    def __init__(
            self,
            uid: str,
            helper: ExchangeHelper,
            uri: str = "ws://localhost:8080",
            auth_func=lambda uid: {"uid": uid}
    ):
        super().__init__(uid, uri, auth_func)
        self.helper = helper
        self.stream_set: Dict[str, Set[str]] = self.helper.stream_set
        self.helper.portal = lambda data: self.portal(data)
        self.helper_task: Optional[asyncio.Task] = None
        self.packet_buffer = []

    @property
    def is_initialized(self):
        return self.helper.is_initialized

    async def send(self, dest: list[str], content: str):
        try:
            await super().send(dest, content)
        except ConnectionClosed:
            # Eat the exception and buffer the data.
            self.packet_buffer.append((dest, content))

    async def handle_data(self, data: dict):
        raise NotImplementedError

    def portal(self, data):
        task = asyncio.create_task(self.handle_data(data))
        self.background_task.add(task)
        task.add_done_callback(self.background_task.discard)

    async def set_up(self):
        await super().set_up()
        if not self.helper_task or self.helper_task.done():
            logger.info("start helper task")
            self.helper_task = asyncio.create_task(self.helper())
        if len(self.packet_buffer):
            logger.info("resend buffered packets")
            packet_buffer, self.packet_buffer = self.packet_buffer, []
            for packet in packet_buffer:
                await self.send(packet[0], packet[1])

    async def check_alive(self, uid: str, request_id: int):
        response = ResponseContent(request_id, get_timestamp())
        await self.send([uid], response.to_content_str())

    async def check_initialized(self, uid: str, request_id: int):
        response = ResponseContent(request_id, self.is_initialized)
        await self.send([uid], response.to_content_str())

    async def subscribe(self, uid: str, stream: str):
        await self.helper.subscribe(uid, stream)

    async def unsubscribe(self, uid: str, stream: str):
        await self.helper.unsubscribe(uid, stream)

    async def react(self, packet: Packet):
        try:
            trade_content = self.load_content(packet.content)
        except Exception as e:
            logger.warning(f"loading content failed: {str(e)}")
            return
        if not isinstance(trade_content, RequestContent):
            return
        request_id = trade_content.request_id
        method = trade_content.method
        if method == "check_alive":
            await self.check_alive(packet.source, request_id)
        elif method == "check_initialized":
            await self.check_initialized(packet.source, request_id)
        if not self.is_initialized:
            return
        params = trade_content.params
        if method == "subscribe":
            await self.subscribe(packet.source, params[0])
        elif method == "unsubscribe":
            await self.unsubscribe(packet.source, params[0])

    def clean_up(self):
        if self.helper_task and not self.helper_task.done():
            logger.info("stopping helper")
            self.helper_task.cancel()

    async def wait_clean_up(self):
        if self.helper_task and not self.helper_task.done():
            await self.helper_task
        if len(self.background_task):
            logger.info("waiting background task")
            await asyncio.gather(*self.background_task)


class StrategyClient(TradeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.request_dict: Dict[int, asyncio.Future] = {}

    async def check_alive(self, uid: str):
        request_id = self.get_request_id()
        request_content = RequestContent(request_id, "check_alive", [])
        await self.send([uid], request_content.to_content_str())
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def check_initialized(self, uid: str):
        request_id = self.get_request_id()
        request_content = RequestContent(request_id, "check_initialized", [])
        await self.send([uid], request_content.to_content_str())
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def subscribe(self, uid: str, stream: str):
        request_id = self.get_request_id()
        request_content = RequestContent(request_id, "subscribe", [stream])
        await self.send([uid], request_content.to_content_str())
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def unsubscribe(self, uid: str, stream: str):
        request_id = self.get_request_id()
        request_content = RequestContent(request_id, "unsubscribe", [stream])
        await self.send([uid], request_content.to_content_str())
        self.request_dict[request_id] = asyncio.Future()
        return request_id

    async def on_response_content(self, content: ResponseContent):
        request_id = content.request_id
        result = content.result
        if content.request_id in self.request_dict:
            self.request_dict[request_id].set_result(result)

    async def on_stream_content(self, content: StreamContent):
        raise NotImplementedError

    async def react(self, packet: Packet):
        try:
            trade_content = self.load_content(packet.content)
        except Exception as e:
            logger.warning(f"loading content failed: {str(e)}")
            return
        if isinstance(trade_content, ResponseContent):
            await self.on_response_content(trade_content)
        elif isinstance(trade_content, StreamContent):
            await self.on_stream_content(trade_content)
