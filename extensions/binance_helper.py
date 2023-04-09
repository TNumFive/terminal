import asyncio
import json
import logging
import time
from typing import Optional, Dict

from websockets.legacy.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosed
from websockets.legacy import client as ws_client

logger = logging.getLogger(__name__)


class BinanceRawHelper:

    def __init__(self, ws_url="wss://stream.binance.com", init_stream="btcusdt@kline_1m"):
        self.ws_url = ws_url
        self.init_stream = init_stream
        self.is_initialized = False
        self.message_buffer: list[dict] = []
        self.websocket: Optional[WebSocketServerProtocol] = None
        self.stream_dict: Dict[str, list] = {}
        self.portal = lambda data: NotImplementedError

    @staticmethod
    def get_request_id():
        return int(time.time() * 1000)

    async def send(self, message):
        try:
            await self.websocket.send(message)
        except ConnectionClosed as e:
            logger.warning(f"connection closed: {str(e)}")
            self.message_buffer.append(message)

    async def resubscribe(self):
        stream_list = list(self.stream_dict.keys())
        stream_list.remove(self.init_stream)
        if not len(stream_list):
            return
        request = {
            "method": "SUBSCRIBE",
            "params": stream_list,
            "id": self.get_request_id()
        }
        await self.websocket.send(json.dumps(request))

    async def resend_after_reconnect(self):
        message_buffer, self.message_buffer = self.message_buffer, []
        for message in message_buffer:
            await self.send(message)

    async def handler(self):
        await self.resubscribe()
        await self.resend_after_reconnect()
        async for message in self.websocket:
            try:
                data: dict = json.loads(message)
                assert isinstance(data, dict), "data type not dict"
            except Exception as e:
                logger.warning(f"load message failed: {str(e)}")
                continue
            self.portal(data)

    async def __call__(self, *args, **kwargs):
        ws_url = self.ws_url + "/stream?streams=" + self.init_stream
        async for websocket in ws_client.connect(ws_url):
            try:
                if self.init_stream not in self.stream_dict:
                    self.stream_dict[self.init_stream] = []
                self.websocket = websocket
                self.is_initialized = True
                await self.handler()
            except asyncio.CancelledError:
                logger.warning(f"websocket exit")
                return
            except Exception as e:
                logger.warning(f"websocket closed: {str(e)}")
            finally:
                self.is_initialized = False

    async def stop(self):
        if not self.websocket:
            return
        await self.websocket.close()

    async def subscribe(self, uid: str, stream: str):
        if stream not in self.stream_dict:
            self.stream_dict[stream] = [uid]
            request = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": self.get_request_id()
            }
            await self.send(json.dumps(request))
        dest = self.stream_dict[stream]
        if uid not in dest:
            dest.append(uid)

    async def unsubscribe(self, uid: str, stream: str):
        if stream not in self.stream_dict:
            return
        dest = self.stream_dict[stream]
        if uid in dest:
            dest.remove(uid)
        if len(dest):
            return
        request = {
            "method": "UNSUBSCRIBE",
            "params": [stream],
            "id": self.get_request_id()
        }
        try:
            await self.websocket.send(json.dumps(request))
        except ConnectionClosed:
            pass
        finally:
            # Just remove the key to prevent resubscribe after reconnect.
            self.stream_dict.pop(stream)
