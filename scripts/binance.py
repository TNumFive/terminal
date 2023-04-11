import asyncio
import json
import signal
import time
from typing import Optional, Dict

from websockets.exceptions import ConnectionClosed
from websockets.legacy import client as ws_client
from websockets.legacy.server import WebSocketServerProtocol

from terminal.core import set_up_logger
from terminal.extensions import ExchangeClient

logger = set_up_logger("binance")


class BinanceRawHelper:

    def __init__(self, ws_url="wss://stream.binance.com", init_stream="btcusdt@kline_1m", request_interval=0.5):
        self.ws_url = ws_url
        self.init_stream = init_stream
        self.is_initialized = False
        self.message_buffer: list[dict] = []
        self.websocket: Optional[WebSocketServerProtocol] = None
        self.stream_dict: Dict[str, list] = {}
        self.portal = lambda data: NotImplementedError
        self.last_request_time = time.time()
        self.request_interval = request_interval

    @staticmethod
    def get_request_id():
        return int(time.time() * 1000)

    async def wait_if_too_frequent(self):
        while True:
            diff = time.time() - self.last_request_time
            if diff < self.request_interval:
                await asyncio.sleep(self.request_interval - diff)
            else:
                break
        self.last_request_time = time.time()

    async def send(self, message):
        await self.wait_if_too_frequent()
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
                logger.info("binance helper is initialized")
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
            logger.info("no binance websocket right now")
            return
        logger.info("wait close binance websocket")
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


class BinanceExchangeClient(ExchangeClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid},
                 binance_helper=BinanceRawHelper()):
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
            logger.warning(f"receive unknown data: {data}")

    def helper_portal(self, data):
        task = asyncio.create_task(self.handle_binance(data))
        self.background_task.add(task)
        task.add_done_callback(self.background_task.discard)

    async def set_up(self):
        await super().set_up()
        if not self.binance_task or self.binance_task.done():
            logger.info("start binance helper task")
            self.binance_task = asyncio.create_task(self.binance_helper())
            self.binance_task.add_done_callback(self.background_task.discard)
        if len(self.packet_buffer):
            logger.info("resend buffered packets")
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
            logger.info("try stop binance helper")
            await self.binance_helper.stop()
        if len(self.background_task):
            logger.info("wait background task")
            await asyncio.gather(*self.background_task)


async def main():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(BinanceExchangeClient("binance")())
    loop.add_signal_handler(signal.SIGINT, task.cancel)
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await task


if __name__ == "__main__":
    set_up_logger()
    asyncio.run(main(), debug=True)
