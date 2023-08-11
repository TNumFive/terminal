import asyncio
import logging
from typing import Optional

from websockets.exceptions import ConnectionClosed

from .base import Client, Packet, decrypt
from .json_rpc import Request, loads, Error, Response

logger = logging.getLogger(__name__)


class Navigator(Client):
    def __init__(
        self,
        user_id: str,
        pub_key: str,
        pri_key: str,
        server_pub_key: str,
        uri: str,
        record_packet: bool = True,
    ):
        super().__init__(user_id, pub_key, pri_key, server_pub_key, uri, record_packet)
        self.request_dict: dict[str, asyncio.Future] = {}
        self.recv_loop_task: Optional[asyncio.Task] = None

    async def process(self, packet: Packet, request: Request):
        pass

    async def recv_loop(self):
        while True:
            message = await self.websocket.recv()
            packet = Packet.loads(message)
            packet.content = decrypt(self.secret_box, packet.content)
            obj = loads(packet.content)
            if isinstance(obj, Error):
                logger.debug(
                    f"JSON-RPC error:{obj.dumps()} from client:{packet.source}"
                )
                await self.send([packet.source], Response("null", error=obj).dumps())
            elif isinstance(obj, Response):
                if obj.id == "null":
                    logger.warning(
                        f"Client:{packet.source} responds error:{obj.dumps()}"
                    )
                elif obj.id in self.request_dict:
                    future = self.request_dict.pop(obj.id)
                    future.set_result(obj)
                else:
                    logger.warning(
                        f"Unknown response:{obj.dumps()} from client:{packet.source}"
                    )
            elif isinstance(obj, Request):
                logger.info(
                    f"Receive request:{obj.dumps()} from client:{packet.source}"
                )
                await self.process(packet, obj)

    async def wait_recv_loop(self):
        try:
            if self.recv_loop_task:
                await self.recv_loop_task
        except ConnectionClosed:
            pass

    async def log_in(self):
        await super().log_in()
        await self.wait_recv_loop()
        self.recv_loop_task = asyncio.create_task(self.recv_loop())

    async def send_request(self, destination: list[str], request: Request):
        future = asyncio.get_event_loop().create_future()
        if request.id:
            self.request_dict[request.id] = future
        else:
            future.set_result(None)
        await self.send(destination, request.dumps())
        return future

    async def close(self, code: int = 1000, reason: str = ""):
        await super().close(code, reason)
        await self.wait_recv_loop()
