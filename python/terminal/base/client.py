import asyncio
import json
import logging
from typing import Optional

from websockets.legacy.client import connect, WebSocketClientProtocol

from .crypto import public_box, secret_box, encrypt, decrypt
from .packet import Packet

logger = logging.getLogger(__name__)


class Client:
    def __init__(
        self,
        user_id: str,
        pub_key: str,
        pri_key: str,
        server_pub_key: str,
        uri: str,
        record_packet: bool = True,
    ):
        self.user_id = user_id
        self.pub_key = pub_key
        self.pri_key = pri_key
        self.server_pub_key = server_pub_key
        self.secret_box = None
        self.uri = uri
        self.record_packet = record_packet
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.connect_task: Optional[asyncio.Task] = None

    async def log_in(self):
        config = {
            "timestamp": Packet.get_timestamp(),
            "record_packet": self.record_packet,
        }
        config_str = json.dumps(config)
        box = public_box(self.pri_key, self.server_pub_key)
        content = encrypt(box, config_str)
        packet = Packet(Packet.get_timestamp(), 0, self.user_id, [], content)
        websocket = await connect(self.uri)
        await websocket.send(packet.dumps())
        response = Packet.loads(await websocket.recv())
        secret_key = decrypt(box, response.content)
        self.secret_box = secret_box(secret_key)
        self.websocket = websocket
        self.connect_task = None

    async def connect(self):
        if self.websocket and self.websocket.open:
            return
        if not self.connect_task:
            self.connect_task = asyncio.create_task(self.log_in())
        await self.connect_task

    async def send_packet(self, packet: Packet):
        packet.content = encrypt(self.secret_box, packet.content)
        await self.connect()
        await self.websocket.send(packet.dumps())

    async def send(self, destination: list[str], message: str):
        packet = Packet(
            Packet.get_timestamp(),
            0,
            self.user_id,
            destination,
            message,
        )
        await self.send_packet(packet)

    async def recv(self):
        await self.connect()
        return await self.websocket.recv()

    async def recv_packet(self):
        message = await self.recv()
        packet = Packet.loads(message)
        packet.content = decrypt(self.secret_box, packet.content)
        return packet

    async def close(self, code: int = 1000, reason: str = ""):
        if self.websocket is None or self.websocket.closed:
            return
        await self.websocket.close(code, reason)
