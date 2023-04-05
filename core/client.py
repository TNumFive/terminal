import json
import logging
from typing import Optional

from websockets.exceptions import ConnectionClosedError
from websockets.legacy import client as ws_client
from websockets.legacy.server import WebSocketServerProtocol

from .utils import get_error_line

logger = logging.getLogger(__name__)


class Client:
    def __init__(
            self,
            uid: str,
            uri: str = "ws://localhost:8080",
            auth_func=lambda uid: {"uid": uid},
    ):
        self.uid = uid
        self.uri = uri
        self.auth_func = auth_func
        self.websocket: Optional[WebSocketServerProtocol] = None

    async def login(self):
        """
        First message after connected to send.

        Json serialized obj with uid and credential attached.
        """
        auth_obj = self.auth_func(self.uid)
        await self.websocket.send(json.dumps(auth_obj))
        logger.info(f"client:{self.uid} logged in")

    @staticmethod
    def load(message) -> dict:
        """
        Load message to packet from terminal and do a sanity check.
        """
        packet: dict = json.loads(message)
        assert isinstance(packet, dict)
        assert len(packet) == 4
        assert isinstance(packet["timestamp"], float)
        assert isinstance(packet["source"], str)
        assert isinstance(packet["action"], str)
        assert isinstance(packet["content"], str)
        return packet

    async def send(self, dest: list[str], content: str):
        message = json.dumps({"destination": dest, "content": content})
        await self.websocket.send(message)

    async def react(self, packet: dict):
        """
        Client react to received packets.
        """
        pass

    async def handler(self):
        await self.login()
        async for message in self.websocket:
            try:
                packet = self.load(message)
            except (KeyError, AssertionError):
                logger.warning(f"client:{self.uid} loading failed: {get_error_line()}")
                continue
            except Exception as e:
                logger.warning(f"client:{self.uid} loading failed: {str(e)}")
                continue
            await self.react(packet)
        logger.info(f"client:{self.uid} logged out")

    async def __call__(self):
        async for websocket in ws_client.connect(self.uri):
            try:
                self.websocket = websocket
                await self.handler()
                self.websocket = None
                return
            except ConnectionClosedError as e:
                self.websocket = None
                logger.warning(f"client:{self.uid} connection error: {str(e)}")


class EchoClient(Client):

    async def react(self, packet: dict):
        await self.send([packet["source"]], packet["content"])
