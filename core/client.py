import asyncio
import json
import logging
from typing import Optional

from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
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
        auth_obj = self.auth_func(self.uid)
        logger.info(f"client:{self.uid} logging in")
        await self.websocket.send(json.dumps(auth_obj))
        auth_result = json.loads(await self.websocket.recv())
        if auth_result[0]:
            logger.info(f"client:{self.uid} logged in")
        else:
            logger.info(f"client:{self.uid} log in failed:{auth_result[-1]}")
            # Server will close the connection when auth failed,
            # and as it's due to auth fail, there is no need to re-connect.
            raise ConnectionClosedOK

    async def set_up(self):
        """
        This method will be called after logged in.
        """
        pass

    @staticmethod
    def load(message) -> dict:
        """
        Helper method to make sure format checked
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
        """
        Helper method to make sure format checked
        """
        message = json.dumps({"destination": dest, "content": content})
        await self.websocket.send(message)

    async def react(self, packet: dict):
        """
        All legal packet will go through here.
        """
        pass

    async def handler(self):
        while True:
            message = await self.websocket.recv()
            try:
                packet = self.load(message)
            except (KeyError, AssertionError):
                logger.warning(f"client:{self.uid} loading failed: {get_error_line()}")
                continue
            except Exception as e:
                logger.warning(f"client:{self.uid} loading failed: {str(e)}")
                continue
            await self.react(packet)

    def clean_up(self):
        """
        This method will be called after connection closed.
        """
        pass

    async def wait_clean_up(self):
        """
        This method will be called after clean_up called.
        """
        pass

    async def __call__(self):
        async for websocket in ws_client.connect(self.uri):
            try:
                self.websocket = websocket
                await self.login()
                await self.set_up()
                await self.handler()
            except ConnectionClosedError as e:
                logger.warning(f"client:{self.uid} connection error: {str(e)}")
                continue
            except (asyncio.CancelledError, ConnectionClosedOK):
                # When there is no need to re-connect, just exit.
                logger.info(f"client:{self.uid} logged out")
                return
            finally:
                self.clean_up()
                await self.wait_clean_up()


class EchoClient(Client):

    async def react(self, packet: dict):
        await self.send([packet["source"]], packet["content"])
