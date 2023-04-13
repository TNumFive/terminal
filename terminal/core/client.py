import asyncio
import json
import logging
from typing import Optional

from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
from websockets.legacy import client as ws_client
from websockets.legacy.server import WebSocketServerProtocol

from .utils import Packet

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
        self.background_task = set()

    async def login(self):
        logger.info(f"client:{self.uid} logging in")
        auth_obj = self.auth_func(self.uid)
        content = json.dumps(auth_obj)
        await self.websocket.send(Packet.to_login(self.uid, content))
        packet = Packet.from_server_message(await self.websocket.recv())
        if not len(packet.content):
            logger.info(f"client:{self.uid} logged in")
        else:
            logger.info(f"client:{self.uid} log in failed:{packet.content}")
            # Server will close the connection when auth failed,
            # and as it's due to auth fail, there is no need to re-connect.
            raise ConnectionClosedOK

    async def set_up(self):
        """
        This method will be called after logged in.
        """
        pass

    async def send(self, dest: list[str], content: str):
        """
        Helper method to make sure format checked
        """
        await self.websocket.send(Packet.to_server(dest, content))

    async def react(self, packet: dict):
        """
        All legal packet will go through here.
        """
        pass

    async def handler(self):
        while True:
            message = await self.websocket.recv()
            try:
                packet = Packet.from_server_message(message)
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
                logger.info(f"client:{self.uid} exiting")
                self.clean_up()
                await self.wait_clean_up()
                break
        logger.info(f"client:{self.uid} exit")


class EchoClient(Client):

    async def react(self, packet: Packet):
        await self.send([packet.source], packet.content)
