"""
Central Server that receive, route and record packet among clients
"""
import asyncio
import json
import logging
import signal

from websockets.legacy.protocol import broadcast
from websockets.legacy.server import serve, WebSocketServerProtocol

from .recorder import Recorder
from .utils import get_timestamp, get_error_line

logger = logging.getLogger(__name__)


class Server:
    def __init__(
            self,
            host="",
            port=8080,
            auth_func=lambda packet: [True, ""],
            auth_timeout=1,
            recorder=Recorder(),
    ) -> None:
        self.host = host
        self.port = port
        self.auth_timeout = auth_timeout
        self.auth_func = auth_func
        self.stop = None
        self.client_dict = {}
        self.recorder = recorder

    async def authenticate(self, websocket: WebSocketServerProtocol):
        """
        Wait for the first message after connection, then do a sanity check.

        Pass the checked data to auth func for further authentication.

        The auth func has to return [True, None] if passed else [False, error_message].
        """
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=self.auth_timeout)
            packet = json.loads(message[:1024])
            uid = packet["uid"]
            assert isinstance(uid, str)
            assert uid.replace("_", "").isalnum()
            assert uid not in self.client_dict
            auth_result = self.auth_func(packet)
        except (KeyError, AssertionError):
            return False, get_error_line()
        except Exception as e:
            return False, str(e)
        if auth_result[0]:
            auth_result[1] = uid
            self.client_dict[uid] = packet
            self.client_dict[uid]["connection"] = websocket
            await self.record(self.decorate(uid, "login"))
            logger.info(f"client:{uid} logged in")
        else:
            logger.info(f"client from {websocket.remote_address()} auth failed: {auth_result[1]}")
        return auth_result

    async def logout(self, uid: str):
        """
        When the client lost connection, clean up resource.
        """
        del self.client_dict[uid]
        await self.record(self.decorate(uid, "logout"))
        logger.info(f"client:{uid} logged out")

    @staticmethod
    def decorate(uid: str, action_type: str, packet=None):
        """
        Add or update field of timestamp, source, type to packet.
        """
        if packet is None:
            packet = dict()
        else:
            packet = packet.copy()
        packet["timestamp"] = get_timestamp()
        packet["source"] = uid
        packet["action"] = action_type
        return packet

    async def record(self, packet: dict):
        """
        Record data that received for later playback.
        """
        packet = packet.copy()
        await self.recorder(packet)

    @staticmethod
    def load(message) -> dict:
        """
        Function that deserialize data.
        """
        packet: dict = json.loads(message)
        assert isinstance(packet, dict)
        assert len(packet) == 2
        destination = packet["destination"]
        assert isinstance(destination, list)
        assert all(isinstance(item, str) for item in destination)
        assert isinstance(packet["content"], str)
        return packet

    def route(self, packet: dict):
        """
        Route received packet to other client.

        Note that there might be no reachable client.
        """
        temp = packet.copy()
        connection_set = set()
        for dest in temp["destination"]:
            if dest in self.client_dict:
                client_info = self.client_dict[dest]
                connection_set.add(client_info["connection"])
        source_connection = self.client_dict[temp["source"]]["connection"]
        if source_connection in connection_set:
            connection_set.remove(source_connection)
        del temp["destination"]
        if len(connection_set):
            broadcast(connection_set, json.dumps(temp))

    async def react(self, packet: dict):
        """
        Terminal can react to packet as well if needed.
        """
        pass

    async def handler(self, websocket: WebSocketServerProtocol):
        """
        Client login, then receive, route, record and react.
        """
        login_result = await self.authenticate(websocket)
        if login_result[0]:
            uid = login_result[1]
        else:
            return
        async for message in websocket:
            try:
                packet = self.load(message)
            except (KeyError, AssertionError):
                logger.warning(f"loading failed: {get_error_line()}")
                continue
            except Exception as e:
                logger.warning(f"loading failed: {str(e)}")
                continue
            packet = self.decorate(uid, "message", packet)
            self.route(packet)
            await self.record(packet)
            await self.react(packet)
        await self.logout(uid)

    async def __call__(self):
        loop = asyncio.get_running_loop()
        self.stop = loop.create_future()
        loop.add_signal_handler(signal.SIGINT, self.stop.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self.stop.set_result, None)
        async with serve(self.handler, self.host, self.port):
            await self.stop
