import asyncio
import json
import logging

from websockets.exceptions import ConnectionClosed
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
            auth_func=lambda packet: (True, ""),
            auth_timeout=1,
            recorder=Recorder(),
    ) -> None:
        self.host = host
        self.port = port
        self.auth_timeout = auth_timeout
        self.auth_func = auth_func
        self.client_dict = {}
        self.recorder = recorder

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

    async def authenticate(self, websocket: WebSocketServerProtocol):
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=self.auth_timeout)
            packet = json.loads(message[:1024])
            uid = packet["uid"]
            assert isinstance(uid, str)
            assert uid.replace("_", "").isalnum()
            assert uid not in self.client_dict
            auth_result = self.auth_func(packet)
            await websocket.send(json.dumps(auth_result))
        except (KeyError, AssertionError):
            return False, get_error_line()
        except Exception as e:
            return False, str(e)
        if auth_result[0]:
            self.client_dict[uid] = packet
            self.client_dict[uid]["connection"] = websocket
            await self.record(self.decorate(uid, "login"))
            logger.info(f"client:{uid} logged in")
            return True, uid
        else:
            logger.info(f"client from {websocket.remote_address()} auth failed: {auth_result[1]}")
            return auth_result

    @staticmethod
    def load(message) -> dict:
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

        Note that there might be no reachable client in destination.
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

    async def logout(self, uid: str):
        """
        When the client lost connection, clean up resource.
        """
        del self.client_dict[uid]
        await self.record(self.decorate(uid, "logout"))
        logger.info(f"client:{uid} logged out")

    async def handler(self, websocket: WebSocketServerProtocol):
        login_result = await self.authenticate(websocket)
        if login_result[0]:
            uid = str(login_result[1])
        else:
            return
        while True:
            try:
                message = await websocket.recv()
                packet = self.load(message)
            except ConnectionClosed as e:
                logger.warning(f"connection closed: {str(e)}")
                break
            except (KeyError, AssertionError):
                logger.warning(f"loading failed: {get_error_line()}")
                continue
            except json.JSONDecodeError as e:
                logger.warning(f"loading failed: {str(e)}")
                continue
            except Exception as e:
                logger.warning(f"loading failed: {str(e)}")
                break
            packet = self.decorate(uid, "message", packet)
            self.route(packet)
            await self.record(packet)
            await self.react(packet)
        await self.logout(uid)

    async def set_up(self):
        """
        This method will be called after server start serving.
        """
        pass

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
        server = await serve(self.handler, self.host, self.port)
        await self.set_up()
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("Server stopping")
        self.clean_up()
        await self.wait_clean_up()
        server.close()
        await server.wait_closed()
