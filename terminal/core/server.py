import asyncio
import logging

from websockets.exceptions import ConnectionClosed
from websockets.legacy.protocol import broadcast
from websockets.legacy.server import serve, WebSocketServerProtocol

from .recorder import Recorder
from .utils import Packet

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
        self.conn_dict = {}
        self.recorder = recorder
        self.background_task = set()

    async def record(self, packet: Packet):
        """
        Record data that received for later playback.
        """
        await self.recorder(packet)

    async def authenticate(self, websocket: WebSocketServerProtocol):
        """
        If anything wrong, raise exception.

        If any exception, auth failed.
        """
        message = await websocket.recv()
        packet = Packet.from_client_login(message[:1024])
        assert packet.source not in self.client_dict, "source already exist"
        auth_result = self.auth_func(packet)
        if auth_result[0]:
            await websocket.send(packet.to_client(""))
            self.client_dict[packet.source] = packet
            self.conn_dict[packet.source] = websocket
            logger.info(f"client:{packet.source} logged in")
            await self.record(packet)
        else:
            await websocket.send(packet.to_client(auth_result[1]))
            assert False, auth_result[1]
        return packet.source

    def route(self, packet: Packet):
        """
        Route received packet to other client.

        Note that there might be no reachable client in destination.
        """
        connection_set = set()
        for dest in packet.destination:
            if dest in self.conn_dict:
                connection_set.add(self.conn_dict[dest])
        source_connection = self.client_dict[packet.source]
        if source_connection in connection_set:
            connection_set.remove(source_connection)
        if len(connection_set):
            broadcast(connection_set, packet.to_route())

    async def react(self, packet: Packet):
        """
        Terminal can react to packet as well if needed.
        """
        pass

    async def logout(self, uid: str):
        """
        When the client lost connection, clean up resource.
        """
        del self.client_dict[uid]
        del self.conn_dict[uid]
        logger.info(f"client:{uid} logged out")
        await self.record(Packet.to_logout(uid))

    async def handler(self, websocket: WebSocketServerProtocol):
        try:
            uid = await asyncio.wait_for(self.authenticate(websocket), self.auth_timeout)
        except Exception as e:
            # If any exception, auth failed.
            address = websocket.remote_address
            logger.info(f"client from {address} login failed: {str(e)}")
            return
        while True:
            try:
                message = await websocket.recv()
                packet = Packet.from_client_message(uid, message)
            except ConnectionClosed as e:
                logger.warning(f"client:{uid} connection closed: {str(e)}")
                break
            except Exception as e:
                # If any exception, drop the message
                logger.warning(f"client:{uid} loading failed: {str(e)}")
                continue
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
        try:
            await self.set_up()
            await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("Server stopping")
        finally:
            self.clean_up()
            await self.wait_clean_up()
            server.close()
            await server.wait_closed()
            logger.info("Server stopped")
