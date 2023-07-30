import asyncio
import json
import logging

from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServerProtocol, serve

from .crypto import public_box, secret_box, generate_secret_key, encrypt, decrypt
from .db import DB
from .packet import Packet

logger = logging.getLogger(__name__)

ClientDictType = dict[str, WebSocketServerProtocol]
ConnectionDictType = dict[WebSocketServerProtocol, Packet]
ConfigDictType = dict[str, dict]


class Server:
    def __init__(
        self,
        pub_key: str,
        pri_key: str,
        db: DB,
        host: str = "127.0.0.1",
        port: int = 5555,
        timeout: float = 1.0,
    ):
        self.pub_key = pub_key
        self.pri_key = pri_key
        self.db = db
        self.host = host
        self.port = port
        self.timeout = max(timeout, 1)
        # websocket <> first_packet
        self.connection_dict: ConnectionDictType = {}
        # user_id <> websocket
        self.client_dict: ClientDictType = {}
        # user_id <> config
        self.config_dict: ConfigDictType = {}
        # stored packet wait for insert into database
        self.packet_list: list[Packet] = []
        # for task that is created and detached
        self.background_task_set: set[asyncio.Task] = set()

    async def log_in(self, websocket: WebSocketServerProtocol):
        try:
            # first check the format
            message = await asyncio.wait_for(websocket.recv(), self.timeout)
            packet = Packet.pack(message)
            assert len(packet.source), "invalid user_id"
            assert not len(packet.destination), "first packet must send to server"
            assert packet.source not in self.client_dict, "user already logged in"
        except (asyncio.TimeoutError, AssertionError) as e:
            logger.debug(f"client from {websocket.remote_address} log in failed: {e}")
            return False
        try:
            # second try to verify by pub_key
            last_login = await self.db.select_user(packet.source)
            box = public_box(self.pri_key, last_login["pub_key"])
            config_str = decrypt(box, packet.content)
            config = json.loads(config_str)
            assert config["timestamp"] > last_login["timestamp"], "invalid timestamp"
        except (json.JSONDecodeError, AssertionError) as e:
            logger.debug(f"client from {websocket.remote_address} verify failed: {e}")
            return False
        try:
            # update timestamp and respond secret_key
            await self.db.update_user(packet.source, config["timestamp"])
            secret_key = generate_secret_key()
            config["box"] = secret_box(secret_key)
            response = Packet(
                Packet.get_timestamp(),
                Packet.get_timestamp(),
                "",
                [packet.source],
                encrypt(box, secret_key),
            )
            await websocket.send(response.to_string())
        except ConnectionClosed as e:
            logger.debug(f"respond client:{packet.source} failed : {str(e)}")
            return False
        logger.debug(f"new client:{packet.source} from {websocket.remote_address}")
        self.connection_dict[websocket] = packet
        self.client_dict[packet.source] = websocket
        self.config_dict[packet.source] = config
        return True

    async def log_out(self, websocket: WebSocketServerProtocol):
        """
        Handle the part added by new log_in; Use `websocket` and `first_packet` to locate newly added.
        """
        packet = self.connection_dict.pop(websocket, None)
        source = packet.source if packet else None
        self.client_dict.pop(source, None)
        self.config_dict.pop(source, None)
        logger.debug(f"client:{packet.source} from {websocket.remote_address} log out")

    async def record(self, packet: Packet):
        async def insert_task():
            packet_list, self.packet_list = self.packet_list, []
            await self.db.insert_packet_list(packet_list)

        self.packet_list.append(packet)
        task = asyncio.create_task(insert_task())
        self.background_task_set.add(task)
        task.add_done_callback(self.background_task_set.discard)

    async def route(self, packet: Packet):
        destination, packet.destination = packet.destination, []
        content = packet.content
        for user_id in destination:
            try:
                websocket = self.client_dict[user_id]
                packet.destination = [user_id]
                packet.content = encrypt(self.config_dict[user_id]["box"], content)
                await websocket.send(packet.to_string())
            except ConnectionClosed as e:
                # other connection shall not affect this one
                logger.info(f"route packet to {user_id} failed: {e}")
        packet.destination = destination
        packet.content = content

    async def manage(self, packet: Packet):
        # send to no one means send to server only
        pass

    async def handler(self, websocket: WebSocketServerProtocol):
        if not await self.log_in(websocket):
            return
        user_id = self.connection_dict[websocket].source
        config = self.config_dict[user_id]
        try:
            async for message in websocket:
                packet = Packet.pack(message)
                packet.source = user_id
                packet.route_time = Packet.get_timestamp()
                packet.content = decrypt(
                    self.config_dict[user_id]["box"], packet.content
                )
                if config.get("record_packet", False):
                    await self.record(packet)
                await self.manage(packet)
                await self.route(packet)
        except ConnectionClosed as e:
            logger.debug(
                f"connection of {user_id} from {websocket.remote_address} closed: {e}"
            )
        finally:
            await self.log_out(websocket)

    async def __call__(self):
        await self.db.connect()
        server = await serve(self.handler, self.host, self.port)
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            logger.debug("Server stopping")
        finally:
            server.close()
            await server.wait_closed()
        await asyncio.gather(*self.background_task_set)
        await self.db.close()
