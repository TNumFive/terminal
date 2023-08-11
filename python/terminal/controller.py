import logging

from .base import Server, Packet, DB, encrypt
from .json_rpc import loads, Response, Request, Error

logger = logging.getLogger(__name__)


class Controller(Server):
    def __init__(
        self,
        pub_key: str,
        pri_key: str,
        db: DB,
        host: str = "127.0.0.1",
        port: int = 5555,
        timeout: float = 1.0,
    ):
        super().__init__(pub_key, pri_key, db, host, port, timeout)

    async def send_to_client(self, user_id, message: str):
        content = encrypt(self.config_dict[user_id]["box"], message)
        packet = Packet(
            Packet.get_timestamp(), Packet.get_timestamp(), "", [user_id], content
        )
        websocket = self.client_dict[user_id]
        await websocket.send(packet.dumps())

    async def process(self, packet: Packet, request: Request):
        pass

    async def manage(self, packet: Packet):
        # send to no one means send to server only
        if len(packet.destination):
            return
        obj = loads(packet.content)
        if isinstance(obj, Error):
            logger.debug(f"JSON-RPC error:{obj.dumps()} from client:{packet.source}")
            await self.send_to_client(
                packet.source, Response("null", error=obj).dumps()
            )
        elif isinstance(obj, Response):
            logger.warning(f"Client:{packet.source} responds {obj.dumps()}")
        elif isinstance(obj, Request):
            logger.info(f"Receive request:{obj.dumps()} from client:{packet.source}")
            await self.process(packet, obj)
