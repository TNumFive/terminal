import asyncio
import json
import logging
from typing import Optional

import aiomysql
from aiomysql import DictCursor

from .packet import Packet

logger = logging.getLogger(__name__)

CREATE_PACKET_TABLE = """CREATE TABLE IF NOT EXISTS `packet`
(
    `id`          VARCHAR(32) PRIMARY KEY,
    `sent_time`   BIGINT      NOT NULL,
    `route_time`  BIGINT      NOT NULL,
    `source`      VARCHAR(32) NOT NULL,
    `destination` TEXT        NOT NULL,
    `content`     TEXT        NOT NULL
)"""

CREATE_ROUTE_TABLE = """CREATE TABLE IF NOT EXISTS `route`
(
    `packet_id` VARCHAR(32) PRIMARY KEY,
    `user_id`   VARCHAR(32) NOT NULL
)"""

CREATE_USER_TABLE = """CREATE TABLE IF NOT EXISTS `user`
(
    `id`        VARCHAR(32) PRIMARY KEY,
    `pub_key`   VARCHAR(64) NOT NULL,
    `timestamp` BIGINT      NOT NULL
)"""


class DB:
    def __init__(
        self, host: str, username: str, password: str, database: str, port: int = 3306
    ):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.connection: Optional[aiomysql.Connection] = None
        self.connect_task: Optional[asyncio.Task] = None

    async def create_table(self, connection: aiomysql.Connection):
        async with connection.cursor(DictCursor) as cursor:
            cursor: DictCursor = cursor
            await cursor.execute(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = %s",
                [self.database],
            )
            table_name_list = [row["TABLE_NAME"] for row in await cursor.fetchall()]
            if "packet" not in table_name_list:
                await cursor.execute(CREATE_PACKET_TABLE)
            if "route" not in table_name_list:
                await cursor.execute(CREATE_ROUTE_TABLE)
            if "user" not in table_name_list:
                await cursor.execute(CREATE_USER_TABLE)

    async def connect(self):
        async def task():
            connection = await aiomysql.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                db=self.database,
                port=self.port,
            )
            await self.create_table(connection)
            self.connection = connection
            self.connect_task = None

        if self.connection and not self.connection.closed:
            return
        if not self.connect_task:
            self.connect_task = asyncio.create_task(task())
        await self.connect_task

    async def close(self):
        if not self.connection or self.connection.closed:
            return
        self.connection.close()
        await self.connection.ensure_closed()

    async def insert_packet_list(self, packet_list: list[Packet]):
        if not len(packet_list):
            return
        await self.connect()
        async with self.connection.cursor() as cursor:
            cursor: aiomysql.Cursor = cursor
            await cursor.executemany(
                (
                    "INSERT INTO `packet`(`id`,`sent_time`,`route_time`,`source`,`destination`,`content`)"
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                ),
                [
                    (
                        packet.id,
                        packet.sent_time,
                        packet.route_time,
                        packet.source,
                        json.dumps(packet.destination),
                        packet.content,
                    )
                    for packet in packet_list
                ],
            )
            await cursor.executemany(
                "INSERT INTO `route`(`packet_id`,`user_id`) VALUES(%s, %s)",
                [
                    [packet.id, user_id]
                    for packet in packet_list
                    for user_id in packet.destination
                ],
            )
            await self.connection.commit()

    async def insert_user(self, user_id: str, pub_key: str):
        await self.connect()
        async with self.connection.cursor() as cursor:
            cursor: aiomysql.Cursor = cursor
            sql = "INSERT INTO `user`VALUES(%s, %s, %s)"
            await cursor.execute(sql, [user_id, pub_key, Packet.get_timestamp()])
            await self.connection.commit()

    async def update_user(self, user_id: str, timestamp: int):
        await self.connect()
        async with self.connection.cursor() as cursor:
            cursor: aiomysql.Cursor = cursor
            sql = "UPDATE `user` SET `timestamp` = %s WHERE `id` = %s"
            await cursor.execute(sql, [timestamp, user_id])
            await self.connection.commit()

    async def select_user(self, user_id: str):
        await self.connect()
        async with self.connection.cursor(DictCursor) as cursor:
            cursor: DictCursor = cursor
            sql = "SELECT * FROM `user` WHERE `id` = %s"
            await cursor.execute(sql, [user_id])
            result: dict = await cursor.fetchone()
            return result
