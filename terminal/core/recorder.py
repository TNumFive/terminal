import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import aiofiles

from .utils import Packet

logger = logging.getLogger(__name__)


class Recorder:
    async def __call__(self, _: Packet):
        pass


class FileRecorder(Recorder):
    def __init__(self, interval=timedelta(hours=1), record_dir="./record"):
        self.interval = interval.total_seconds()
        class_name = __class__.__name__
        self.base_name = f"{class_name}.log"
        self.format = f"{class_name}.%Y%m%d_%H%M%S.log"
        self.record_dir = Path(record_dir)
        self.record_dir.mkdir(parents=True, exist_ok=True)
        self.base_path = self.record_dir.joinpath(self.base_name)
        try:
            with self.base_path.open("r") as f:
                packet = Packet.from_str(f.readline().strip())
                self.now = packet.route_time / 1000
        except (FileNotFoundError, json.JSONDecodeError):
            self.now = time.time()
        self.buffer: List[Packet] = []
        self.background_task = set()

    def rotate_if_should(self):
        now = time.time()
        if now <= self.now + self.interval:
            return
        try:
            rotate_name = datetime.fromtimestamp(self.now).strftime(self.format)
            new_path = self.record_dir.joinpath(rotate_name)
            os.rename(self.base_path, new_path)
        finally:
            self.now = now

    async def write_if_should(self):
        if not len(self.buffer):
            return
        buffer, self.buffer = self.buffer, []
        self.rotate_if_should()
        async with aiofiles.open(self.base_path, "a") as f:
            for packet in buffer:
                await f.write(f"{packet.to_str()}\n")

    async def __call__(self, packet: Packet):
        self.buffer.append(packet)
        task = asyncio.create_task(self.write_if_should())
        self.background_task.add(task)
        task.add_done_callback(self.background_task.discard)
