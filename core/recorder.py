import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import aiofiles


class Recorder:
    async def __call__(self, _):
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
                data = json.loads(f.readline().strip())
                self.now = data["timestamp"] / 1000
        except (FileNotFoundError, json.JSONDecodeError):
            self.now = time.time()
        self.buffer = []
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
        take_out, self.buffer = self.buffer, []
        self.rotate_if_should()
        async with aiofiles.open(self.base_path, "a") as f:
            for packet in take_out:
                await f.write(f"{json.dumps(packet)}\n")

    async def __call__(self, packet: dict):
        self.buffer.append(packet)
        task = asyncio.create_task(self.write_if_should())
        self.background_task.add(task)
        task.add_done_callback(self.background_task.discard)
