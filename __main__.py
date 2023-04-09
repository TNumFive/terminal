import asyncio
import logging
import signal
import time
from typing import Dict, List

import pandas as pd
from core import FileRecorder
from core import Server
from extensions import BinanceExchangeClient, StrategyClient

logger = logging.getLogger("core")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s |+| %(name)s |+| %(levelname)s |+| %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger = logging.getLogger("extensions")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s |+| %(name)s |+| %(levelname)s |+| %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


class TestStrategy(StrategyClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.n1 = 10
        self.n2 = 20
        self.book_ticker_list: List[Dict[str:float]] = []
        self.timer = time.time()

    async def set_up(self):
        await super().set_up()
        await asyncio.sleep(1)
        await self.subscribe("binance", "linkusdt@bookTicker")

    def trade_action(self):
        if len(self.book_ticker_list) < 2:
            return
        btd = pd.DataFrame.from_records(self.book_ticker_list)
        btd.index = pd.to_datetime(btd["timestamp"], utc=True, unit="s")
        btd["mid"] = btd["ask"] + btd["ask"]
        btd["mid"] /= 2
        btd["sma1"] = btd["mid"].rolling(f"{self.n1}s").mean()
        btd["sma2"] = btd["mid"].rolling(f"{self.n2}s").mean()
        last = btd.iloc[-2]["sma1"] > btd.iloc[-2]["sma2"]
        now = btd.iloc[-1]["sma1"] > btd.iloc[-1]["sma2"]
        if not last and now:
            print(f"{btd.index[-1]} buy")
        elif last and not now:
            print(f"{btd.index[-1]} sell")
        else:
            if time.time() - 1 > self.timer:
                self.timer = time.time()
                print(f"{btd.index[-1]} wait")

    def on_book_ticker(self, data: dict):
        self.book_ticker_list.append({
            "timestamp": time.time(),
            "bid": float(data["b"]),
            "ask": float(data["b"]),
        })
        while True:
            timespan = self.book_ticker_list[-1]["timestamp"]
            timespan -= self.book_ticker_list[0]["timestamp"]
            if timespan > max(self.n1, self.n2) + 1:
                self.book_ticker_list.pop(0)
            else:
                break
        if timespan > max(self.n1, self.n2):
            self.trade_action()

    async def react(self, packet: dict):
        data = await super().react(packet)
        if "stream" in data:
            self.on_book_ticker(data["data"])


class Mux(Server):

    def __init__(
            self,
            host="",
            port=8080,
            auth_func=lambda packet: [True, ""],
            auth_timeout=1,
            recorder=FileRecorder(),
    ) -> None:
        super().__init__(host, port, auth_func, auth_timeout, recorder)

    async def set_up(self):
        await super().set_up()
        create_task = asyncio.create_task
        self.background_task.add(create_task(BinanceExchangeClient("binance")()))
        self.background_task.add(create_task(TestStrategy("TestStrategy")()))


async def main():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(Mux()())
    loop.add_signal_handler(signal.SIGINT, task.cancel)
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await task


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
