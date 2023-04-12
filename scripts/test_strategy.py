import asyncio
import signal
import time
from typing import Dict, List

import pandas as pd

from terminal.extensions import set_up_logger, StrategyClient

logger = set_up_logger("test_strategy")


class TestStrategy(StrategyClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.n1 = 10
        self.n2 = 20
        self.book_ticker_list: List[Dict[str:float]] = []

    async def set_up(self):
        await super().set_up()
        await asyncio.sleep(1)
        await self.subscribe("binance", "enjusdt@bookTicker")
        await self.subscribe("binance", "eosusdt@bookTicker")
        await self.subscribe("binance", "linkusdt@bookTicker")
        await self.subscribe("binance", "maticusdt@bookTicker")
        await self.subscribe("binance", "trxusdt@bookTicker")
        await self.subscribe("binance", "vetusdt@bookTicker")
        await self.subscribe("binance", "xlmusdt@bookTicker")

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
            logger.info(f"buy link usdt at {btd.index[-1]}")
        elif last and not now:
            logger.info(f"sell link usdt at {btd.index[-1]}")

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
        if "stream" in data and data["stream"] == "linkusdt@bookTicker":
            self.on_book_ticker(data["data"])


async def main():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(TestStrategy("test_strategy")())
    loop.add_signal_handler(signal.SIGINT, task.cancel)
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await task


if __name__ == "__main__":
    set_up_logger()
    asyncio.run(main(), debug=True)
