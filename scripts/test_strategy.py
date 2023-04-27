import asyncio
import signal
import time
from typing import Dict, List

import pandas as pd

from terminal.extensions import set_up_logger, StrategyClient
from terminal.extensions.trade import StreamContent

logger = set_up_logger("test_strategy")


class TestStrategy(StrategyClient):

    def __init__(self, uid: str, uri: str = "ws://localhost:8080", auth_func=lambda uid: {"uid": uid}):
        super().__init__(uid, uri, auth_func)
        self.n1 = 10
        self.n2 = 20
        self.book_ticker_list: List[Dict[str:float]] = []

    async def set_up(self):
        await super().set_up()
        await self.subscribe("binance", "link_usdt@trade")
        await self.subscribe("binance", "link_usdt@bookTicker")
        await self.subscribe("binance", "link_usdt@book")
        await self.subscribe("binance", "link_usdt@kline")

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
            logger.info(f"buy link usdt at {btd.index[-1]} with {btd.iloc[-1]['ask']}")
        elif last and not now:
            logger.info(f"sell link usdt at {btd.index[-1]} with {btd.iloc[-1]['bid']}")

    def on_book_ticker(self, data: dict):
        self.book_ticker_list.append({
            "timestamp": time.time(),
            "ask": float(data["a"]),
            "bid": float(data["b"]),
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

    async def on_stream_content(self, stream_content: StreamContent):
        if stream_content.stream == "link_usdt@bookTicker":
            self.on_book_ticker(stream_content.data)


async def main():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(TestStrategy("test_strategy")())
    loop.add_signal_handler(signal.SIGINT, task.cancel)
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await task


if __name__ == "__main__":
    set_up_logger()
    asyncio.run(main(), debug=True)
