import asyncio
import logging
import signal

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

    async def set_up(self):
        await super().set_up()
        await asyncio.sleep(1)
        await self.check_alive("binance")
        await self.check_initialized("binance")
        await self.subscribe("binance", "btcusdt@bookTicker")

    async def react(self, packet: dict):
        await super().react(packet)
        print(packet)


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
        self.background_task = set()

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
