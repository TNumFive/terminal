import asyncio
import logging
from datetime import timedelta

from core import FileRecorder
from core import Server
from core import Client, EchoClient
from core import TimedRotatingFileHandler

logger = logging.getLogger("core")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s |+| %(name)s |+| %(levelname)s |+| %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
custom_handler = TimedRotatingFileHandler("./log/terminal.log", interval=timedelta(hours=1))
custom_handler.setLevel(logging.DEBUG)
custom_handler.setFormatter(formatter)
logger.addHandler(custom_handler)
logger.debug("set core.terminal logging level debug")


async def main():
    mux = Server(recorder=FileRecorder(interval=timedelta(hours=1)))
    rabit_hole = Client("rabit_hole", "ws://localhost:8080")
    echo_client = EchoClient("echo_client", "ws://localhost:8080")
    await asyncio.gather(mux(), echo_client(), rabit_hole())


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
