import asyncio
import signal

from terminal.core import Client, EchoClient
from terminal.core import FileRecorder
from terminal.core import Server
from terminal.extensions import set_up_logger


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
        self.background_task.add(create_task(Client("rabit_hole")()))
        self.background_task.add(create_task(EchoClient("echo_client")()))


async def main():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(Mux()())
    loop.add_signal_handler(signal.SIGINT, task.cancel)
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await task


if __name__ == "__main__":
    set_up_logger()
    asyncio.run(main(), debug=True)
