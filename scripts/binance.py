import asyncio
import json
import signal

from terminal.extensions import ExchangeClient, set_up_logger, ExchangeRawHelper, StreamContent

logger = set_up_logger("binance")


class BinanceRawHelper(ExchangeRawHelper):
    def __init__(
            self,
            ws_url="wss://stream.binance.com",
            init_stream="btcusdt@kline_1m",
            proxy=None,
            websocket_send_interval=0.5,
            max_connect_retry_times=10
    ):
        super().__init__(ws_url, proxy, websocket_send_interval, max_connect_retry_times)
        self.init_stream = init_stream
        self.ws_url = self.ws_url + "/stream?streams=" + self.init_stream

    async def set_up(self):
        if self.init_stream not in self.stream_dict:
            self.stream_dict[self.init_stream] = []
        await super().set_up()
        # do the resubscribe
        stream_list = list(self.stream_dict.keys())
        stream_list.remove(self.init_stream)
        if not len(stream_list):
            return
        request = {
            "method": "SUBSCRIBE",
            "params": stream_list,
            "id": self.get_request_id()
        }
        await self.websocket_send(json.dumps(request))

    async def subscribe(self, uid: str, stream: str):
        if stream not in self.stream_dict:
            self.stream_dict[stream] = [uid]
            request = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": self.get_request_id()
            }
            await self.websocket_send(json.dumps(request))
            return
        dest = self.stream_dict[stream]
        if uid not in dest:
            dest.append(uid)

    async def unsubscribe(self, uid: str, stream: str):
        if stream not in self.stream_dict:
            return
        dest = self.stream_dict[stream]
        if uid in dest:
            dest.remove(uid)
        if len(dest):
            return
        request = {
            "method": "UNSUBSCRIBE",
            "params": [stream],
            "id": self.get_request_id()
        }
        await self.websocket_send(json.dumps(request))
        self.stream_dict.pop(stream)


class BinanceExchangeClient(ExchangeClient):

    def __init__(
            self,
            uid="binance",
            helper=BinanceRawHelper(),
            uri: str = "ws://localhost:8080",
            auth_func=lambda uid: {"uid": uid}
    ):
        super().__init__(uid, helper, uri, auth_func)

    async def handle_helper(self, data: dict):
        stream = data.get("stream", None)
        stream_data = data.get("data", {})
        if stream and isinstance(stream, str):
            dest = self.stream_dict[stream]
            if len(dest):
                content = StreamContent(stream, stream_data)
                await self.send(dest, content.to_content_str())
        else:
            logger.warning(f"receive unknown data: {data}")


async def main():
    loop = asyncio.get_running_loop()
    binance_helper = BinanceRawHelper(proxy="http://192.168.5.10:10809")
    binance_exchange = BinanceExchangeClient(helper=binance_helper)
    task = asyncio.create_task(binance_exchange())
    loop.add_signal_handler(signal.SIGINT, task.cancel)
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    await task


if __name__ == "__main__":
    set_up_logger()
    asyncio.run(main(), debug=True)
