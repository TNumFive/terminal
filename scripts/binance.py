import asyncio
import json
import signal
import time
from typing import Optional, List

from terminal.extensions import ExchangeClient, set_up_logger, ExchangeRawHelper, StreamContent
from terminal.extensions.trade.trade_content import TradeContent, BookLevel

logger = set_up_logger("binance")


class BinanceRawHelper(ExchangeRawHelper):
    def __init__(
            self,
            http_url="https://api.binance.com",
            ws_url="wss://stream.binance.com",
            init_stream="btc_usdt@kline_1m",
            proxy=None,
            websocket_send_interval=0.2,
            max_connect_retry_times=10
    ):
        super().__init__(http_url, ws_url, proxy, websocket_send_interval, max_connect_retry_times)
        self.init_stream = init_stream
        self.available_symbol_set: set = set()

    @staticmethod
    def get_request_id():
        return int(time.time() * 1e6)

    def format_stream_name(self, stream: str):
        try:
            stream_symbol, stream_type = ExchangeRawHelper.parse_stream(stream)
            stream_symbol = ''.join(stream_symbol).lower()
            # stream_symbol must in given pairs
            if stream_symbol not in self.available_symbol_set:
                raise ValueError("symbol not available")
            # stream_type must correspond to exchange request
            if "trade" in stream_type:
                stream_type = "trade"
            elif "kline" in stream_type:
                stream_type = "kline_1m"
            elif "bookTicker" in stream_type:
                stream_type = "bookTicker"
            elif "book" in stream_type:
                stream_type = "depth20@100ms"
            else:
                raise ValueError("stream type error")
        except Exception as e:
            logger.warning(f"stream({stream}) format error: {str(e)}")
            return None
        stream_name = f"{stream_symbol}@{stream_type}"
        self.stream_substitute[stream_name] = stream
        return stream_name

    async def get_exchange_info(self):
        async with self.session.get(
                f"{self.http_url}/api/v3/exchangeInfo",
                proxy=self.proxy
        ) as response:
            exchange_info = await response.json()
            if "code" in exchange_info and "msg" in exchange_info:
                assert False, str(exchange_info)
            symbol_list: list[dict] = exchange_info["symbols"]
            for symbol in symbol_list:
                symbol = str(symbol["symbol"]).lower()
                self.available_symbol_set.add(symbol)

    async def connect(self):
        # request exchange info
        await self.get_exchange_info()
        ws_url_suffix = "/stream?streams="
        if ws_url_suffix not in self.ws_url:
            self.ws_url = self.ws_url + ws_url_suffix
            stream_name = self.format_stream_name(self.init_stream)
            self.ws_url += stream_name
        if self.init_stream not in self.stream_set:
            self.stream_set[self.init_stream] = set()
        await super().connect()

    async def resubscribe(self):
        # do the resubscribe
        stream_name_list = []
        for stream in self.stream_set:
            if stream == self.init_stream:
                continue
            stream_name = self.format_stream_name(stream)
            if not stream_name:
                continue
            stream_name_list.append(stream_name)
        if not len(stream_name_list):
            return
        request = {
            "method": "SUBSCRIBE",
            "params": stream_name_list,
            "id": self.get_request_id()
        }
        await self.websocket_send(json.dumps(request))

    async def set_up(self):
        await self.resubscribe()
        await super().set_up()

    async def preprocess(self, data: dict) -> Optional[TradeContent]:
        if "stream" not in data:
            logger.debug(f"not stream: {data}")
            return None
        if data["stream"] not in self.stream_substitute:
            logger.debug(f"unknown stream: {data}")
            return None
        stream = self.stream_substitute[data["stream"]]
        if not len(self.stream_set[stream]):
            return None
        data = data.pop("data")
        stream_symbol, stream_type = ExchangeRawHelper.parse_stream(stream)
        content = StreamContent(stream, data)
        if stream_type[0] == "trade":
            content.embed_trade_data(data.pop("T"), float(data.pop("p")), float(data.pop("q")))
        elif stream_type[0] == "book":
            ask_level_list: List[BookLevel] = []
            for ask in data.pop("asks"):
                ask_level_list.append(BookLevel(float(ask[0]), float(ask[1])))
            bid_level_list: List[BookLevel] = []
            for bid in data.pop("bids"):
                bid_level_list.append(BookLevel(float(bid[0]), float(bid[1])))
            content.embed_book_data(ask_level_list, bid_level_list)
        elif stream_type[0] == "kline":
            kline: dict = data.pop("k")
            content.embed_kline_data(kline.pop("t"), kline.pop("T"), float(kline.pop("o")), float(kline.pop("c")),
                                     float(kline.pop("h")), float(kline.pop("l")), float(kline.pop("v")))
        return content

    async def subscribe(self, uid: str, stream: str):
        stream_name = self.format_stream_name(stream)
        if not stream_name:
            return
        if stream not in self.stream_set:
            self.stream_set[stream] = {uid}
            request = {
                "method": "SUBSCRIBE",
                "params": [stream_name],
                "id": self.get_request_id()
            }
            await self.websocket_send(json.dumps(request))
        else:
            self.stream_set[stream].add(uid)

    async def unsubscribe(self, uid: str, stream: str):
        if stream not in self.stream_set:
            return
        dest = self.stream_set[stream]
        if uid in dest:
            dest.remove(uid)
        if len(dest):
            return
        stream_name = self.format_stream_name(stream)
        if not stream_name:
            return
        request = {
            "method": "UNSUBSCRIBE",
            "params": [stream_name],
            "id": self.get_request_id()
        }
        await self.websocket_send(json.dumps(request))
        self.stream_set.pop(stream)


class BinanceExchangeClient(ExchangeClient):

    def __init__(
            self,
            uid="binance",
            helper=BinanceRawHelper(),
            uri: str = "ws://localhost:8080",
            auth_func=lambda uid: {"uid": uid}
    ):
        super().__init__(uid, helper, uri, auth_func)

    async def handle_content(self, content: TradeContent):
        if isinstance(content, StreamContent):
            dest = self.stream_set[content.stream]
            await self.send(list(dest), content.to_content_str())


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
