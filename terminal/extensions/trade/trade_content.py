import json
import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


class TradeContent:
    """
    The content used by TradeClient.

    Base content contains klass and other data.
    """

    def __init__(self, klass: str, content: dict):
        self.klass = klass
        self.content = content

    @staticmethod
    def from_content(content: str):
        try:
            trade_content = json.loads(content)
        except json.JSONDecodeError:
            assert False, "content decode error"
        assert isinstance(trade_content, dict), "content type error"
        assert "kl" in trade_content, "klass missing error"
        klass = trade_content.pop("kl")
        assert isinstance(klass, str), "klass type error"
        return TradeContent(klass, trade_content)

    def to_content_str(self):
        content = self.content.copy()
        content["kl"] = self.klass
        return json.dumps(content)


class RequestContent(TradeContent):
    """
    Content of request type, extract id, mt and pr from base's other data.
    """

    def __init__(self, request_id, method: str, params: list):
        super().__init__("request", {})
        self.request_id = request_id
        self.method = method
        self.params = params

    @staticmethod
    def from_content(trade_content: dict):
        assert len(trade_content) == 3, "field num error"
        assert "id" in trade_content, "id missing error"
        request_id = trade_content.pop("id")
        assert "mt" in trade_content, "method missing error"
        method = trade_content.pop("mt")
        assert isinstance(method, str), "method type error"
        assert "pr" in trade_content, "params missing error"
        params = trade_content.pop("pr")
        assert isinstance(params, list), "params type error"
        return RequestContent(request_id, method, params)

    def to_content_str(self):
        content = {"kl": self.klass, "id": self.request_id, "mt": self.method, "pr": self.params}
        return json.dumps(content)


class ResponseContent(TradeContent):
    """
    Content of response type, extract id and rs from base's other data.
    """

    def __init__(self, request_id, result):
        super().__init__("response", {})
        self.request_id = request_id
        self.result = result

    @staticmethod
    def from_content(trade_content: dict):
        assert len(trade_content) == 2, "field num error"
        assert "id" in trade_content, "id missing error"
        request_id = trade_content.pop("id")
        assert "rs" in trade_content, "result missing error"
        result = trade_content.pop("rs")
        return ResponseContent(request_id, result)

    def to_content_str(self):
        content = {"kl": self.klass, "id": self.request_id, "rs": self.result}
        return json.dumps(content)


@dataclass
class TradeData:
    trade_time: int
    price: float
    quantity: float


@dataclass
class BookLevel:
    price: float
    quantity: float


@dataclass
class BookData:
    ask_level_list: List[BookLevel] = field(default_factory=list)
    bid_level_list: List[BookLevel] = field(default_factory=list)


@dataclass
class KlineData:
    start_time: int
    end_time: int
    open: float
    close: float
    high: float
    low: float
    volume: float


class StreamContent(TradeContent):
    """
    Content of stream type, data part depends on stream type.
    """

    def __init__(self, stream: str, data: dict):
        super().__init__("stream", {})
        self.stream = stream
        self.data = data

    @staticmethod
    def from_content(trade_content: dict):
        assert len(trade_content) == 2, "field num error"
        assert "st" in trade_content, "stream missing error"
        stream = trade_content.pop("st")
        assert isinstance(stream, str), "stream type error"
        assert "dt" in trade_content, "data missing error"
        data = trade_content.pop("dt")
        assert isinstance(data, dict), "data type error"
        return StreamContent(stream, data)

    def to_content_str(self):
        content = {"kl": self.klass, "st": self.stream, "dt": self.data}
        return json.dumps(content)

    def embed_trade_data(self, trade_time: int, price: float, quantity: float):
        self.data = {"raw": self.data, "t": trade_time, "p": price, "q": quantity}

    def extract_trade_data(self):
        return TradeData(self.data["t"], self.data["p"], self.data["q"])

    def embed_book_data(self, ask_level_list: List[BookLevel], bid_level_list: List[BookLevel]):
        self.data = {"raw": self.data, "b": [], "a": []}
        for level in ask_level_list:
            self.data["a"].append({"p": level.price, "q": level.quantity})
        for level in bid_level_list:
            self.data["b"].append({"p": level.price, "q": level.quantity})

    def extract_book_data(self):
        ask_level_list: List[BookLevel] = []
        for level in self.data["a"]:
            ask_level_list.append(BookLevel(level["p"], level["q"]))
        bid_level_list: List[BookLevel] = []
        for level in self.data["b"]:
            bid_level_list.append(BookLevel(level["p"], level["q"]))
        return BookData(ask_level_list, bid_level_list)

    def embed_kline_data(self, start_time: int, end_time: int, open_price: float, close_price: float, high_price: float,
                         low_price: float, volume: float):
        self.data = {"raw": self.data, "s": start_time, "e": end_time, "o": open_price, "c": close_price,
                     "h": high_price, "l": low_price, "v": volume, }

    def extract_kline_data(self):
        return KlineData(self.data["s"], self.data["e"], self.data["o"], self.data["c"], self.data["h"], self.data["l"],
                         self.data["v"], )
