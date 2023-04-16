import json
import logging

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
    def check_content(content: str):
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
        content = {
            "kl": self.klass,
            "id": self.request_id,
            "mt": self.method,
            "pr": self.params
        }
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
        content = {
            "kl": self.klass,
            "id": self.request_id,
            "rs": self.result
        }
        return json.dumps(content)


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
        content = {
            "kl": self.klass,
            "st": self.stream,
            "dt": self.data
        }
        return json.dumps(content)
