"""
A simplified version of [json-rpc 2.0](https://www.jsonrpc.org/specification).
Only support `params` index by name and can't be omitted.
Does not present `jsonrpc` string.
Use `"null"` when id exists and have no value before serializing it.
The `id` is `str` and `len(id) == 32`.
Does not support **Batch**.
"""
import json
from dataclasses import dataclass, asdict


@dataclass
class Obj:
    def to_dict(self):
        # remove key if value is None
        return asdict(
            self, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}
        )

    def dumps(self):
        return json.dumps(self.to_dict())


@dataclass
class Request(Obj):
    method: str
    params: dict
    id: str | None = None


@dataclass
class Error(Obj):
    code: int
    message: str
    data: dict | None = None


@dataclass
class Response(Obj):
    id: str
    result: dict | None = None
    error: Error | None = None

    def to_dict(self):
        obj = super().to_dict()
        if self.id == "null":
            obj["id"] = None
        return obj


def load_notification(obj: dict):
    try:
        # request must not have id
        assert len(obj) == 2
        assert "method" in obj
        assert isinstance(obj["method"], str)
        assert "params" in obj
        assert isinstance(obj["params"], dict)
    except AssertionError:
        return Error(-32600, "Invalid Request")
    return Request(obj["method"], obj["params"])


def load_request(obj: dict):
    try:
        # request must have id
        assert len(obj) == 3
        assert isinstance(obj["id"], str)
        assert len(obj["id"]) == 32
        assert "method" in obj
        assert isinstance(obj["method"], str)
        assert "params" in obj
        assert isinstance(obj["params"], dict)
    except AssertionError:
        return Error(-32600, "Invalid Request")
    return Request(obj["method"], obj["params"], obj["id"])


def load_response(obj: dict):
    try:
        # response must have id and one of error or result
        assert len(obj) == 2
        # id could normal 32byte str or null
        if obj["id"] is not None:
            assert isinstance(obj["id"], str)
            assert len(obj["id"]) == 32
        else:
            obj["id"] = "null"
            assert "result" not in obj
        if "result" in obj:
            assert isinstance(obj["result"], dict)
            return Response(obj["id"], obj["result"])
        else:
            assert "error" in obj
            assert isinstance(obj["error"], dict)
            error_dict: dict = obj["error"]
            assert len(error_dict) == 2 or len(error_dict) == 3
            if len(error_dict) == 3:
                assert "data" in error_dict
                assert isinstance(error_dict["data"], dict)
            assert "code" in error_dict
            assert isinstance(error_dict["code"], int)
            assert "message" in error_dict
            assert isinstance(error_dict["message"], str)
            return Response(
                obj["id"],
                error=Error(
                    error_dict["code"],
                    error_dict["message"],
                    error_dict.pop("data", None),
                ),
            )
    except AssertionError:
        return None


def loads(content: str):
    """
    Return Request, then handle the request.
    Return Response, then try to resolve future.
    Return Error, respond that back.
    Return None, log it and leave.
    """
    try:
        obj = json.loads(content)
        assert isinstance(obj, dict)
    except (json.JSONDecodeError, AssertionError):
        return Error(-32700, "Parse error")
    # first we need to tell whether it's request or response
    if "id" not in obj:
        return load_notification(obj)
    elif "method" in obj:
        return load_request(obj)
    elif "result" in obj or "error" in obj:
        return load_response(obj)
    else:
        # with id and only id, drop it
        return None
