import json
import time
import uuid


class Packet:
    def __init__(
        self,
        sent_time: int,
        route_time: int,
        source: str,
        destination: list[str],
        content: str,
    ):
        self.id = self.get_id()
        self.sent_time = sent_time
        self.route_time = route_time
        self.source = source
        self.destination = destination
        self.content = content

    @staticmethod
    def get_id():
        # convert uuid_v1 to ordered
        uuid_v1 = uuid.uuid1().hex
        return uuid_v1[12:16] + uuid_v1[8:12] + uuid_v1[0:8] + uuid_v1[16:]

    @staticmethod
    def get_timestamp():
        return int(time.time() * 1e3)

    @staticmethod
    def loads(message: str):
        obj = json.loads(message)
        assert isinstance(obj, dict), "type error"
        assert len(obj) == 6, "field num error"
        assert "id" in obj, "missing key id"
        assert isinstance(obj["id"], str), "id type error"
        assert len(obj["id"]) == 32, "id length error"
        assert "st" in obj, "missing key st"
        sent_time = obj["st"]
        assert isinstance(sent_time, int), "sent_time type error"
        assert "rt" in obj, "missing key rt"
        route_time = obj["rt"]
        assert isinstance(route_time, int), "route_time type error"
        assert "sc" in obj, "missing key sc"
        source = obj["sc"]
        assert isinstance(source, str), "source type error"
        assert len(source) <= 32, "source too long"
        assert "dt" in obj, "missing key dt"
        destination = obj["dt"]
        assert isinstance(destination, list), "destination type error"
        assert all(
            isinstance(item, str) for item in destination
        ), "destination type error"
        assert "ct" in obj, "missing key ct"
        content = obj["ct"]
        assert isinstance(content, str), "content type error"
        return Packet(sent_time, route_time, source, destination, content)

    def dumps(self):
        obj = {
            "id": self.id,
            "st": self.sent_time,
            "rt": self.route_time,
            "sc": self.source,
            "dt": self.destination,
            "ct": self.content,
        }
        return json.dumps(obj)
