import json
import time
from typing import Union, List


def get_timestamp():
    """
    return timestamp in milliseconds
    """
    return time.time() * 1000


class Packet:
    """
    The "Unit" send and received by Server and Client.

    Every function with name from_* must return **A Complete Packet**
    """

    def __init__(
            self,
            sent_time: Union[int, float],
            route_time: Union[int, float],
            action: str,
            source: str,
            destination: List[str],
            content: str
    ):
        self.sent_time = sent_time
        self.route_time = route_time
        self.action = action
        self.source = source
        self.destination = destination
        self.content = content

    @staticmethod
    def check_message(message: str, field_num: int):
        try:
            packet = json.loads(message)
        except json.JSONDecodeError:
            assert False, "packet decode error"
        assert isinstance(packet, dict), "packet type error"
        assert len(packet) == field_num, "field num error"
        return packet

    @staticmethod
    def check_sent_time(packet: dict):
        assert "st" in packet, "sent_time missing error"
        assert isinstance(packet["st"], Union[int, float]), "sent_time type error"
        assert packet["st"] < get_timestamp() + 1, "server time error"

    @staticmethod
    def check_route_time(packet: dict):
        assert "st" in packet, "sent_time missing error"
        assert isinstance(packet["st"], Union[int, float]), "sent_time type error"
        assert packet["st"] < get_timestamp() + 1, "server time error"

    @staticmethod
    def check_action(packet: dict):
        assert "ac" in packet, "action missing"
        assert isinstance(packet["ac"], str), "source type error"
        assert packet["ac"] in ["login", "message", "logout"], "action type error"

    @staticmethod
    def check_source(packet: dict):
        assert "sc" in packet, "source missing"
        assert isinstance(packet["sc"], str), "source type error"
        sc = packet["sc"].replace("_", "")
        assert sc.isalnum() or sc == "#", "source format error"

    @staticmethod
    def check_destination(packet: dict):
        assert "dt" in packet, "destination missing error"
        assert isinstance(packet["dt"], list), "destination type error"
        assert all(isinstance(item, str) for item in packet["dt"]), "destination type error"

    @staticmethod
    def check_content(packet: dict):
        assert "ct" in packet, "content missing error"
        assert isinstance(packet["ct"], str), "content type error"

    @staticmethod
    def to_login(source: str, content: str):
        packet = {
            "st": get_timestamp(),
            "sc": source,
            "ct": content
        }
        return json.dumps(packet)

    @staticmethod
    def from_client_login(message: str):
        packet = Packet.check_message(message, 3)
        Packet.check_sent_time(packet)
        Packet.check_source(packet)
        Packet.check_content(packet)
        return Packet(packet["st"], get_timestamp(), "login", packet["sc"], [], packet["ct"])

    @staticmethod
    def to_server(destination: List[str], content: str):
        packet = {
            "st": get_timestamp(),
            "dt": destination,
            "ct": content
        }
        return json.dumps(packet)

    @staticmethod
    def from_client_message(uid: str, message: str):
        packet = Packet.check_message(message, 3)
        Packet.check_sent_time(packet)
        Packet.check_destination(packet)
        Packet.check_content(packet)
        return Packet(packet["st"], get_timestamp(), "message", uid, packet["dt"], packet["ct"])

    def to_route(self):
        packet = {
            "st": self.sent_time,
            "rt": self.route_time,
            "sc": self.source,
            "ct": self.content
        }
        return json.dumps(packet)

    def to_client(self, content: str):
        """
        "#" stands for server
        """
        packet = {
            "st": self.sent_time,
            "rt": self.route_time,
            "sc": "#",
            "ct": content
        }
        return json.dumps(packet)

    @staticmethod
    def from_server_message(message: str):
        packet = Packet.check_message(message, 4)
        Packet.check_sent_time(packet)
        Packet.check_route_time(packet)
        Packet.check_source(packet)
        Packet.check_content(packet)
        return Packet(packet["st"], packet["rt"], "message", packet["sc"], [], packet["ct"])

    def to_str(self):
        """
        Function for record, or serialize.
        """
        packet = {
            "st": self.sent_time,
            "rt": self.route_time,
            "ac": self.action,
            "sc": self.source,
            "dt": self.destination,
            "ct": self.content
        }
        return json.dumps(packet)

    @staticmethod
    def from_str(message: str):
        """
        Load from output of to_str.
        """
        packet = Packet.check_message(message, 6)
        Packet.check_sent_time(packet)
        Packet.check_route_time(packet)
        Packet.check_action(packet)
        Packet.check_source(packet)
        Packet.check_content(packet)
        return Packet(
            packet["st"],
            packet["rt"],
            packet["ac"],
            packet["sc"],
            packet["dt"],
            packet["ct"]
        )
