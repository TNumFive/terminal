import time
import traceback


def get_timestamp():
    """
    return timestamp in milliseconds
    """
    return time.time() * 1000


def get_error_line():
    exc = traceback.format_exc(limit=-1)
    exc_lines = exc.splitlines()
    return f"{exc_lines[-1].strip()}, {exc_lines[-2].strip()}"
