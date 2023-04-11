import logging
import os
import time
import traceback
from datetime import datetime, timedelta
from logging.handlers import BaseRotatingHandler
from stat import ST_MTIME


def get_timestamp():
    """
    return timestamp in milliseconds
    """
    return time.time() * 1000


def get_error_line():
    exc = traceback.format_exc(limit=-1)
    exc_lines = exc.splitlines()
    return f"{exc_lines[-1].strip()}, {exc_lines[-2].strip()}"


class TimedRotatingFileHandler(BaseRotatingHandler):
    def __init__(self, filename, interval=timedelta(hours=1), encoding=None, delay=False, errors=None) -> None:
        self.shouldRollover = self.should_rollover
        self.doRollover = self.do_rollover
        super().__init__(filename, "a", encoding=encoding, delay=delay, errors=errors)
        self.interval = interval.total_seconds()
        self.suffix_format = f".%Y%m%d_%H%M%S.log"
        if os.path.exists(self.baseFilename):
            self.now = os.stat(self.baseFilename)[ST_MTIME]
        else:
            self.now = time.time()
        if self.baseFilename.endswith(".log"):
            self.filename_prefix = self.baseFilename[:-4]
        else:
            self.filename_prefix = self.baseFilename

    def should_rollover(self, _):
        now = time.time()
        if now > self.now + self.interval:
            if os.path.exists(self.baseFilename) and os.path.isfile(self.baseFilename):
                return True
        return False

    def do_rollover(self):
        if self.stream:
            self.stream.close()
        time_suffix = datetime.fromtimestamp(self.now).strftime(self.suffix_format)
        new_filename = self.filename_prefix + time_suffix
        new_filename = self.rotation_filename(new_filename)
        if os.path.exists(new_filename):
            os.remove(new_filename)
        self.rotate(self.baseFilename, new_filename)
        self.now = time.time()
        if not self.delay:
            self.stream = self._open()


def set_up_logger(name="terminal"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s |+| %(name)s |+| %(levelname)s |+| %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
