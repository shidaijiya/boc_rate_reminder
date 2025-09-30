import logging
import os
import sys



LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "rate_reminding.log")


logger = logging.getLogger("rate_reminding")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    fmt = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')


    fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)


    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)


class LogPrint:
    def __init__(self, logger):
        self._logger = logger

    def info(self, msg): self._logger.info(msg)
    def debug(self, msg): self._logger.debug(msg)
    def warning(self, msg): self._logger.warning(msg)
    def error(self, msg): self._logger.error(msg)


log_print = LogPrint(logger)
