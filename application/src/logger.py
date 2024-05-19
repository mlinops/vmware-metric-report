import logging
from logging.handlers import RotatingFileHandler
from variables import LOG_FILE, LOGGING_LEVEL, LOG_FORMAT,LOG_DATEFMT

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(LOGGING_LEVEL)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATEFMT)

    # Console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

