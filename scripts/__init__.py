import logging
import os
import sys


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(
        logging.getLevelName(os.getenv("EUPHROSYNE_TOOLS_LOGGING_LEVEL", "INFO"))
    )
    logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger
