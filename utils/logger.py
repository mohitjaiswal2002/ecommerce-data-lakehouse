import logging
import os

from utils.config import LOG_PATH


def get_logger(name):

    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)


    if not logger.handlers:

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        )


        console_handler = logging.StreamHandler()

        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)



        os.makedirs(
            os.path.dirname(LOG_PATH),
            exist_ok=True
        )


        file_handler = logging.FileHandler(LOG_PATH)

        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)


    return logger