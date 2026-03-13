import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

LOG_DIR = '/home/duplicate/logs'
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(nombre: str) -> logging.Logger:
    logger = logging.getLogger(nombre)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt    ='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = RotatingFileHandler(
        f'{LOG_DIR}/{datetime.now().strftime("%Y-%m-%d")}.log',
        maxBytes   = 5 * 1024 * 1024,
        backupCount= 15
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger