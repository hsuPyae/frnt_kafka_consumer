# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler

def get_logging(path:str, max_bytes:int, backup_count:int, ps_log:bool=False) -> logging.Logger:

    _logger = logging.getLogger('#kafka_client')
    c_handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count)
    c_format = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s') if not ps_log else logging.Formatter('%(message)s')
    c_handler.setFormatter(c_format)
    _logger.addHandler(c_handler)
    _logger.setLevel(logging.INFO)

    return _logger
