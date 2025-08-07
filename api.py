# -*- coding: utf-8 -*-
import json
import logging
from typing import List
import requests


_logger = logging.getLogger('#kafka_client')

DEFAULT_TIMEOUT = 3

CONTENT_TYPE = 'application/json'

CONTENT = 'Content-Type'
METHOD = 'POST'


class Api:
    @staticmethod
    def get_header(header:str, token:str) -> dict:
        return {header:token,CONTENT:CONTENT_TYPE}

    def call_api(self, url:str, header:str, token:str, payload:json, timeout:int=DEFAULT_TIMEOUT) -> int:
        try:
            response = requests.request(METHOD, url, headers=self.get_header(header,token), data=payload, timeout=timeout)
            _logger.info(f'Status Code : {response.status_code}, Response : {response.text}')

            return response.status_code
        except (requests.exceptions.RequestException) as err:
            _logger.error(f'From api call {err}')
            return 500
