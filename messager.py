
import json


PAYLOAD_KEY = 'data'


class Messager:

    def get_json(payload:list) -> json:
        """data format : {"data": [{"topic": '', "value": {}, {}}]}"""
        return json.dumps({PAYLOAD_KEY: payload})
