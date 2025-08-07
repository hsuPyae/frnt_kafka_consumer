# -*- coding: utf-8 -*-
# pylint: disable=W0707
# pylint: disable=W7935

import logging
import os
import sys
from json import dumps,loads
from typing import List
from datetime import datetime, timedelta
from time import sleep

from consumer_log import get_logging
from kafka import KafkaConsumer
from kafka_config import KafkaConfig,ConfigConst
from api import Api
from rpc import XmlRpc
from messager import Messager

SER_LOG_FILE = 'server.log'
TOPIC_LIST_POSTFIX = '.log'

SUCCESS_CODE = 200
SKIPED_CODE = 300

key_decoder = lambda x: type(x) == bytes and x.decode('utf-8') or x
val_decoder =lambda x: loads(x.decode('utf-8'))
counsumer_to = lambda x: datetime.now() + timedelta(seconds=x)
poll_to = lambda x: x*1000
zzz = lambda x: sleep(x)
app_log = lambda p,s,c,f: get_logging(p,s,c,f)


class ConsumerMixin():

    def __init__(self) -> None:
        self.workers = {}
        Config = KafkaConfig()
        self.config = Config.get_config()
        self._logger = self.get_log(self.get_log_path(SER_LOG_FILE))

    def get_log(self, path:str, max_bytes:int=None ,bk_count:int=None,ps_log:bool=False) -> logging.Logger:
        max_bytes = max_bytes if max_bytes else self.config.get(ConfigConst.MAX_BYTES)
        bk_count = bk_count if bk_count else self.config.get(ConfigConst.BACKUP_COUNT)
        return app_log(path, max_bytes,bk_count,ps_log)

    def get_log_path(self,file_name:str, path:str=None) -> str:
        dir_name = path if path else self.config.get(ConfigConst.PATH)
        return os.path.join(dir_name,file_name)

    def audit(self, message:List) -> None:
        ini_offset = 0
        CHECK_KEY = 'offset'
        for item in message:
            for k,v in item.items():
                if k == CHECK_KEY:
                    if ini_offset != 0:
                        if v - ini_offset != 1:
                            audit_log = f'#audit Something want not correct, previous offset is {ini_offset}, current offset is {v}'
                            self._logger.error(audit_log)
                            sys.exit(2)

                    ini_offset = v
                    print(f'Offset detail -> {v}')

    def create_consumer(self) -> KafkaConsumer:
        try:
            return KafkaConsumer(
                                self.config.get(ConfigConst.TOPIC),
                                group_id = self.config.get(ConfigConst.GROUP_ID),
                                bootstrap_servers = self.config.get(ConfigConst.BOOTSTRAP_SERVERS),
                                auto_offset_reset = self.config.get(ConfigConst.AUTO_OFFSET_RESET),
                                value_deserializer = val_decoder,
                                enable_auto_commit = self.config.get(ConfigConst.ENABLE_AUTO_COMMIT),
                                security_protocol = self.config.get(ConfigConst.SECURITY_PROTOCOL),
                                sasl_mechanism = self.config.get(ConfigConst.SASL_MECHANISM),
                                sasl_plain_username = self.config.get(ConfigConst.USERNAME),
                                sasl_plain_password = self.config.get(ConfigConst.PASSWORD)
                            )
        except Exception as consumer_err:
            self._logger.error(consumer_err)

    def _get_message(self, messages:str) -> dict:
        if self.config.get(ConfigConst.MESSAGE_ONLY) == 'True':
            return dumps(loads(messages).get("data")[0].get("value"))

        return messages

    def _api(self, messages:str):
        self._logger.info(f'Calling to endpoint {self.config.get(ConfigConst.ENDPOINT)}')
        return Api().call_api(
            self.config.get(ConfigConst.ENDPOINT),
            self.config.get(ConfigConst.AUTH_HEADER),
            self.config.get(ConfigConst.TOKEN),
            self._get_message(messages=messages),
            self.config.get(ConfigConst.TIMEOUT)
        )

    def _rpc(self, messages:str) -> int:
        self._logger.info(f'Calling to rpc {self.config.get(ConfigConst.ODOO_METHOD)}')
        return XmlRpc(self._logger).call_rpc(
                    self.auth,
                    self.config.get(ConfigConst.ODOO_MODEL),
                    self.config.get(ConfigConst.ODOO_METHOD),
                    messages)

    def call_endpoint(self, messages:List) -> bool:
        messages = Messager.get_json(messages)
        if self.config.get(ConfigConst.IS_RPC):
            res = self._rpc(messages)
            if res == SKIPED_CODE:
                self._logger.info(f'#Skiped-message of {self.config.get(ConfigConst.TOPIC)} : {messages}')
                return True
            return res == SUCCESS_CODE
        else:
            res = self._api(messages)
            if res in self.config.get(ConfigConst.API_SKIPED_CODE):
                self._logger.info(f'#API-Skiped-message of {self.config.get(ConfigConst.TOPIC)} : {messages}')
                return True
            return res == SUCCESS_CODE

    @staticmethod
    def header_decoder(headers:list) -> None:
        return [(item[0],key_decoder(item[1])) for item in headers]

    def to_dict(self, messages:list) -> dict:
        dict_message = [item._asdict() for item in messages]
        for item in dict_message: item.update({'key':key_decoder(item.get('key')),'headers':self.header_decoder(item.get('headers'))})
        return dict_message

    def consume(self) -> None:
        _logger = self.get_log(self.get_log_path(self.config.get(ConfigConst.TOPIC)+TOPIC_LIST_POSTFIX))
        consumer_timeout = counsumer_to(self.config.get(ConfigConst.CONSUMER_TIMEOUT_SEC))
        consumer = self.create_consumer()

        _logger.info(f'Start Pooling for Topic -> {self.config.get(ConfigConst.TOPIC)}')

        while True:
            msg_pack = consumer.poll(
                timeout_ms = poll_to(self.config.get(ConfigConst.POLL_TIMEOUT_SEC)),
                max_records = self.config.get(ConfigConst.MAX_RECORDS)
            )
            items = msg_pack.items()
            if items:
                tpn, messages = next(iter(items))
                if messages:
                    try:
                        _logger.info(f'Message count from poll {len(messages)} and size is {sys.getsizeof(messages)}')
                        dict_message = self.to_dict(messages)
                        self.audit(dict_message)
                        if self.call_endpoint(dict_message):
                            _logger.info(f'Successfully call to given endpoint.')
                            consumer.commit()
                        else:
                            consumer.close()
                            consumer = self.create_consumer()
                    except Exception as err:
                        _logger.error(f'An error occured when calling to the given endpoint {err}')
                        consumer.close()
                        consumer = self.create_consumer()

                    _logger.info(f'Current offset is -> {consumer.committed(tpn) if tpn else False}')
            else:
                consumer.commit()

            if datetime.now() > consumer_timeout:
                if consumer:
                    _logger.info(f'Consumer is Timeout after {(datetime.now()-consumer_timeout).seconds} sec.')
                    consumer.close()
                break

        _logger.info(f'End Pooling for Topic -> {self.config.get(ConfigConst.TOPIC)}')
