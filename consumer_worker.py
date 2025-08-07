# -*- coding: utf-8 -*-
# pylint: disable=W0707
# pylint: disable=W7935

import os
import sys
import getopt

from json import dumps
from time import sleep
from multiprocessing import Process

from consumer_log import get_logging
from kafka_config import KafkaConfig,ConfigConst,ConfigConst
from consumer_mixin import ConsumerMixin
from api import Api
from rpc import XmlRpc
from http_server import http_server

PS_LOG_FILE = 'ps_status.log'

zzz = lambda x: sleep(x)
class ConsumerWorker(ConsumerMixin):

    def __init__(self) -> None:
        super().__init__()

    def register_me(self, worker:Process) -> None:
        self.workers.update({self.config.get(ConfigConst.TOPIC):worker})

    def write_ps_log(self) -> None:
        _logger_ps = self.get_log(self.get_log_path(PS_LOG_FILE),1,1,True)
        _logger_ps.info(dumps({key:value.pid for key,value in self.workers.items()}))

    def spawn_log(self) -> None:
        log_worker = Process(target=self.write_ps_log)
        log_worker.start()
        log_worker.join()

    def spawn_me(self) -> None:
        if not self.auth:
            self.auth = XmlRpc(self._logger).authenticate_odoo(self.config)

        worker = Process(target=self.consume)
        worker.start()
        self.register_me(worker)

    def should_i_spawn(self, topic:str) -> bool:
        # ensure serialize cron for particular topic
        if self.workers.get(topic):
            if self.workers.get(topic).is_alive():
                self._logger.info(f'Worker {topic} is busy, will not spawn another process for this topic.')
                return False

        return True

    def start(self) -> None:
        # reload the topic config
        self.topics = KafkaConfig().get_topic()
        for topic in self.topics:
            topic_config = self.topics.get(topic)
            # inject the topic and topic specific config
            if topic_config.get(ConfigConst.ACTIVE):
                self.config.update(
                    {
                        ConfigConst.TOPIC:topic,
                        ConfigConst.GROUP_ID:topic_config.get(ConfigConst.GROUP_ID),
                        ConfigConst.USERNAME:topic_config.get(ConfigConst.USERNAME),
                        ConfigConst.PASSWORD:topic_config.get(ConfigConst.PASSWORD),
                        ConfigConst.ENDPOINT:topic_config.get(ConfigConst.ENDPOINT),
                        ConfigConst.TIMEOUT:topic_config.get(ConfigConst.TIMEOUT),
                        ConfigConst.MAX_RECORDS:topic_config.get(ConfigConst.MAX_RECORDS),
                        ConfigConst.ACTIVE:topic_config.get(ConfigConst.ACTIVE),
                        ConfigConst.AUTH_HEADER:topic_config.get(ConfigConst.AUTH_HEADER),
                        ConfigConst.TOKEN:topic_config.get(ConfigConst.TOKEN),
                        ConfigConst.IS_RPC:topic_config.get(ConfigConst.IS_RPC),
                        ConfigConst.ODOO_MODEL:topic_config.get(ConfigConst.ODOO_MODEL),
                        ConfigConst.ODOO_METHOD:topic_config.get(ConfigConst.ODOO_METHOD),
                        ConfigConst.MESSAGE_ONLY:topic_config.get(ConfigConst.MESSAGE_ONLY),
                    }
                )
                # complete the kafka config for each topic and will start consume accordingly.
                if self.should_i_spawn(topic):
                    self.spawn_me()
                    # self.consume()

            else:
                if topic in self.workers: del self.workers[topic]

            self.spawn_log()

    def run(self,ts:int) -> None:
        webui_server = Process(target=http_server.start, args=())
        webui_server.start()
        self.auth = XmlRpc(self._logger).authenticate_odoo(self.config)

        while 1:
            try:
                self._logger.info('Start Call')
                self.start()
                zzz(ts)
            except Exception as err:
                self._logger.error(err)
                sys.exit(2)
