# -*- coding: utf-8 -*-
# pylint: disable=R0903

import logging

import ast
import os
import configparser as ConfigParser
from pathlib import Path
from typing import Dict

eval_me = lambda x: ast.literal_eval(x)

DIRECTORY_NAME = 'config'

SERVER = 'kafka'
API = 'api'
LOGGING = 'logging'
WEBUI = 'webui'
XMLRPC = 'xmlrpc'
ODOO = 'odoo'
class ConfigConst():
    SASL_PLAIN_USERNAME = 'sasl_plain_username'
    SASL_PLAIN_PASSWORD = 'sasl_plain_password'
    # [server.config]
    # [server]
    BOOTSTRAP_SERVERS = 'bootstrap_servers'
    AUTO_OFFSET_RESET = 'auto_offset_reset'
    ENABLE_AUTO_COMMIT = 'enable_auto_commit'
    ASKS_ARG = 'asks_arg'
    POLL_TIMEOUT_SEC = 'poll_timeout_sec'
    CONSUMER_TIMEOUT_SEC = 'consumer_timeout_sec'
    MAX_RECORDS = 'max_records'
    # [security]
    SECURITY_PROTOCOL = 'security_protocol'
    SASL_MECHANISM = 'sasl_mechanism'
    TIMEOUT = 'timeout'

    # [kafka]
    SETTINGS = 'settings'
    # [topic.conf]
    TOPIC = 'topic'
    GROUP_ID = 'group_id'
    USERNAME = 'username'
    PASSWORD = 'password'
    ENDPOINT = 'endpoint'
    ACTIVE = 'active'
    AUTH_HEADER = 'auth_header'
    TOKEN = 'token'
    IS_RPC = 'is_rpc'
    ODOO_MODEL = 'odoo_model'
    ODOO_METHOD = 'odoo_method'
    MESSAGE_ONLY = 'message_only'

    # [logging]
    PATH = 'path'
    MAX_BYTES = 'max_bytes'
    BACKUP_COUNT = 'backup_count'
    # [webui]
    HOST = 'host'
    PORT = 'port'

    # [xmlrpc]
    URL = 'url'
    OBJ_COMMON = 'obj_common'
    OBJ_RPC = 'obj_rpc'

    # [odoo]
    ODOO_SERVER = 'odoo_server'
    ODOO_PORT = 'odoo_port'
    ODOO_DB = 'odoo_db'
    ODOO_USER = 'odoo_user'
    ODOO_PASSWORD = 'odoo_password'

    # [api]
    API_SKIPED_CODE = 'api_skiped_code'

class DefaultConfigFile:
    KAFKA_FILE_NAME = 'server.conf'
    TOPIC_FILE_NAME = 'topic.conf'

class KafkaConfig:
    def __init__(self) -> None:
        self.kafka_config_file = self.get_file_path(DefaultConfigFile.KAFKA_FILE_NAME)
        self.topic_file = self.get_file_path(DefaultConfigFile.TOPIC_FILE_NAME)
        self.cfp = ConfigParser.ConfigParser()

    @staticmethod
    def get_file_path(file_name:DefaultConfigFile) -> str:
        path = Path(os.path.dirname(__file__))
        return os.path.join(path.absolute(), DIRECTORY_NAME, file_name)

    def get_config(self) -> Dict[str,str]:
        # return a dict which initialized with kafka config
        try:
            self.cfp.clear()
            self.cfp.read([self.kafka_config_file])
            settings = eval_me(self.cfp.get(SERVER,ConfigConst.SETTINGS))
            settings.update({
                    ConfigConst.PATH: self.cfp.get(LOGGING,ConfigConst.PATH),
                    ConfigConst.MAX_BYTES: eval_me(self.cfp.get(LOGGING,ConfigConst.MAX_BYTES)),
                    ConfigConst.BACKUP_COUNT: eval_me(self.cfp.get(LOGGING,ConfigConst.BACKUP_COUNT)),
                    ConfigConst.PORT: eval_me(self.cfp.get(WEBUI, ConfigConst.PORT)),
                    ConfigConst.URL: self.cfp.get(XMLRPC, ConfigConst.URL),
                    ConfigConst.OBJ_COMMON: self.cfp.get(XMLRPC, ConfigConst.OBJ_COMMON),
                    ConfigConst.OBJ_RPC: self.cfp.get(XMLRPC, ConfigConst.OBJ_RPC),
                    ConfigConst.ODOO_SERVER: self.cfp.get(ODOO, ConfigConst.ODOO_SERVER),
                    ConfigConst.ODOO_PORT: eval_me(self.cfp.get(ODOO, ConfigConst.ODOO_PORT)),
                    ConfigConst.ODOO_DB: self.cfp.get(ODOO, ConfigConst.ODOO_DB),
                    ConfigConst.ODOO_USER: self.cfp.get(ODOO, ConfigConst.ODOO_USER),
                    ConfigConst.ODOO_PASSWORD: self.cfp.get(ODOO, ConfigConst.ODOO_PASSWORD),
                    ConfigConst.API_SKIPED_CODE : eval_me(self.cfp.get(API, ConfigConst.API_SKIPED_CODE))
                }
            )

            return settings
        except (ConfigParser.Error, Exception) as config_err:
            print(config_err)

    def get_topic(self) -> Dict[str,str]:
        # return a dict of dicts which include topic and topic related config for each  topic
        try:
            topics = {}
            self.cfp.clear()
            self.cfp.read([self.topic_file])
            sections = [section for section in self.cfp.sections()]
            for section in sections:
                topics.update(
                    {
                        section:
                                    {
                                        ConfigConst.GROUP_ID:self.cfp.get(section,ConfigConst.GROUP_ID),
                                        ConfigConst.USERNAME:self.cfp.get(section,ConfigConst.USERNAME),
                                        ConfigConst.PASSWORD:self.cfp.get(section,ConfigConst.PASSWORD),
                                        ConfigConst.ENDPOINT:self.cfp.get(section,ConfigConst.ENDPOINT),
                                        ConfigConst.TIMEOUT: eval_me(self.cfp.get(section,ConfigConst.TIMEOUT)),
                                        ConfigConst.MAX_RECORDS: eval_me(self.cfp.get(section,ConfigConst.MAX_RECORDS)),
                                        ConfigConst.ACTIVE: eval_me(self.cfp.get(section,ConfigConst.ACTIVE)),
                                        ConfigConst.AUTH_HEADER:self.cfp.get(section,ConfigConst.AUTH_HEADER),
                                        ConfigConst.TOKEN:self.cfp.get(section,ConfigConst.TOKEN),
                                        ConfigConst.IS_RPC: eval_me(self.cfp.get(section,ConfigConst.IS_RPC)),
                                        ConfigConst.ODOO_MODEL:self.cfp.get(section,ConfigConst.ODOO_MODEL),
                                        ConfigConst.ODOO_METHOD:self.cfp.get(section,ConfigConst.ODOO_METHOD),
                                        ConfigConst.MESSAGE_ONLY:self.cfp.get(section,ConfigConst.MESSAGE_ONLY)
                                    }
                    }
                )
            return topics
        except (ConfigParser.Error, Exception) as topic_err:
            print(topic_err)

# Testing purpose
# config = KafkaConfig()

# should get valid dict accordingly
# print(f'Dict for Kafka Config - > {config.get_config()}')
# print('------------------------------------------------')
# print(f'Dict for Topic Config -> {config.get_topic()}')
