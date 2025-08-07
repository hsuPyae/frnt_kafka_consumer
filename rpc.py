#!/usr/bin/env python
from dataclasses import dataclass
import json
from xmlrpc.client import ServerProxy
from kafka_config import ConfigConst
import logging


_logger = logging.getLogger(__name__)

ERROR_CODE = 400

@dataclass
class Auth:
    v13_common: str=''
    v13_models: str=''
    db: str=''
    user: str=''
    paw: str=''
    v13_uid: int=None


class XmlRpc:
    def __init__(self, logger=None) -> None:
        self._logger = logger or _logger

    def authenticate_odoo(self, config=None) -> Auth:
        auth = False
        URL = config.get(ConfigConst.URL).format(config.get(ConfigConst.ODOO_SERVER),config.get(ConfigConst.ODOO_PORT))
        try:
            v13_common = ServerProxy(config.get(ConfigConst.OBJ_COMMON).format(URL))
            v13_uid = v13_common.authenticate(config.get(ConfigConst.ODOO_DB),
                                              config.get(ConfigConst.ODOO_USER),
                                              config.get(ConfigConst.ODOO_PASSWORD),
                                              {})
            if v13_uid:
                v13_models = ServerProxy(config.get(ConfigConst.OBJ_RPC).format(URL), allow_none=True)
                auth = Auth(v13_common=v13_common,
                            v13_models=v13_models,
                            db=config.get(ConfigConst.ODOO_DB),
                            user=config.get(ConfigConst.ODOO_USER),
                            paw= config.get(ConfigConst.ODOO_PASSWORD),
                            v13_uid=v13_uid)

                self._logger.info("ERP Authentication success.")
            else:
                self._logger.error("ERP Authentication failed, please check user and password again.")

        except Exception as err:
            self._logger.error("Fault string: %s", str(err))

        return auth

    def call_rpc(self, auth:Auth, model:str, method:str, messages:json) -> int:
        if not auth:
            self._logger.error('Authenticaltion error.')
            return ERROR_CODE

        try:
            res = json.loads(auth.v13_models.execute_kw(auth.db, auth.v13_uid, auth.paw, model, method, [messages]))
            # response json format {"status": 200, "message": "", "result": "True"}
            self._logger.info(f'RPC Return info {res}.')
            return res.get('status')
        except Exception as err:
            self._logger.error(f'RPC Return error {err}.')
            return ERROR_CODE
