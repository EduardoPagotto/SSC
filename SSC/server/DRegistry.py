'''
Created on 20220924
Update on 20221104
@author: Eduardo Pagotto
'''

import logging
import time

from threading import  Thread
from typing import Any, List

from  sJsonRpc.RPC_Responser import RPC_Responser
from SSC.server import create_queue, create_queues

from SSC.server.Tenant import Tenant
from SSC.server.FunctionCrt import FunctionCrt

from SSC.__init__ import __version__ as VERSION
from SSC.__init__ import __date_deploy__ as DEPLOY

class DRegistry(RPC_Responser):
    def __init__(self, function_crt : FunctionCrt, tenant : Tenant) -> None:
        super().__init__(self)

        self.function_crt = function_crt
        self.tenant = tenant
        self.done : bool = False
        self.ticktack = 0
        self.log = logging.getLogger('SSC.DRegistry')

        self.proc = 0
        self.erros = 0

        self.t_cleanner : Thread = Thread(target=self.cleanner, name='cleanner_files')
        self.t_cleanner.start()

    def sumario(self) -> dict:
        return {'app':'SSC', 
                'version':VERSION ,
                'deploy':DEPLOY,
                'tictac': self.ticktack,
                'topics': self.tenant.sumario(),
                'functions' : self.function_crt.summario()}

    def cleanner(self) ->None:
        """[Garbage collector of files]
        """

        time.sleep(10)
        self.log.info('thread cleanner_files start')

        while self.done is False:

            self.proc, self.erros = self.function_crt.execute()

            self.log.debug(f'Tick-Tack {self.ticktack} [{self.proc}  / {self.erros}]... ')
            self.ticktack += 1
            time.sleep(5)

        self.function_crt.stop_func_all()
        self.log.info('thread cleanner_files stop')

    # ClientQueue
    def create_producer(self, topic_name : str) -> dict:
        return create_queue(self.tenant.database, topic_name)
  
    # ClientQueue
    def create_subscribe(self, topic_name : str | List[str]) -> dict:

        if type(topic_name) == list:
            return create_queues(self.tenant.database, topic_name)

        if type(topic_name) == str:
            return create_queue(self.tenant.database, topic_name)

        raise Exception('topic invalid ' + str(topic_name))

    # --

    # Admin
    def topics_create(self, topic_name : str) -> str:
        return self.tenant.create_topic(topic_name)

    # Admin
    def topics_delete(self, topic_name : str) -> str:
        return self.tenant.delete_topics(topic_name)

    # Admin
    def topics_list(self, ns : str) -> List[str]:
        return self.tenant.list_topics(ns)

    # ---

    # Admin
    def tenants_create(self, name : str) -> str:
        return self.tenant.create(name)

    # Admin
    def tenants_delete(self, name : str) -> str:
        return self.tenant.delete(name)

    # Admin
    def tenants_list(self) -> List[str]:
        return self.tenant.list_all()

    # ---

    # Admin
    def namespaces_create(self, name : str) -> str:
        return self.tenant.create_namespace(name)

    # Admin
    def namespaces_delete(self, name : str) -> str:
        return self.tenant.delete_namespace(name)

    # Admin
    def namespaces_list(self, name : str) -> List[str]:
        return self.tenant.list_all_namespace(name)

    # ---

    def function_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.function_crt.pause_resume(name, is_pause)

    # Admin
    def function_create(self, params: dict) -> str:
        return self.function_crt.create(params)

    # Admin
    def function_delete(self, name: str) -> str:
        return self.function_crt.delete(name)

    # Admin
    def functions_list(self, tenant_ns : str) -> List[str]:
        return self.function_crt.list_all(tenant_ns)

        #--user-config '{"FileCfg":"aaaaa"}'
        #--user-config-file "/pulsar/host/etc/func1.yaml"

        # /pulsar/bin/pulsar-admin functions create \
        #   --name ConvertTxt2Dic \
        #   --py /var/app/src/ConvertTxt2Dic.py \
        #   --classname ConvertTxt2Dic.ConvertTxt2Dic \
        #   --inputs "persistent://rpa/manifesto/q01DecodeTxt"  \
        #   --output "persistent://rpa/manifesto/q99Erro" \
        #   --parallelism 1 