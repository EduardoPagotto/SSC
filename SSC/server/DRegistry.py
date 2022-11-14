'''
Created on 20220924
Update on 20221114
@author: Eduardo Pagotto
'''

import logging
import pathlib
import time

from threading import  Thread
from typing import Any, List

from tinydb import TinyDB

from  sJsonRpc.RPC_Responser import RPC_Responser
from SSC.server import create_queue, create_queues
from SSC.server.SourceCrt import SourceCrt

from SSC.server.Tenant import Tenant
from SSC.server.FunctionCrt import FunctionCrt
from SSC.server.SinkCrt import SinkCrt

from SSC.__init__ import __version__ as VERSION
from SSC.__init__ import __date_deploy__ as DEPLOY

class DRegistry(RPC_Responser):
    def __init__(self, database : TinyDB , path : pathlib.Path, redis_url : str) -> None:
        super().__init__(self)

        self.path = path
        path_str = str(self.path.resolve())

        self.function_crt : FunctionCrt = FunctionCrt(database, path_str)
        self.source_crt : SourceCrt = SourceCrt(database, path_str)
        self.sink_crt : SinkCrt = SinkCrt(database, path_str)
        self.tenant : Tenant = Tenant(database, path_str, redis_url)

        self.done : bool = False
        self.ticktack : int = 0
        self.log = logging.getLogger('SSC.DRegistry')

        self.t_cleanner : Thread = Thread(target=self.cleanner, name='cleanner_files')
        self.t_cleanner.start()

    def sumario(self) -> dict:

        return {'app':{'name':'SSC', 'version':VERSION ,'deploy':DEPLOY},
                'tictac': self.ticktack,
                'topics': self.tenant.sumario(),
                'functions' : self.function_crt.summario(),
                'sources' : self.source_crt.summario(),
                'sinks' : self.sink_crt.summario()}

    def cleanner(self) ->None:
        """[Garbage collector of files]
        """

        time.sleep(10)
        self.log.info('thread cleanner_files start')

        while self.done is False:

            f_ok, f_err = self.function_crt.execute()
            s_ok, s_err = self.source_crt.execute()
            i_ok, i_err = self.sink_crt.execute()
            self.log.debug(f'on:{self.ticktack} fu:({f_ok}/{f_err}) so:({s_ok}/{s_err}) si:({i_ok}/{i_err})') 

            self.ticktack += 1
            time.sleep(5)

        self.function_crt.stop_func_all()
        self.source_crt.stop_func_all()
        
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

    # -- Topics Admin
    def topics_create(self, topic_name : str) -> str:
        return self.tenant.create_topic(topic_name)

    def topics_delete(self, topic_name : str) -> str:
        return self.tenant.delete_topics(topic_name)

    def topics_list(self, ns : str) -> List[str]:
        return self.tenant.list_topics(ns)

    # -- Tenants Admin
    def tenants_create(self, name : str) -> str:
        return self.tenant.create(name)

    def tenants_delete(self, name : str) -> str:
        return self.tenant.delete(name)

    def tenants_list(self) -> List[str]:
        return self.tenant.list_all()

    # -- Namespaces Admin
    def namespaces_create(self, name : str) -> str:
        return self.tenant.create_namespace(name)

    def namespaces_delete(self, name : str) -> str:
        return self.tenant.delete_namespace(name)

    def namespaces_list(self, name : str) -> List[str]:
        return self.tenant.list_all_namespace(name)

    # -- Functions Admin
    def function_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.function_crt.pause_resume(name, is_pause)

    def function_create(self, params: dict) -> str:
        return self.function_crt.create(params)

    def function_delete(self, name: str) -> str:
        return self.function_crt.delete(name)

    def functions_list(self, tenant_ns : str) -> List[str]:
        return self.function_crt.list_all(tenant_ns)

    # -- Source Admin
    def source_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.source_crt.pause_resume(name, is_pause)

    def source_create(self, params: dict) -> str:
        return self.source_crt.create(params)

    def source_delete(self, name: str) -> str:
        return self.source_crt.delete(name)

    def source_list(self, tenant_ns : str) -> List[str]:
        return self.source_crt.list_all(tenant_ns)

    # -- Sinks Admin
    def sink_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.sink_crt.pause_resume(name, is_pause)

    def sink_create(self, params: dict) -> str:
        return self.sink_crt.create(params)

    def sink_delete(self, name: str) -> str:
        return self.sink_crt.delete(name)

    def sink_list(self, tenant_ns : str) -> List[str]:
        return self.sink_crt.list_all(tenant_ns)