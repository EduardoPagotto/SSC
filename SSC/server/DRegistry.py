'''
Created on 20220924
Update on 20230314
@author: Eduardo Pagotto
'''

import logging
import pathlib
import time

from threading import  Thread
from typing import Any, List, Optional
from queue import Empty

from tinydb import TinyDB

from  sJsonRpc.RPC_Responser import RPC_Responser

from SSC.server.Namespace import Namespace
from SSC.server.FunctionCrt import FunctionCrt

from SSC.__init__ import __version__ as VERSION
from SSC.__init__ import __date_deploy__ as DEPLOY

class DRegistry(RPC_Responser):
    def __init__(self, database : TinyDB , path : pathlib.Path) -> None:
        super().__init__(self)

        self.path = path
        path_str = str(self.path.resolve())

        self.namespace : Namespace = Namespace(database, path_str)
        self.source_crt : FunctionCrt = FunctionCrt('source', self.namespace)
        self.sink_crt : FunctionCrt = FunctionCrt('sink', self.namespace)
        self.function_crt : FunctionCrt = FunctionCrt('function', self.namespace)

        self.done : bool = False
        self.ticktack : int = 0
        self.log = logging.getLogger('SSC.DRegistry')

        self.t_cleanner : Thread = Thread(target=self.cleanner, name='cleanner_files')
        self.t_cleanner.start()

    def sumario(self) -> dict:

        return {'app':{'name':'SSC', 'version':VERSION ,'deploy':DEPLOY},
                'tictac': self.ticktack,
                'queues': self.namespace.summario(),
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
        self.sink_crt.stop_func_all()
        self.source_crt.stop_func_all()
        
        self.log.info('thread cleanner_files stop')

    # -- ClientQueue
    def create_producer(self, queue_name_full : str) -> None:
        self.namespace.queue_get(queue_name_full)

    def create_subscribe(self, queue_name_full : str) -> None:
        self.namespace.queue_get(queue_name_full)
    
    def send_producer(self, key : str, prop: dict, queue_name_full : str, msg : str):
        self.namespace.push(key, prop, queue_name_full, msg)

    def subscribe_receive(self, queue_name_full: str, timeOut: int) -> Optional[Any]:
        try:
            return self.namespace.pop(queue_name_full, timeOut=timeOut)
        except Empty:
            return None

    # -- Namespaces Admin
    def namespaces_create(self, name : str) -> str:
        self.namespace.create(name)
        return f'sucess namespace create {name}'

    def namespaces_delete(self, name : str) -> str:
        self.namespace.delete(name)
        return f'sucess namespace delete {name}'

    def namespaces_list(self) -> List[str]:
        return self.namespace.list_all()

    # -- Queues Admin
    def queues_create(self, queue_name_full : str) -> str:
        self.namespace.queue_create(queue_name_full)
        return f'success create {queue_name_full} '

    def queues_delete(self, queue_name_full : str) -> str:
        self.namespace.queue_delete(queue_name_full)
        return f'sucess delete {queue_name_full}'

    def queues_list(self, ns : str) -> List[str]:
        return self.namespace.queue_list(ns)

    # # -- Source Admin
    def source_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.source_crt.pause_resume(name, is_pause)

    def source_create(self, params: dict) -> str:
        return self.source_crt.create(params)

    def source_delete(self, name: str) -> str:
        return self.source_crt.delete(name)

    def source_list(self, ns : str) -> List[str]:
        return self.source_crt.list_all(ns)

    # -- Sinks Admin
    def sink_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.sink_crt.pause_resume(name, is_pause)

    def sink_create(self, params: dict) -> str:
        return self.sink_crt.create(params)

    def sink_delete(self, name: str) -> str:
        return self.sink_crt.delete(name)

    def sink_list(self, ns : str) -> List[str]:
        return self.sink_crt.list_all(ns)

    # -- Functions Admin
    def function_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.function_crt.pause_resume(name, is_pause)

    def function_create(self, params: dict) -> str:
        return self.function_crt.create(params)

    def function_delete(self, name: str) -> str:
        return self.function_crt.delete(name)

    def functions_list(self, ns : str) -> List[str]:
        return self.function_crt.list_all(ns)
