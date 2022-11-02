'''
Created on 20221006
Update on 20221102
@author: Eduardo Pagotto
'''

import time
import logging
import pathlib
from threading import Lock, Thread
from typing import List, Tuple

from SSC.server import splitTopic
from SSC.server.FuncCocoon import FuncCocoon
from SSC.server.FunctionDB import FunctionDB

class FunctionCrt(object):

    def __init__(self, fdb : FunctionDB, path_storage : str) -> None:

        self.fdb = fdb
        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)

        self.lock_func = Lock()
        self.list_function : List[FuncCocoon] = []

        self.log = logging.getLogger('SSC.FunctionCrt')

        self.load_funcs_db()

    def __list_in_memory(self, tenant : str, ns : str, name_func : str) -> List[FuncCocoon]:

        ret : List[FuncCocoon] = []
        with self.lock_func:
            for func in self.list_function:
                if (tenant == func.document['tenant']) and (ns == func.document['namespace']) and (name_func == func.document['name']):
                    ret.append(func)

        return ret
    
    def summario(self) ->dict:
        
        result = []
        tot = 0
        with self.lock_func:
            tot = len(self.list_function)  
            for i in self.list_function:
                result.append(i.sumary())  

        return {'online': tot, 'tasks': result}

    def create(self, params : dict) -> str:

        self.log.debug(f"function create {params['name']}")
        with self.lock_func:
            for func in self.list_function:
                if (params['tenant'] == func.document['tenant']) and (params['namespace'] == func.document['namespace']) and (params['name'] == func.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

        cocoon : FuncCocoon = FuncCocoon(params, self.fdb.database)
        cocoon.start()
        self.list_function.append(cocoon)

        return f"success create {params['name']}"

    def stop_func_all(self):

        with self.lock_func:
            for func in self.list_function:
                func.stop()

            for func in self.list_function:
                func.join()
                self.list_function.remove(func)


    def pause(self, func_name : str):
        self.log.debug(f'function pause {func_name}')
        tenant, namespace, name = splitTopic(func_name)   

        for fun in self.list_function:
            if ((fun.document['tenant'] == tenant) and (fun.document['namespace'] == namespace) and (fun.document['name'] == name)):
                fun.pause()
                return f'func {name} paused'

        raise Exception(f'function {func_name} does not exist')

    def resume(self, func_name : str):
        self.log.debug(f'function resume {func_name}')
        tenant, namespace, name = splitTopic(func_name)

        for fun in self.list_function:
            if ((fun.document['tenant'] == tenant) and (fun.document['namespace'] == namespace) and (fun.document['name'] == name)):
                fun.resume()
                return f'func {name} paused'

    def delete(self, func_name : str):

        self.log.debug(f'function delete {func_name}')

        tenant, namespace, func_name = splitTopic(func_name)

        lista = self.__list_in_memory(tenant, namespace, func_name)
        if len(lista) > 0:
            doc_id = lista[0].document.doc_id
            for func in lista:
                func.stop()

            with self.lock_func:
                for func in lista:
                    func.join()
                    self.list_function.remove(func)

            lista.clear()

            self.fdb.delete_id(doc_id)
            return

        raise Exception(f'function {func_name} does not exist')

    def list_all(self, tenant_ns : str) -> List[str]:
        return self.fdb.list_all(tenant_ns)

    def load_funcs_db(self):

        lista : List[FuncCocoon] = self.fdb.get_all()
        for item in lista:
            self.log.debug(f'function load from db: {item.name}')
            item.start()
            self.list_function.append(item)

    def execute(self) -> Tuple[int, int]:

        tot_proc = 0
        tot_erro = 0
        with self.lock_func:
            for func in self.list_function:
                ok, erro = func.count_tot()
                tot_proc += ok
                tot_erro += erro

        return tot_proc, tot_erro
