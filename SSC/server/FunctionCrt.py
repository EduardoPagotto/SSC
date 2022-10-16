'''
Created on 20221006
Update on 20221016
@author: Eduardo Pagotto
'''

import time
import logging
import pathlib
from threading import Lock, Thread
from typing import List, Tuple

from SSC.Function import Function
from SSC.server.FunctionDB import FunctionDB

class FunctionCrt(object):
    def __init__(self, database : FunctionDB, path_storage : str) -> None:

        self.database = database

        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)

        self.lock_func = Lock()
        self.list_function : List[Function] = []

        self.log = logging.getLogger('SSC.FunctionCrt')

        self.load_funcs_db()

    def __list_in_memory(self, tenant : str, ns : str, name_func : str) -> List[Function]:
        ret : List[Function] = []
        with self.lock_func:
            for func in self.list_function:
                if (tenant == func.document['tenant']) and (ns == func.document['namespace']) and (name_func == func.document['name']):
                    ret.append(func)

        return ret
    
    def __start_f(self, func : Function):

        with self.lock_func:
            num = 1
            max = func.document['parallelism']
            func.paralel = Thread(target=func.execute ,args=(self.database.topic_crt, 5), name=f't_{str(num)}_{max}_{func.name}')
            func.paralel.start()
            self.list_function.append(func)
            
            if max > 1:
                for c in range(num, max):
                    aux = self.database.find(func.document['name'])
                    aux.paralel = Thread(target=aux.execute ,args=(self.database.topic_crt, 5), name=f't_{str(c + 1)}_{max}_{aux.name}')
                    aux.paralel.start()
                    self.list_function.append(aux)

    def __signed_f(self, func : Function) -> None:
        func.alive = False
        self.log.debug(f'func {func.name} signed to stop')

    def __stop_f(self, func : Function) -> None:

        count : int = 0
        if func.paralel:
            while func.paralel.is_alive():
                time.sleep(1)
                count += 1
                if count > 30:
                    self.log.debug(f'func {func.name} overtime')
                    break

                self.log.debug(f'func {func.name} waiting ....')
                
            func.paralel.join()

        self.list_function.remove(func)
        self.log.debug(f'func {func.name} is dead')
        
    def create(self, params : dict) -> str:

        self.log.debug(f"function create {params['name']}")
        with self.lock_func:
            for func in self.list_function:
                if (params['tenant'] == func.document['tenant']) and (params['namespace'] == func.document['namespace']) and (params['name'] == func.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

        function = self.database.create(params)
        if function.document:
            self.__start_f(function)

        return f"success create {params['name']}"

    def stop_func_all(self):
        with self.lock_func:
            for func in self.list_function:
                self.__signed_f(func)

            for func in self.list_function:
                self.__stop_f(func)


    def delete(self, func_name : str):

        self.log.debug(f'Function delete {func_name}')

        val = func_name.split('/')
        if len(val) != 3:
            raise Exception(f'function {func_name} invalid')

        lista = self.__list_in_memory(val[0], val[1], val[2])
        if len(lista) > 0:
            doc_id = lista[0].document.doc_id
            for func in lista:
                self.__signed_f(func)

            with self.lock_func:
                for func in lista:
                    self.__stop_f(func)

            lista.clear()

            self.database.delete_id(doc_id)
            return

        raise Exception(f'function {func_name} does not exist')

    def list_all(self, tenant_ns : str) -> List[str]:
        return self.database.list_all(tenant_ns)

    def load_funcs_db(self):
        lista : List[Function] = self.database.get_all()
        for item in lista:
            self.log.debug(f'function load from db: {item.name}')
            self.__start_f(item)

    def execute(self) -> Tuple[int, int]:

        tot_proc = 0
        tot_erro = 0

        with self.lock_func:
            for func in self.list_function:
                tot_proc += func.tot_proc 
                tot_erro += func.tot_erro 

        return tot_proc, tot_erro
