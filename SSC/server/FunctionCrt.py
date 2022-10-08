'''
Created on 20221006
Update on 20221007
@author: Eduardo Pagotto
'''

import logging
import os
import pathlib
import shutil
from threading import Lock, Thread
import time
from typing import Any, Dict, List, Optional, Tuple

from SSC.Function import Context, Function
from SSC.server.FunctionDB import FunctionDB

class FunctionCrt(object):
    def __init__(self, database : FunctionDB, path_storage : str) -> None:

        self.database = database

        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)

        self.lock_func = Lock()
        self.map_functions : Dict[int, Function] = {}
        self.t_functions : Dict[int, Thread] = {}

        self.log = logging.getLogger('SSC.FunctionCrt')

        self.load_funcs_db()

    def create(self, params) -> str:

        self.log.debug(f"function create {params['name']}")

        function : Optional[Function] = None
        try:
            function = self.find_and_load(params['name'])
        except:
            pass

        if function:
            raise Exception(f'topic {params["name"]} already exists')
        
        tst = pathlib.Path(os.path.join(str(self.storage), params['tenant'], params['namespace']))
        if not tst.is_dir():
            raise Exception(f'tenant or namespace invalid: {str(tst)}') 
        #tst.mkdir(parents=True, exist_ok=True) # remover depois

        # copicar pgm para area interna
        path_file_src = pathlib.Path(params['py'])
        names = params['classname'].split('.')

        path_dest = pathlib.Path(os.path.join(str(self.storage), params['tenant'], params['namespace'], names[0]))
        path_dest.mkdir(parents=True, exist_ok=True)
        final = pathlib.Path(str(path_dest) + '/' + path_file_src.name)

        shutil.copy(str(path_file_src), str(final))

        params['path'] = str(final)
        params['useConfig'] = {} # TODO Implementar

        function = self.database.create(params)
        if function.document:
            with self.lock_func:
                self.map_functions[function.document.doc_id] = function 
                self.start_func(function, function.document.doc_id)

        return f"success create {params['name']}"

    def stop_func_all(self):
        for k, v in self.t_functions.items():
            self.map_functions[k].alive = False

        for k, v in self.t_functions.items():
            self.stop_func(self.map_functions[k],k)

        

    def start_func(self, func : Function, doc_id : int):

        t_function : Thread = Thread(target=func.execute ,args=(self.database.topic_crt, 5), name=f't_func_{func.name}')
        t_function.start()
        self.t_functions[doc_id] = t_function

    def stop_func(self, func : Function, doc_id : int):
        func.alive = False
        count : int = 0

        self.log.debug(f'func {func.name} signed to stop')

        while self.t_functions[doc_id].is_alive():
            time.sleep(1)
            count += 1
            if count > 15:
                self.log.debug(f'func {func.name} overtime')
                break

            self.log.debug(f'func {func.name} waiting ....')

        self.t_functions[doc_id].join()
        del self.t_functions[doc_id]

        self.log.debug(f'func {func.name} is dead')
        

    def find_and_load(self, func_name : str) ->  Function:

        with self.lock_func:
            for k, v in self.map_functions.items():
                if v.name == func_name:
                    self.log.debug(f'function find in cache {func_name}')
                    return v

        function : Function = self.database.find(func_name)
        if function.document:
            with self.lock_func:
                self.map_functions[function.document.doc_id] = function
                self.log.debug(f'function find in db {func_name}')

        return function

    def delete(self, func_name : str):

        self.log.debug(f'Function delete {func_name}')

        val = func_name.split('/')

        if len(val) != 3:
            raise Exception(f'function {func_name} invalid')

        with self.lock_func:
            for k, v in self.map_functions.items(): 
                if v.document:
                    params = v.document
                    doc_id = v.document.doc_id
                    if ((val[0] == params['tenant']) and 
                        (val[1] == params['namespace']) and 
                        (val[2] == v.name) and 
                        (doc_id == k)):
                            try:
                                path_delete = pathlib.Path(params['path'])
                        
                                # stop thread and wait
                                self.stop_func(self.map_functions[k], k)

                                del self.map_functions[k] 
                        
                                self.database.delete_id(k)
                                shutil.rmtree(path_delete.parent)   
                                return
                            except Exception as exp: 
                                self.log.error(f'Falha ao apagar function {func_name} : {exp.args[0]}')
                                raise exp

        path_temp = self.database.delete(val[0], val[1], val[2])
        if len(path_temp) > 0:
            p = pathlib.Path(path_temp)
            shutil.rmtree(p.parent)

    def list_all(self, tenant_ns : str) -> List[str]:
        return self.database.list_all(tenant_ns)

    def load_funcs_db(self):
        lista : List[Function] = self.database.get_all()
        with self.lock_func:
            for item in lista:
                self.map_functions[item.document.doc_id] = item
                self.log.debug(f'function load from db: {item.name}')
                self.start_func(item, item.document.doc_id)


    def execute(self) -> Tuple[int, int]:

        inputs = 0
        outputs = 0

        with self.lock_func:
            for k, obj in self.map_functions.items():
                inputs += obj.tot_input
                outputs += obj.tot_output
                
        return inputs, outputs
