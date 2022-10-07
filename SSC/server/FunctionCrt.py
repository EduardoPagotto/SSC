'''
Created on 20221006
Update on 20221007
@author: Eduardo Pagotto
'''

import logging
import os
import pathlib
import shutil
from threading import Lock
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
        with self.lock_func:
            if function.document:
                self.map_functions[function.document.doc_id] = function 

        return f"success create {params['name']}"


    def find_and_load(self, func_name : str) ->  Function:

        for k, v in self.map_functions.items():
            if v.name == func_name:
                self.log.debug(f'function find in cache {func_name}')
                return v

        function : Function = self.database.find(func_name)
        if function.document:
            self.map_functions[function.document.doc_id] = function
            self.log.debug(f'function find in db {func_name}')

        return function

    def delete(self, func_name : str):

        self.log.debug(f'Function delete {func_name}')

        val = func_name.split('/')

        for k, v in self.map_functions.items():
            
            if v.document:
                params = v.document
                doc_id = v.document.doc_id
                if ((val[0] == params['tenant']) and 
                    (val[1] == params['namespace']) and 
                    (val[2] == v.name) and 
                    (doc_id == k)):
                    del self.map_functions[doc_id]
                    self.database.delete_id(doc_id)
                    return


        self.database.delete(val[0], val[1], val[2])


    def list_all(self, tenant_ns : str) -> List[str]:
        return self.database.list_all(tenant_ns)

    def load_funcs_db(self):
        lista : List[Function] = self.database.get_all()
        for item in lista:
            self.map_functions[item.document.doc_id] = item
            self.log.debug(f'function load from db: {item.name}')

    def execute(self) -> Tuple[int, int]:

        inputs = 0
        outputs = 0

        context : Context = Context(self.database.topic_crt)

        with self.lock_func:

            for k, obj in self.map_functions.items():
                
                if (obj.topic_in) and (obj.topic_in.qsize() > 0):

                    res = obj.topic_in.pop(0)
                    if res:

                        self.log.debug(f'Function exec {obj.name} topic in: {obj.topic_in.name} ..')
                        inputs += 1
                        obj.tot_input += 1

                        try:

                            ret = obj.process(res, context)

                        except Exception as exp:
                            
                            # auto nack
                            obj.topic_in.push(res)

                            obj.tot_erro += 1
                            self.log.error(f'Function exec {obj.name} erro: ' + exp.args[0])
                            time.sleep(1)

                        if (obj.topic_out) and (ret != None):

                            self.log.debug(f'Function exec {obj.name} topic out: {obj.topic_out.name} ..')
                            obj.topic_out.push(ret)
                            obj.tot_output += 1

                            outputs += 1

        return inputs, outputs
