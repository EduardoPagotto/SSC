'''
Created on 20221006
Update on 20221006
@author: Eduardo Pagotto
'''

import logging
import os
import pathlib
import shutil
from threading import Lock
from typing import Any, Dict, List, Optional

from SSC.Function import Function
from SSC.server.FunctionDB import FunctionDB

class FunctionCrt(object):
    def __init__(self, database : FunctionDB, path_storage : str) -> None:

        self.database = database

        self.storage = pathlib.Path(path_storage)
        self.storage.mkdir(parents=True, exist_ok=True)

        self.lock_func = Lock()
        self.map_functions : Dict[int, Function] = {}

        self.log = logging.getLogger('SSC.FunctionCrt')

        self.load_funcs_db()

    def create(self, params) -> str:

        function : Optional[Function] = None
        try:
            function = self.find_and_load(params['name'])
        except:
            pass

        if function:
            raise Exception(f'topic {params["name"]} already exists')
        
        tst = pathlib.Path(os.path.join(str(self.storage), params['tenant'], params['namespace']))
        # if not tst.is_dir():
        #     return f'tenant or namespace invalid' 
        tst.mkdir(parents=True, exist_ok=True) # remover depois

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
                return v

        function : Function = self.database.find(func_name)
        if function.document:
            self.map_functions[function.document.doc_id] = function

        return function

    def delete(self, func_name : str):

        for k, v in self.map_functions.items():
            if v.name == func_name:
                if v.document:
                    del self.map_functions[v.document.doc_id]

        self.database.delete(func_name)


    def list_all(self) -> List[str]:
        return self.database.list_all()

    def load_funcs_db(self):
        pass

        # with self.lock_db:
        #     table = self.db.table('funcs')
        #     funcs = table.all()

        # for params in funcs:

        #     idQueueIn = -1
        #     idQueueOut = -1
        #     if 'input' in params:
        #         idQueueIn = self.subscribe(params['input'])

        #     if 'output' in params:
        #         idQueueOut = self.create_producer(params['output'])

        #     names = params['class'].split('.')
        #     path_dest = pathlib.Path(os.path.join(str(self.storage), params['tenant'], params['namespace'], names[0]))

        #     base = str(path_dest).replace('/','.') + '.' + params['class']
        #     klass : Function = self.function_load(base)
        #     klass.name = params['name']
        #     klass.qIn = idQueueIn
        #     klass.qOut = idQueueOut
        #     klass.useConfig = {}
        #     klass.id = params.doc_id

        #     self.func_list.append(klass)