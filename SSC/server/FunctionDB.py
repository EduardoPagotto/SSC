'''
Created on 20221006
Update on 20221007
@author: Eduardo Pagotto
'''

import importlib
import logging
import pathlib
from typing import Any, List, Optional

from tinydb import TinyDB, Query

from SSC.Function import Function
from SSC.server.Topic import Topic
from SSC.server.TopicCrt import TopicsCrt
from SSC.subsys.LockDB import LockDB

class FunctionDB(object):
    def __init__(self, database : TinyDB, topic_crt : TopicsCrt) -> None:
        self.database = database
        self.topic_crt = topic_crt
        self.log = logging.getLogger('SSC.TopicDB')

    def load(self, path_file : pathlib.Path, class_name : str) -> Any:
            klass = None

            plugin = str(path_file.parent).replace('/','.') + '.' + class_name

            self.log.debug(f'function import {plugin}')

            if plugin is None or plugin == '':
                self.log.error("Cannot have an empty plugin string.")

            try:
                (module, x, classname) = plugin.rpartition('.')

                if module == '':
                    raise Exception()
                mod = importlib.import_module(module)
                klass = getattr(mod, classname)

            except Exception as ex:
                msg_erro = "Could not enable class %s - %s" % (plugin, str(ex))
                self.log.error(msg_erro)
                raise Exception(msg_erro)

            if klass is None:
                self.log.error(f"Could not enable at least one class: {plugin}")
                raise Exception(f"Could not enable at least one class: {plugin}") 

            return klass()

    def create(self, params : dict) -> Function:

        topic_in : Optional[Topic] = None
        topic_out : Optional[Topic] = None

        if 'inputs' in params and params['inputs'] != None:
            topic_in = self.topic_crt.find_and_load(params['inputs'])

        if 'output' in params and params['output'] != None:
            topic_out = self.topic_crt.find_and_load(params['output']) 

        klass : Function = self.load(pathlib.Path(params['path']), params['classname'])

        with LockDB(self.database, 'funcs', True) as table:
            klass.document = table.get(doc_id=table.insert(params))

        klass.name = params['name']
        klass.topic_in = topic_in
        klass.topic_out = topic_out

        return klass


    def find(self, function_name : str) -> Function:

        with LockDB(self.database, 'funcs', False) as table:
            q = Query()
            itens = table.search(q.name == function_name)
        
        if len(itens) == 1:
            params = itens[0]

            topic_in : Optional[Topic] = None
            topic_out : Optional[Topic] = None

            if 'inputs' in params and params['inputs'] != None:
                topic_in = self.topic_crt.find_and_load(params['inputs'])

            if 'output' in params and params['output'] != None:
                topic_out = self.topic_crt.find_and_load(params['output']) 

            klass : Function = self.load(pathlib.Path(params['path']), params['classname'])
            klass.document = params
            klass.name = params['name']
            klass.topic_in = topic_in
            klass.topic_out = topic_out

            return klass

        raise Exception(f'function {function_name} does not exist')


    def delete(self, func_name : str) -> None:

        with LockDB(self.database, 'funcs', True) as table:
            q = Query()
            table.remove(q.topic == func_name)

    def list_all(self) -> List[str]:

        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            lista.append(item['topic'])

        return lista

    def get_all(self) -> List[Function]:

        lista : List[Function] = []

        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()
            
        for params in itens:

            topic_in : Optional[Topic] = None
            topic_out : Optional[Topic] = None

            if 'inputs' in params and params['inputs'] != None:
                topic_in = self.topic_crt.find_and_load(params['inputs'])

            if 'output' in params and params['output'] != None:
                topic_out = self.topic_crt.find_and_load(params['output']) 

            klass : Function = self.load(pathlib.Path(params['path']), params['classname'])
            klass.document = params
            klass.name = params['name']
            klass.topic_in = topic_in
            klass.topic_out = topic_out

            lista.append(klass)

        return lista
