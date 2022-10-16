'''
Created on 20221006
Update on 20221016
@author: Eduardo Pagotto
'''

import importlib
import logging
import pathlib
from typing import Any, List, Optional

from tinydb import TinyDB, Query
from tinydb.table import Document

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

        klass : Function = self.__make_func(params)

        with LockDB(self.database, 'funcs', True) as table:
            klass.document = table.get(doc_id=table.insert(params))

        return klass


    def find(self, function_name : str) -> Function:

        with LockDB(self.database, 'funcs', False) as table:
            q = Query()
            itens = table.search(q.name == function_name)
        
        if len(itens) == 1:
            params = itens[0]

            klass : Function = self.__make_func(params)
            klass.document = params

            return klass

        raise Exception(f'function {function_name} does not exist')

    def delete_id(self, doc_id : int) -> None:
        with LockDB(self.database, 'funcs', True) as table:
            table.remove(doc_ids=[doc_id])

    def delete(self, tenant :str, namespace : str,  name : str) -> None:

        itens : List[Document] = []
        with LockDB(self.database, 'funcs', True) as table:
            q = Query()
            itens = table.search((q.tenant == tenant) & (q.namespace == namespace) & (q.name == name))

            if (len(itens) != 1) :
                raise Exception(f'function {name} does not exist')

            table.remove(doc_ids=[itens[0].doc_id])
    

    def list_all(self, tenant_ns : str) -> List[str]:

        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            val = tenant_ns.split('/')
            te = item['tenant']
            ns = item['namespace']
            if (val[0] == te) and (val[1] == ns): 
                lista.append(item['name'])

        return lista

    def get_all(self) -> List[Function]:

        lista : List[Function] = []

        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()
            
        for params in itens:

            klass : Function = self.__make_func(params)
            klass.document = params

            lista.append(klass)

        return lista

    def __make_func(self, params : Document | dict) -> Function:

        topics_in : List[Topic] = []
        topic_out : Optional[Topic] = None

        if 'inputs' in params and params['inputs'] != None:
            for item in params['inputs']:
                topics_in.append(self.topic_crt.find_and_load(item))


        if 'output' in params and params['output'] != None:
            topic_out = self.topic_crt.find_and_load(params['output']) 

        klass : Function = self.load(pathlib.Path(params['py']), params['classname'])

        klass.name = params['name']
        klass.topics_in = topics_in
        klass.topic_out = topic_out

        klass.log = logging.getLogger('SSC.function')
        klass.tot_proc  = 0
        klass.tot_erro  = 0
        klass.alive  = True

        return klass