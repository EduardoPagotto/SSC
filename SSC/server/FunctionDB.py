'''
Created on 20221006
Update on 20221102
@author: Eduardo Pagotto
'''

import logging
from typing import List

from tinydb import TinyDB, Query
from tinydb.table import Document

from SSC.server import splitNamespace
from SSC.server.FuncCocoon import FuncCocoon
from SSC.subsys.LockDB import LockDB

class FunctionDB(object):
    def __init__(self, database : TinyDB) -> None:
        self.database = database
        self.log = logging.getLogger('SSC.TopicDB')

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
            tenant, namespace = splitNamespace(tenant_ns)
            if (tenant == item['tenant']) and (namespace == item['namespace']): 
                lista.append(item['name'])

        return lista

    def get_all(self) -> List[FuncCocoon]:

        lista : List[FuncCocoon] = []
        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()
            
        for params in itens:
            try:
                lista.append(FuncCocoon(params, self.database))
            except:
                pass

        return lista
