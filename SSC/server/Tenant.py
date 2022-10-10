'''
Created on 20221006
Update on 20221010
@author: Eduardo Pagotto
'''

import logging
import os
import pathlib
import shutil
from typing import List

from tinydb import TinyDB, Query
from tinydb.table import Document

from SSC.subsys.LockDB import LockDB

class Tenant(object):
    def __init__(self, database : TinyDB, path_storage : str) -> None:

        self.database = database
        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger('SSC.Tenant')

        itens : List[Document] = []
        with LockDB(self.database, 'tenants') as table:
            itens = table.all()

        for t in itens:
            self.log.debug(f'Init tenant:{t["name"]} path:{t["path"]}')
            path = pathlib.Path(str(self.storage), t["path"]) 
            path.mkdir(parents=True, exist_ok=True)

            for n in t['namespaces']:
                self.log.debug(f'Init namespace:{t["name"]}/{n}')
                pn = pathlib.Path(path, n) 
                pn.mkdir(parents=True, exist_ok=True)


    def create(self, name : str) -> str:

        self.log.debug(f'tenant create {name}')

        path = pathlib.Path(str(self.storage), name)

        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == name)

        tot = len(itens)
        if tot == 0:
            id = table.insert({'name': name, 'path': str(path.resolve()), 'namespaces':[]})
            existe : bool = False
            for val in self.storage.iterdir():
                if val.name == name:
                    existe = True
                    break
                    
            if not existe:
                path.mkdir(parents=True, exist_ok=True)

        elif tot > 0:
            raise Exception(f'tenant {name} already exists') 


        return f'Sucess {name}'

    def delete(self, name : str) -> str:

        self.log.debug(f'tenant delete {name}')

        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == name)

        tot = len(itens)
        if tot == 0:
            raise Exception(f'tenant {name} does not exist')
        elif tot > 1:
            raise Exception(f'tenant {name} invalid values db')
            
        params = itens[0]
        lista_ns : List[str] = params['namespaces']
        if len(lista_ns) > 0:
            raise Exception(f'tenant {name} has namespaces')

        with LockDB(self.database, 'tenants', True) as table:
            table.remove(doc_ids=[params.doc_id])

        shutil.rmtree(str(params['path']), ignore_errors=True)
        return f'tenant {name} deleted'


    def list_all(self) -> List[str]:
        lista = []

        tenant_list = []
        with LockDB(self.database, 'tenants') as table:
            tenant_list = table.all()

        for val in tenant_list:
            lista.append(val['name'])

        return lista

    def create_namespace(self, name):
        self.log.debug(f'namespace create {name}')

        lista = name.split('/')
        if len(lista) != 2:
            raise Exception(f'namespace {name} is invalid')

        tenant_name = lista[0]
        ns = lista[1]

        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == tenant_name)

        tot = len(itens)
        if tot == 0:
            raise Exception(f'tenant {name} does not exist')

        params = itens[0]
        lista_ns : List[str] = params['namespaces']

        novo = pathlib.Path(os.path.join(str(params['path']), ns))
        novo.mkdir(parents=True, exist_ok=True)

        if ns in lista_ns:
            raise Exception(f'namespace {ns} already exists')

        lista_ns.append(ns)
        with LockDB(self.database, 'tenants', True) as table:
            table.update({'namespaces': lista_ns}, doc_ids=[params.doc_id])

        return f'Success {ns}'

    def list_all_namespace(self, name : str) -> List[str]:

        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == name)

        tot = len(itens)
        if tot == 0:
            raise Exception(f'tenant {name} does not exist')

        params = itens[0]
        lista_ns : List[str] = params['namespaces']
        return lista_ns

    def delete_namespace(self, name)-> str:

        self.log.debug(f'namespace delete {name}')

        # separa itens tenant de namespace
        lista = name.split('/')
        if len(lista) != 2:
            raise Exception(f'namespace {name} is invalid')

        tenant = lista[0]
        ns = lista[1]

        # pega tenant do DB
        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == tenant)

        tot = len(itens)
        if tot == 0:
            raise Exception(f'tenant {name} does not exist')

        # localiza o ns dentro do tenant
        params = itens[0]
        lista_ns : List[str] = params['namespaces']
        if ns not in lista_ns:
            raise Exception(f'namespace {name} does not exist')
        
        # verifica se não há topicos vinculados
        with LockDB(self.database, 'topics') as table:
            q = Query()
            lista = table.search((q.tenant == tenant) & (q.namespace==ns))

        final = []
        for val in lista:
            final.append(val['topic'])

        if len(lista) > 0:
            raise Exception(f'namespace {name} has topics: {str(final)}')

        # remove ns do DB
        with LockDB(self.database, 'tenants', True) as table:
            for d in lista_ns:
                if d == ns:
                    lista_ns.remove(d)

            table.update({'namespaces': lista_ns}, doc_ids=[params.doc_id])
    
        target = pathlib.Path(os.path.join(str(params['path']),ns))
        shutil.rmtree(str(target))
        return f'namespace {name} deleted'

    def find_tenant_params(self, name) -> Document:

        self.log.debug(f'find tenant {name}')

        lista = name.split('/')
        if len(lista) < 2:
            raise Exception(f'namespace {name} is invalid')

        tenant = lista[0]
        ns = lista[1]

        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == tenant)

        tot = len(itens)
        if tot == 0:
            raise Exception(f'tenant {tenant} does not exist')

        params = itens[0]
        lista_ns : List[str] = params['namespaces']
        if ns not in lista_ns:
            raise Exception(f'namespace {ns} does not exist in tenant {tenant}')

        return params

