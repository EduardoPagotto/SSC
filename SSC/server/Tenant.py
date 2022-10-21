'''
Created on 20221006
Update on 20221022
@author: Eduardo Pagotto
'''

import logging
import pathlib
from typing import List, Tuple

from tinydb import TinyDB, Query
from tinydb.table import Document
from SSC.server import splitNamespace, splitTopic, topic_to_redis_queue

from SSC.subsys.LockDB import LockDB

class Tenant(object):
    def __init__(self, database : TinyDB, path_storage : str, urlRedis : str) -> None:

        self.database = database
        self.redis_url = urlRedis
        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger('SSC.Tenant')


    def find_tenant_by_name(self, tenant_name : str) -> Document:
        with LockDB(self.database, 'tenants') as table:
            q = Query()
            itens = table.search(q.name == tenant_name)
            if len(itens) > 0:
                return itens[0]

        raise Exception(f'tenant {tenant_name} does not exist')

    def __find_namespace_from_name(self, tenant_name : str, namespace_name : str) -> Document:

        tn = self.find_tenant_by_name(tenant_name)
        with LockDB(self.database, 'namespaces') as table:
            q = Query()
            list_ns = table.search((q.tenant == tn.doc_id) & (q.name == namespace_name))
            if len(list_ns) > 0:
                return list_ns[0]
            
        raise Exception(f'namespace {namespace_name} does not exist in tenant {tenant_name}')


    def __find_namespace_from_tenant_id(self, tenant_id : int) -> List[Document]:
        with LockDB(self.database, 'namespaces') as table:
            q = Query()
            return table.search(q.tenant == tenant_id)


    def find_namespace_from_tenant_id_name(self, tenant_id : int, namespace_name : str) -> Document:

        with LockDB(self.database, 'namespaces') as table:
            q = Query()
            list_ns = table.search((q.tenant == tenant_id) & (q.name == namespace_name))
            if len(list_ns) > 0:
                return list_ns[0]

        raise Exception(f'namespace {namespace_name} does not exist')

    def hasQueue(self, tenant_name : str, namespace_name : str, queue_name : str) -> bool:
        try:
            ns = self.__find_namespace_from_name(tenant_name, namespace_name)
            for item in ns['queues']:
                if item == queue_name:
                    return True
        except:
            pass

        return False



    def create(self, name : str) -> str:

        self.log.debug(f'tenant create {name}')
        exist : bool = False
        try:
            self.find_tenant_by_name(name)
            exist = True
        except:
            pass

        if exist:
            raise Exception(f'tenant {name} already exists')

        with LockDB(self.database, 'tenants', True) as table:
            id = table.insert({'name': name,'redis':self.redis_url})

        return f'Sucess {name} id: {id}'

    def delete(self, name : str) -> str:

        self.log.debug(f'tenant delete {name}')

        tn = self.find_tenant_by_name(name)
        ns_list = self.__find_namespace_from_tenant_id(tn.doc_id)
        if len(ns_list) == 0:
            with LockDB(self.database, 'tenants', True) as table:
                table.remove(doc_ids=[tn.doc_id])
                return f'tenant {name} deleted'
        else:
            names = []
            for v in ns_list:
                names.append(v['name'])

            raise Exception(f'tenant has namespaces: {str(names)}')
        


    def list_all(self) -> List[str]:
        lista = []

        tenant_list = []
        with LockDB(self.database, 'tenants') as table:
            tenant_list = table.all()

        for val in tenant_list:
            lista.append(val['name'])

        return lista

    

    def create_namespace(self, name : str):
        self.log.debug(f'namespace create {name}')

        tenant_name, namespace = splitNamespace(name)

        tn = self.find_tenant_by_name(tenant_name)

        exist : bool = False
        try:
            self.find_namespace_from_tenant_id_name(tn.doc_id, namespace)
            exist = True
        except:
            pass

        if exist:
            raise Exception(f'namespace {name} already exists')

        with LockDB(self.database, 'namespaces', True) as table:
            id = table.insert({'name':namespace, "tenant": tn.doc_id, "queues":[]})
            return f'Success {name} id: {id}'
        
        

    def list_all_namespace(self, name : str) -> List[str]:

        tn = self.find_tenant_by_name(name)
        ns = self.__find_namespace_from_tenant_id(tn.doc_id)
        if ns:
            lista_ns = []
            for val in ns:
                lista_ns.append(val['name'])

            return lista_ns

        raise Exception(f'namespace {name} does not exist')    


    def delete_namespace(self, name)-> str:

        self.log.debug(f'namespace delete {name}')

        tenant_name, namespace = splitNamespace(name)

        tn = self.find_tenant_by_name(tenant_name)
        ns = self.find_namespace_from_tenant_id_name(tn.doc_id, namespace)
        lista = ns['queues'] 
        if len(lista) > 0:
            raise Exception(f'namespace {namespace} has topics: {str(lista)}')

        with LockDB(self.database, 'namespaces', True) as table:
            table.remove(doc_ids=[ns.doc_id])
            return f'namespace {name} deleted'

    def create_topic(self, topic_name : str) -> str:

        tenant, namespace, queue = splitTopic(topic_name)

        ns = self.__find_namespace_from_name(tenant, namespace)
        if queue not in ns['queues']:
            ns['queues'].append(queue)
            with LockDB(self.database, 'namespaces', True) as table:
                table.update({'queues' : ns['queues']}, doc_ids=[ns.doc_id])
                return f'success create {topic_name}'
        
        raise Exception(f'topic {queue} already exists')


    def list_topics(self, name) -> List:

        tenant, namespace = splitNamespace(name)

        ns = self.__find_namespace_from_name(tenant, namespace)
        return ns['queues']
                   
    def delete_topics(self, name) -> str:

        tenant, namespace, queue = splitTopic(name)

        ns = self.__find_namespace_from_name(tenant, namespace)
        lista_queue : List[str] = ns['queues']
        if queue in lista_queue:
            lista_queue.remove(queue)
            with LockDB(self.database, 'namespaces', True) as table:
                table.update({'queues' : lista_queue}, doc_ids=[ns.doc_id]) # TODO verificar se nao há functions anexado ao topic
                return f'success delete {name}'

        raise Exception(f'topic {name} does not exist')  
  

    def create_queue(self, topic_name : str) -> dict:
        
        tenant, namespace, queue = splitTopic(topic_name)
        tn = self.find_tenant_by_name(tenant)
        ns = self.find_namespace_from_tenant_id_name(tn.doc_id, namespace)
        if queue in ns['queues']:
            return{'urlRedis' : tn['redis'], 'queue' : topic_to_redis_queue(tenant, namespace, queue)}

        raise Exception(f'topic {topic_name} does not exist') 


    def create_queues(self, topics_name : List[str]) -> dict:

        last_tenant = ''
        tn : Document = Document({},0)
        redisUrl = ''
        lista_topics : List[str] = []
        for item in topics_name:

            tenant, namespace, queue = splitTopic(item)
            if last_tenant == '':
                last_tenant = tenant
                tn = self.find_tenant_by_name(tenant)
                redisUrl = tn['redis']

            ns = self.find_namespace_from_tenant_id_name(tn.doc_id, namespace)
            if queue in ns['queues']:
                lista_topics.append(topic_to_redis_queue(tenant, namespace, queue))
            else:
                raise Exception(f'topic {item} does not exist') 

        return {'urlRedis': redisUrl, 'queue' : lista_topics}