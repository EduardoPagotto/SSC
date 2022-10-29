'''
Created on 20221006
Update on 20221029
@author: Eduardo Pagotto
'''

import logging
import pathlib
from typing import List

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


    def find_topics_by_tenant(self, tenant) -> List[Document]:
        with LockDB(self.database, 'topics') as table:
            q = Query()
            itens = table.search(q.tenant == tenant)
            if len(itens) > 0:
                return itens

        raise Exception(f'tenant {tenant} does not exist')        

    def find_topic_by_namespace(self, tenant, namespace) -> Document:
        with LockDB(self.database, 'topics') as table:
            q = Query()
            itens = table.search((q.tenant == tenant) & (q.namespace == namespace ))
            if len(itens) == 1:
                return itens[0]

        raise Exception(f'tenant ou namespace {tenant}/{namespace} does not exist') 

    def create(self, name : str) -> str:

        self.log.debug(f'tenant create {name}')

        tot = 0
        try:
            tot = len(self.find_topics_by_tenant(name))
        except:
            pass

        if tot > 0:
            raise Exception(f'tenant {name} already exists')

        with LockDB(self.database, 'topics', True) as table:
            id = table.insert({'tenant': name,
                               'redis':self.redis_url,
                               'namespace':None,
                               'queues':[]})

        return f'Sucess {name} id: {id}'

    def delete(self, name : str) -> str:

        self.log.debug(f'tenant delete {name}')

        lista = self.find_topics_by_tenant(name)
        tot = len(lista)
        if tot == 1:
            doc = lista[0]
            if len(doc['queues']) > 0:
                raise Exception(f'tenant has queues: {str(doc["queues"])}')

            if doc['namespace'] != None:
                raise Exception(f'tenant has namespace: {doc["namespace"]}')

            with LockDB(self.database, 'topics', True) as table:
                table.remove(doc_ids=[doc.doc_id])
                return f'tenant {name} deleted'            

        ns_list : List[str] = []
        for doc in lista:
            ns_list.append(doc['namespace'])

        raise Exception(f'tenant has namespaces: {str(ns_list)}')

    def list_all(self) -> List[str]:
        lista = []

        tenant_list = []
        with LockDB(self.database, 'topics') as table:
            tenant_list = table.all()

        for val in tenant_list:
            if val['tenant'] not in lista:
                lista.append(val['tenant'])

        return lista

    def create_namespace(self, name : str):
        self.log.debug(f'namespace create {name}')

        tenant_name, namespace = splitNamespace(name)

        lista = self.find_topics_by_tenant(tenant_name)
        tot = len(lista)
        if tot == 1:
            doc = lista[0]
            if doc['namespace'] == None:
                doc['namespace'] = namespace
                with LockDB(self.database, 'topics', True) as table:
                    table.update(doc, doc_ids=[doc.doc_id])
                    return f'Success {name} id: {doc.doc_id}'  
        

        for doc in lista:
            if doc['namespace'] == namespace:
                raise Exception(f'namespace {name} already exists')   


        with LockDB(self.database, 'topics', True) as table:
            id = table.insert({'tenant': tenant_name,
                                'redis':self.redis_url,
                                'namespace':namespace,
                                'queues':[]})  
            return f'Success {name} id: {id}'

    def list_all_namespace(self, name : str) -> List[str]:

        lista_ns = []
        lista = self.find_topics_by_tenant(name)
        for doc in lista:
            if doc['namespace'] != None:
                lista_ns.append(doc['namespace'])            

        return lista_ns

    def delete_namespace(self, name)-> str:

        self.log.debug(f'namespace delete {name}')

        tenant_name, namespace = splitNamespace(name)

        doc = self.find_topic_by_namespace(tenant_name, namespace)
        if len(doc['queues']) > 0:
            raise Exception(f'namespace has queues: {str(doc["queues"])}')

        if len(self.find_topics_by_tenant(tenant_name)) == 1:
            doc['namespace'] = None
            with LockDB(self.database, 'topics', True) as table:
                table.update(doc, doc_ids=[doc.doc_id])
                return f'namespace {name} deleted'

        with LockDB(self.database, 'topics', True) as table:
            table.remove(doc_ids=[doc.doc_id])
            return f'namespace {name} deleted'


    def create_topic(self, topic_name : str) -> str:

        tenant, namespace, queue = splitTopic(topic_name)
        doc = self.find_topic_by_namespace(tenant, namespace)
        if queue in doc['queues']:
            raise Exception(f'topic {queue} already exists')

        doc['queues'].append(queue)
        with LockDB(self.database, 'topics', True) as table:
            table.update({'queues' : doc['queues']}, doc_ids=[doc.doc_id])
            return f'success create {topic_name}'
        
    def list_topics(self, name) -> List:
        tenant, namespace = splitNamespace(name)
        doc = self.find_topic_by_namespace(tenant, namespace)
        return doc['queues']

    def delete_topics(self, name) -> str:

        tenant, namespace, queue = splitTopic(name)
        doc = self.find_topic_by_namespace(tenant, namespace)

        if queue in doc['queues']:
            doc['queues'].remove(queue)

            with LockDB(self.database, 'topics', True) as table:
                table.update({'queues' : doc['queues']}, doc_ids=[doc.doc_id]) # TODO verificar se nao há functions anexado ao topic
                return f'success delete {name}'

        raise Exception(f'topic {name} does not exist')  

    def create_queue(self, topic_name : str) -> dict:
        
        tenant, namespace, queue = splitTopic(topic_name)
        doc = self.find_topic_by_namespace(tenant, namespace)
        if queue in doc['queues']:
            return{'urlRedis' : doc['redis'], 'queue' : topic_to_redis_queue(tenant, namespace, queue)}

        raise Exception(f'topic {topic_name} does not exist') 

    def create_queues(self, topics_name : List[str]) -> dict:

        redisUrl = ''
        lista_topics : List[str] = []
        for item in topics_name:

            tenant, namespace, queue = splitTopic(item)

            doc = self.find_topic_by_namespace(tenant, namespace)
            if redisUrl == '':
                redisUrl = doc['redis']

            if queue in doc['queues']:
                lista_topics.append(topic_to_redis_queue(tenant, namespace, queue))
            else:
                raise Exception(f'topic {item} does not exist') 

        return {'urlRedis': redisUrl, 'queue' : lista_topics}