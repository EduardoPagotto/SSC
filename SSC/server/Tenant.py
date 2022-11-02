'''
Created on 20221006
Update on 20221101
@author: Eduardo Pagotto
'''

import logging
import pathlib
from typing import List

import redis

from tinydb import TinyDB, Query
from tinydb.table import Document
from SSC.server import splitNamespace, splitTopic, topic_to_redis_queue, topic_by_namespace

from SSC.subsys.LockDB import LockDB
from SSC.topic.RedisQueue import RedisQueue

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


    def sumario(self):
        results = []

        try:
            with LockDB(self.database, 'topics') as table:
                    lista = table.all()
                    for doc in lista:
                        for q in doc['queues']:
                            queue = RedisQueue(redis.Redis.from_url(doc['redis']), topic_to_redis_queue(doc['tenant'], doc['namespace'], q))
                            results.append({'topic' : f"{doc['tenant']}/{doc['namespace']}/{q}", 'size': queue.qsize()})


        except Exception as exp:
            self.log.error(exp.args[0])

        return results



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

        doc = topic_by_namespace(self.database, tenant_name, namespace)
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
        doc = topic_by_namespace(self.database, tenant, namespace)
        if queue in doc['queues']:
            raise Exception(f'topic {queue} already exists')

        doc['queues'].append(queue)
        with LockDB(self.database, 'topics', True) as table:
            table.update({'queues' : doc['queues']}, doc_ids=[doc.doc_id])
            return f'success create {topic_name}'
        
    def list_topics(self, name) -> List:
        tenant, namespace = splitNamespace(name)
        doc = topic_by_namespace(self.database, tenant, namespace)
        return doc['queues']

    def delete_topics(self, name) -> str:

        tenant, namespace, queue = splitTopic(name)
        doc = topic_by_namespace(self.database, tenant, namespace)

        if queue in doc['queues']:
            doc['queues'].remove(queue)

            with LockDB(self.database, 'topics', True) as table:
                table.update({'queues' : doc['queues']}, doc_ids=[doc.doc_id]) # TODO verificar se nao há functions anexado ao topic
                return f'success delete {name}'

        raise Exception(f'topic {name} does not exist')  

