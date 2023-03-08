'''
Created on 20221007
Update on 20221102
@author: Eduardo Pagotto
'''

from typing import List, Tuple

from tinydb import TinyDB, Query
from tinydb.table import Document

from SSC.subsys.LockDB import LockDB

class EstatData(object):
    def __init__(self) -> None:
        self.tot_ok = 0
        self.tot_err = 0
        self.pause = False
        self.done = False

    def summary(self):
        return {'ok' : self.tot_ok, 'err': self.tot_err, 'pause':str(self.pause)}


def splitTopic(topic : str) -> Tuple[str, str, str]:
    lista = topic.split('/')
    if len(lista) == 3:
        return lista[0], lista[1], lista[2]
    
    raise Exception(f'namespace {topic} is invalid')


def splitQueue(q_fulll_name : str) -> Tuple[str, str]:
    lista = q_fulll_name.split('/')
    if len(lista) == 3:
        return lista[0] + '/' + lista[1], lista[2] #TODO: implementar melhor
    
    raise Exception(f'queue name {q_fulll_name} is invalid')


def splitNamespace(namespace_name : str) -> Tuple[str, str]:

    lista = namespace_name.split('/')
    if len(lista) == 2:
        return lista[0], lista[1]

    raise Exception(f'namespace {namespace_name} is invalid')

def topic_to_redis_queue(tenant : str, namespace : str, queue : str):
    return f'{tenant}:{namespace}:{queue}'


def topic_by_namespace(database : TinyDB, tenant : str, namespace : str) -> Document:
    with LockDB(database, 'topics') as table:
        q = Query()
        itens = table.search((q.tenant == tenant) & (q.namespace == namespace ))
        if len(itens) == 1:
            return itens[0]

    raise Exception(f'tenant ou namespace {tenant}/{namespace} does not exist')

def create_queue(database : TinyDB, topic_name : str) -> dict:
    
    tenant, namespace, queue = splitTopic(topic_name)
    doc = topic_by_namespace(database, tenant, namespace)
    if queue in doc['queues']:
        return {'urlRedis' : doc['redis'], 'queue' : topic_to_redis_queue(tenant, namespace, queue)}

    raise Exception(f'topic {topic_name} does not exist') 

def create_queues(database : TinyDB, topics_name : List[str]) -> dict:

    redisUrl = ''
    lista_topics : List[str] = []
    for item in topics_name:

        tenant, namespace, queue = splitTopic(item)

        doc = topic_by_namespace(database, tenant, namespace)
        if redisUrl == '':
            redisUrl = doc['redis']

        if queue in doc['queues']:
            lista_topics.append(topic_to_redis_queue(tenant, namespace, queue))
        else:
            raise Exception(f'topic {item} does not exist') 

    return {'urlRedis': redisUrl, 'queue' : lista_topics}