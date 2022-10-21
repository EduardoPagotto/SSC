'''
Created on 20221007
Update on 20221017
@author: Eduardo Pagotto
'''

from typing import Tuple


def splitTopic(topic : str) -> Tuple[str, str, str]:
    lista = topic.split('/')
    if len(lista) == 3:
        return lista[0], lista[1], lista[2]
    
    raise Exception(f'namespace {topic} is invalid')

def splitNamespace(namespace_name : str) -> Tuple[str, str]:

    lista = namespace_name.split('/')
    if len(lista) == 2:
        return lista[0], lista[1]

    raise Exception(f'namespace {namespace_name} is invalid')

def topic_to_redis_queue(tenant : str, namespace : str, queue : str):
    return f'{tenant}:{namespace}:{queue}'