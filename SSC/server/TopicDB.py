'''
Created on 20221006
Update on 20221010
@author: Eduardo Pagotto
'''

import logging
from typing import List

from tinydb import TinyDB, Query
from SSC.server.Topic import Topic
from SSC.subsys.LockDB import LockDB

class TopicDB(object):
    def __init__(self, database : TinyDB) -> None:
        self.database = database
        self.log = logging.getLogger('SSC.TopicDB')

    def find(self, topic_name : str) -> Topic:

        with LockDB(self.database, 'topics') as table:
            q = Query()
            itens = table.search(q.topic == topic_name)
        
        if len(itens) == 1:
            return Topic(itens[0].doc_id, itens[0]['topic'])

        raise Exception(f'topic {topic_name} does not exist')

    def create(self, topic_name : str) -> Topic:

        lista = topic_name.split('/')
        if len(lista) != 3:
            raise Exception(f'topic {topic_name} is invalid')

        with LockDB(self.database, 'topics', True) as table:
            id = table.insert({'topic': topic_name, 'tenant':lista[0], 'namespace':lista[1], 'queue':lista[2]})
            return Topic(id, topic_name)
        
    def delete(self, topic_name : str) -> None:

        with LockDB(self.database, 'topics', True) as table:
            q = Query()
            table.remove(q.topic == topic_name)

    def list_all(self, ns : str) -> List[str]:
        with LockDB(self.database, 'topics') as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            if ns + '/' in item['topic']:
                lista.append(item['topic'])

        return lista