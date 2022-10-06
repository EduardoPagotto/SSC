'''
Created on 20221006
Update on 20221006
@author: Eduardo Pagotto
'''

import logging
from typing import List

from tinydb import Query

from SSC.server.DataBaseCrt import DataBaseCrt
from SSC.server.Topic import Topic

class TopicDB(object):
    def __init__(self, database : DataBaseCrt) -> None:
        self.database = database
        self.log = logging.getLogger('SSC.TopicDB')

    def find(self, topic_name : str) -> Topic:

        with self.database.lock_db:
            table = self.database.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic_name)
        
        if len(itens) == 1:
            return Topic(itens[0].doc_id, itens[0]['topic'])

        raise Exception(f'topic {topic_name} does not exist')

    def create(self, topic_name : str) -> Topic:
        with self.database.lock_db:
            table = self.database.db.table('topics')
            id = table.insert({'topic': topic_name, 'name_app':'', 'user_config':''})
            return Topic(id, topic_name)
        
    def delete(self, topic_name : str) -> None:
        with self.database.lock_db:
            table = self.database.db.table('topics')
            q = Query()
            table.remove(q.topic == topic_name)

    def list_all(self, ns : str) -> List[str]:
        with self.database.lock_db:
            table = self.database.db.table('topics')
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            if ns + '/' in item['topic']:
                lista.append(item['topic'])

        return lista