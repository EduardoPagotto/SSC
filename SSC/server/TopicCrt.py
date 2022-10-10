'''
Created on 20221006
Update on 20221010
@author: Eduardo Pagotto
'''

import logging
from typing import Any, Dict, List, Optional

from SSC.server.TopicDB import TopicDB
from .Topic import Topic

class TopicsCrt(object):
    def __init__(self, database : TopicDB) -> None:
        self.database = database
        self.map_topics : Dict[int, Topic] = {}
        self.log = logging.getLogger('SSC.TopicsCrt')

    def summario(self) -> List[dict]:

        lista : List[dict] = []
        for k, topic in self.map_topics.items():
            lista.append({'topic': topic.name, 'size':topic.qsize()})

        return lista    


    def create(self, topic_name : str) -> Topic:

        self.log.debug(f'topic create {topic_name}')

        topic : Optional[Topic] = None
        try:
            topic = self.find_and_load(topic_name)
        except:
            pass

        if topic:
            raise Exception(f'topic {topic_name} already exists')
 
        topic = self.database.create(topic_name)

        self.map_topics[topic.id] = topic

        return topic

    def delete(self, topic_name : str):

        self.log.debug(f'topic delete {topic_name}')

        for k, v in self.map_topics.items():
            if v.name == topic_name:
                del self.map_topics[v.id]
                break

        self.database.delete(topic_name)


    def list_all(self, ns : str) -> List[str]:
        return self.database.list_all(ns)

    def find_and_load(self, topic_name : str) ->  Topic:

        for k, v in self.map_topics.items():
            if v.name == topic_name:
                #self.log.debug(f'topic find in cache {topic_name}')
                return v

        topic : Topic = self.database.find(topic_name)
        self.log.debug(f'topic find in db {topic_name}')

        self.map_topics[topic.id] = topic

        return topic

    def push_id(self, id : int, msg : str):
        self.map_topics[id].push(msg)

    def pop_id(self, id : int, timeOut: int) -> Optional[Any]:
        return self.map_topics[id].pop(timeOut)

    def push_name(self, topic_name : str, msg : str) -> None:
        self.find_and_load(topic_name).push(msg)
        
    def pop_name(self, topic_name: str, timeOut: int) -> Optional[Any]:
        return self.find_and_load(topic_name).pop(timeOut)
