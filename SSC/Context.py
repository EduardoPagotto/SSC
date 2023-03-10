'''
Created on 20221007
Update on 20221120
@author: Eduardo Pagotto
'''

from logging import Logger
from typing import Any, List, Optional

from tinydb.table import Document
from tinydb import TinyDB
from SSC.Message import Message

from SSC.server import splitTopic, topic_by_namespace, topic_to_redis_queue
from SSC.server.QueueProdCons import QueueProducer


class Context(object):
    def __init__(self, msg : Message, extra : dict[str, QueueProducer], params: Document, database : TinyDB, log : Logger) -> None:
        self.log : Logger = log
        self.database : TinyDB = database
        self.params : Document = params

        self.extra : dict[str, QueueProducer] = extra
        self.msg : Message = msg

    def get_message_id(self) -> int:
        return self.msg.message_id()

    def get_message_properties(self) -> dict:
        return self.msg.properties()

    def get_message_key(self) -> str:
        return self.msg.partition_key()

    def get_current_message_topic_name(self) -> str:
        return self.msg.topic_name()

    def get_function_name(self) -> str:
        return self.params['name']

    def get_function_tenant(self) -> str:
        return self.params['tenant']

    def get_function_namespace(self) -> str:
        return self.params['namespace']

    def get_function_id(self):
        return self.params.doc_id
        
    def get_logger(self) -> Logger:
        return self.log

    def get_user_config_value(self, key : str) -> Any:
        return self.params['useConfig'][key]

    def get_input_topics(self) -> List[str]:
        return [self.params['inputs']]

    def get_output_topic(self) -> str:
        return self.params['output']

    def publish(self, topic : str, data : str, properties : dict  = {}, msg_key : str = '' ,sequence_id : int = 0):

        if topic not in self.extra:
            tenant_name, namespace, queue = splitTopic(topic)

            doc = topic_by_namespace(self.database, tenant_name, namespace) # FIXME!!!!! vou precisdar do DB!!!!
            if queue in doc['queues']:
                self.extra[topic] = QueueProducer(doc['redis'], topic_to_redis_queue(tenant_name, namespace, queue), self.params['name'])                
            else:
                raise Exception(f'topic invalid im publish func ' + topic)
        
        self.extra[topic].send(data, properties, msg_key, sequence_id)
