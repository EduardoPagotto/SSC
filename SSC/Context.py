'''
Created on 20221007
Update on 20230310
@author: Eduardo Pagotto
'''

from logging import Logger
from typing import Any, List, Optional

from tinydb.table import Document
from tinydb import TinyDB

from SSC.Message import Message
from SSC.server.Namespace import Namespace
from SSC.server.QueueProdCons import QueueProducer


class Context(object):
    def __init__(self, msg : Message, extra : dict[str, QueueProducer], params: Document, namespace : Namespace, log : Logger) -> None:
        self.log : Logger = log
        self.namespace : Namespace = namespace
        self.params : Document = params
        self.extra : dict[str, QueueProducer] = extra
        self.msg : Message = msg

    def get_message_id(self) -> int:
        return self.msg.message_id()

    def get_message_properties(self) -> dict:
        return self.msg.properties()

    def get_message_key(self) -> str:
        return self.msg.partition_key()

    def get_current_message_queue_name(self) -> str:
        return self.msg.queue_name()

    def get_function_name(self) -> str:
        return self.params['name']

    def get_function_namespace(self) -> str:
        return self.params['namespace']

    def get_function_id(self):
        return self.params.doc_id
        
    def get_logger(self) -> Logger:
        return self.log

    def get_user_config_value(self, key : str) -> Any:
        return self.params['useConfig'][key]

    def get_input_queues(self) -> List[str]:
        return [self.params['inputs']]

    def get_output_queue(self) -> str:
        return self.params['output']

    def publish(self, queue_name_full : str, data : str, properties : dict  = {}, msg_key : str = '' ,sequence_id : int = 0):
        
        if queue_name_full not in self.extra:
            self.extra[queue_name_full] = QueueProducer(queue_name_full, self.namespace.queue_get(queue_name_full), self.params['name']) 

        self.extra[queue_name_full].send(data, properties, msg_key, sequence_id)
