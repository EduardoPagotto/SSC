'''
Created on 20221007
Update on 20221022
@author: Eduardo Pagotto
'''

from logging import Logger, getLogger
from typing import Any, List, Optional
from tinydb.table import Document
from SSC.server import splitTopic

from SSC.server.Tenant import Tenant
from SSC.topic.QueueProdCons import Producer, QueueProducer


class Context(object):
    def __init__(self, params: Document, curr_topic : str, tenant : Tenant, log : Logger) -> None:
        self.__log : Logger = log
        self.__curr_topic : str = curr_topic
        self.__tenant : Tenant = tenant
        self.__params = params

        self.__map_puplish : dict[str, Producer] = {}

    def get_message_key(self) -> Optional[dict]:
        return None # TODO: implementar

    def get_current_message_topic_name(self) -> str:
        return self.__curr_topic

    def get_function_name(self) -> str:
        return self.__params['name']

    def get_function_tenant(self) -> str:
        return self.__params['tenant']

    def get_function_namespace(self) -> str:
        return self.__params['namespace']

    def get_function_id(self):
        return self.__params.doc_id
        
    def get_logger(self) -> Logger:
        return self.__log

    def get_user_config_value(self, key : str) -> Any:
        return self.__params['useConfig'][key]

    def get_input_topics(self) -> List[str]:
        return [self.__params['inputs']]

    def get_output_topic(self) -> str:
        return self.__params['output']

    def publish(self, topic : str, data : str):

        if topic not in self.__map_puplish: # FIXME: trocar para a base de func
            tenant_name, namespace, queue = splitTopic(topic)
            tn = self.__tenant.find_tenant_by_name(tenant_name)
            if self.__tenant.hasQueue(tenant_name, namespace, queue):
                self.__map_puplish[topic] = QueueProducer(tn['redis'], topic.replace('/',':'))
            else:
                raise Exception(f'topic invalid im publish func ' + topic)
        
            self.__map_puplish[topic].send(data)
