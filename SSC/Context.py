'''
Created on 20221007
Update on 20221015
@author: Eduardo Pagotto
'''

from logging import Logger, getLogger
from typing import Any, List, Optional
from tinydb.table import Document

from SSC.server.TopicCrt import TopicsCrt


class Context(object):
    def __init__(self, params: Document, topic_crt : TopicsCrt, log : Logger) -> None:
        self.__log : Logger = log
        self.__topic_crt : TopicsCrt = topic_crt
        self.__params = params

    def get_message_key(self) -> Optional[dict]:
        return None # TODO: implementar

    def get_current_message_topic_name(self) -> str:
        return '' # TODO: implementar

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
        self.__topic_crt.push_name(topic, data)