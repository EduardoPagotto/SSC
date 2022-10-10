'''
Created on 20221007
Update on 20221007
@author: Eduardo Pagotto
'''

from logging import Logger, getLogger
from typing import Optional

from SSC.server.TopicCrt import TopicsCrt


class Context(object):
    def __init__(self, topic_crt : TopicsCrt, log : Logger) -> None:
        self.log = log
        self.topic_crt = topic_crt

    def get_logger(self) -> Logger:
        return self.log

    def publish(self, topic : str, data : str):
        self.topic_crt.push_name(topic, data)

    def get_message_key(self) -> Optional[dict]:
        return None 