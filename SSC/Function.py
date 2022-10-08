'''
Created on 20220924
Update on 20221007
@author: Eduardo Pagotto
'''


from abc import ABC, abstractmethod
from logging import getLogger
import time
from typing import Optional
from tinydb.table import Document

from SSC.server.Topic import Topic
from SSC.Context import Context
from SSC.server.TopicCrt import TopicsCrt


class Function(ABC):
    def __init__(self) -> None:
        self.log = getLogger('SSC.function')
        self.topic_in : Optional[Topic] = None
        self.topic_out : Optional[Topic] = None
        self.name : str = ''
        self.document : Optional[Document] = None
        self.tot_input : int = 0
        self.tot_output : int = 0
        self.tot_erro : int = 0
        self.alive : bool = True

    @abstractmethod
    def process(self, input : str, context : Context):
        pass


    def execute(self, topic_crt : TopicsCrt, timeout : int):
        
        self.log.info(f'function thread start... {self.name}')

        if timeout <= 0:
            timeout = 5

        while (self.alive):

            inputs = 0
            outputs = 0

            self.log.info(f'function thread tiktak ... {self.name}')

            if (self.topic_in) and (self.topic_in.qsize() > 0):

                res = self.topic_in.pop(timeout)
                if res:

                    self.log.debug(f'Function exec {self.name} topic in: {self.topic_in.name} ..')
                    inputs += 1

                    try:

                        ret = self.process(res, Context(topic_crt, self.log))

                    except Exception as exp:
                        
                        # auto nack
                        self.topic_in.push(res)

                        self.tot_erro += 1
                        self.log.error(f'Function exec {self.name} erro: ' + exp.args[0])
                        time.sleep(1)

                    if (self.topic_out) and (ret != None):

                        self.log.debug(f'Function exec {self.name} topic out: {self.topic_out.name} ..')
                        self.topic_out.push(ret)
                        self.tot_output += 1

                        outputs += 1

            if (inputs == 0) and (outputs == 0):
                time.sleep(timeout)
            else:
                self.tot_input += inputs
                self.tot_output += outputs


        self.log.info(f'function thread stop... {self.name}')