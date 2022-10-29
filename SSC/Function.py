'''
Created on 20220924
Update on 20221029
@author: Eduardo Pagotto
'''


from abc import ABC, abstractmethod
from logging import getLogger
from threading import Thread
import time
from typing import List, Optional
from tinydb.table import Document
from SSC.server.Tenant import Tenant

from SSC.Context import Context
from SSC.topic import Consumer, Producer

class Function(ABC):
    def __init__(self) -> None:
        self.log = getLogger('SSC.function')
        self.consumer : Optional[Consumer]
        self.producer : Optional[Producer]
        self.name : str = ''
        self.document : Document = Document({},0)
        self.tot_proc : int = 0
        self.tot_erro : int = 0
        self.alive : bool = True
        self.paralel : Optional[Thread] = None
        self.tenant : Optional[Tenant] = None

    @abstractmethod
    def process(self, input : str, context : Context):
        pass


    def execute(self, timeout : int):
        
        self.log.info(f'function thread start... {self.name}')

        if timeout <= 0:
            timeout = 5

        extra_map_puplish : dict[str, Producer] = {}

        while (self.alive):

            inputs = 0
            outputs = 0

            if self.consumer:

                try:

                    res = self.consumer.receive(timeout)
                    if res:
                        for k, v in res.items():
                            #self.log.debug(f'Function exec {self.name} topic in: {self.topic_in.name} ..')
                            inputs += 1
                            self.tot_proc += 1

                            try:
                                ret = self.process(v, Context(extra_map_puplish, self.document, k, self.tenant ,self.log))
                                if (self.producer) and (ret != None):

                                    outputs += 1

                                    #self.log.debug(f'Function exec {self.name} topic out: {self.topic_out.name} ..')
                                    self.producer.send(ret)

                            except Exception as exp:
                                
                                # auto nack
                                #self.topic_in.push(res)
                                #TODO: imlementar erro critico de queue 

                                self.tot_erro += 1
                                self.log.error(f'Function exec {self.name} erro: ' + exp.args[0])
                                time.sleep(1)

                            continue

                except Exception as exp:
                    self.log.error(exp.args[0])
                    self.tot_erro += 1

            if (inputs == 0) and (outputs == 0):
                time.sleep(timeout)

        self.log.info(f'function thread stop... {self.name}')