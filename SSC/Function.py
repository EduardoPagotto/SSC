'''
Created on 20220924
Update on 20221007
@author: Eduardo Pagotto
'''


from abc import ABC, abstractmethod
from logging import getLogger
from typing import Optional
from tinydb.table import Document

from SSC.server.Topic import Topic
from SSC.Context import Context


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

    @abstractmethod
    def process(self, input : str, context : Context):
        pass

    