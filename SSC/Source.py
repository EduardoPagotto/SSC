'''
Created on 20221108
Update on 20221108
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod
from typing import Optional

from SSC.Message import Message

class Source(ABC):

    @abstractmethod
    def start(self, config : dict) -> None:
        pass

    @abstractmethod
    def process(self, config : dict) -> Optional[Message]:
        pass
