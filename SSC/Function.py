'''
Created on 20220924
Update on 20221101
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod
from SSC.Context import Context

class Function(ABC):
    @abstractmethod
    def process(self, input : str, context : Context):
        pass
