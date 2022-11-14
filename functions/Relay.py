'''
Created on 20220922
Update on 20221114
@author: Eduardo Pagotto
'''

from SSC.Function import Function
from SSC.Context import Context

class Relay(Function):
    def __init__(self) -> None:
        print('contrutor OK!!!!!')
        #self.val = val

    def process(self, input : str, context : Context) -> str:
        return input
    