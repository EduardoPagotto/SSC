'''
Created on 20220922
Update on 20230315
@author: Eduardo Pagotto
'''

from SSC.Function import Function
from SSC.Context import Context

class Relay(Function):
    def __init__(self) -> None:
        super().__init__()

    def process(self, input : str, context : Context) -> int:

        context.publish('test/ns01/queue03', input, {}, '', 0)
        context.publish('test/ns01/queue04', input, {}, '', 0)
        context.publish(context.get_output_queue(), input, {}, '', 0)
        return 1
    