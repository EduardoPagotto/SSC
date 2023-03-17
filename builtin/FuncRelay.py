'''
Created on 20220922
Update on 20230316
@author: Eduardo Pagotto
'''

from SSC.Function import Function
from SSC.Context import Context

class FuncRelay(Function):
    def __init__(self) -> None:
        super().__init__()

    def process(self, input : str, context : Context) -> int:

        #context.publish('test/ns01/queue03', input, context.get_message_properties(), context.get_message_key(), context.get_message_id())
        #context.publish('test/ns01/queue04', input, context.get_message_properties(), context.get_message_key(), context.get_message_id())
        context.publish(context.get_output_queue(), input, context.get_message_properties(), context.get_message_key(), context.get_message_id())
        return 1
    