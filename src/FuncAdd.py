
from SSC.Function import Function
from SSC.Context import Context

class FuncAdd(Function):
    def __init__(self) -> None:
        print('contrutor OK!!!!!')
        #self.val = val

    def process(self, input : str, context : Context) -> str:
        return 'recebido ' + input
    