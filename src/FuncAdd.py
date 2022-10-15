
from SSC.Function import Function

class FuncAdd(Function):
    def __init__(self) -> None:
        print('contrutor OK!!!!!')
        #self.val = val

    def process(self, input : str, context : dict) -> str:
        return 'recebdo ' + input
    