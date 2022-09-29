
from SSC.Function import Function

class FuncAdd(Function):
    def __init__(self, val : int) -> None:
        print('contrutor OK!!!!!')
        self.val = val

    def process(self, input, context) -> str:
        return 'recebdo ' + input + ' ' + str(self.val)
    