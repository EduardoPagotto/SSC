#!/usr/bin/env python3
'''
Created on 20180208
Update on 20190917
@author: Eduardo Pagotto
'''

class Singleton(type):
    """
    Pattner de Singleton no-thread-safe
    """
    def __init__(cls, name, bases, attrs, **kwargs):
        '''
        inicializa objeto vazio
        '''
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        '''
        executa uma nova instancia ou retorna a existente 
        '''
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance

# class MyClass(metaclass=Singleton):
#     """
#     Example class.
#     """
#     pass

# def main():
#     m1 = MyClass()
#     m2 = MyClass()
#     assert m1 is m2

# if __name__ == "__main__":
#     main()
