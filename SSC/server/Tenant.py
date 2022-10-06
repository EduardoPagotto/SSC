'''
Created on 20221006
Update on 20221006
@author: Eduardo Pagotto
'''

import logging
import os
import pathlib
import shutil
from typing import List

class Tenant(object):
    def __init__(self, path_storage : str) -> None:

        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger('SSC.Tenant')

    def create(self, name : str) -> str:

        for val in self.storage.iterdir():
            if val.name == name:
                raise Exception(f'tenant {name} already exists') 

        novo = pathlib.Path(os.path.join(str(self.storage),name))
        novo.mkdir()

        return f'Sucess {name}'

    def delete(self, name : str) -> str:
        for val in self.storage.iterdir():
            if val.name == name:
                target = pathlib.Path(os.path.join(str(self.storage),name))
                shutil.rmtree(str(target))
                return f'tenant {name} deleted'

        raise Exception('tenant {name} does not exist')

    def list_all(self) -> List[str]:
        lista = []
        for val in self.storage.iterdir():
            lista.append(val.name)

        return lista