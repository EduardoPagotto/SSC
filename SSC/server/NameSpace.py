'''
Created on 20221006
Update on 20221007
@author: Eduardo Pagotto
'''

import logging
import os
import pathlib
import shutil
from typing import List

from SSC.server.Tenant import Tenant

class NameSpace(object):
    def __init__(self, tenant : Tenant) -> None:

        self.tenant = tenant
        self.log = logging.getLogger('SSC.NameSpace')

    def create(self, name : str) -> str:

        self.log.debug(f'namesapce create {name}')

        lista = name.split('/')
        if len(lista) != 2:
            raise Exception(f'namespace {name} is invalid')

        tenant = lista[0]
        ns = lista[1]

        for val in self.tenant.storage.iterdir():
            if val.name == tenant:
                novo = pathlib.Path(os.path.join(str(self.tenant.storage), name))

                zzz = novo.is_dir()
                if not zzz:
                    novo.mkdir()
                    return f'Success {ns}'

                raise Exception(f'namespace {ns} already exists')

        raise Exception(f'tenant {tenant} does not exist')

    def delete(self, name : str) -> str:

        self.log.debug(f'namespace delete {name}')

        lista = name.split('/')
        if len(lista) != 2:
            return f'namespace {name} is invalid'

        tenant = lista[0]
        ns = lista[1]

        for val in self.tenant.storage.iterdir():
            if val.name == tenant:

                target = pathlib.Path(os.path.join(str(self.tenant.storage),name))
                shutil.rmtree(str(target))
                return f'namespace {name} deleted'

        return f'tenant {name} does not exist'

    def list_all(self, name : str) -> List[str]:
        for val in self.tenant.storage.iterdir():
            if val.name == name:

                lista = []
                for nn in val.iterdir():
                    lista.append(nn.name)                

                return lista

        raise Exception(f'tenant {name} does not exist')