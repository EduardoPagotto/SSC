'''
Created on 20221110
Update on 20221110
@author: Eduardo Pagotto
'''

import logging

from typing import Any, List, Tuple

from tinydb import TinyDB
from tinydb.table import Document

from SSC.subsys.LockDB import LockDB

class Cocoon(object):
    def __init__(self, sufix : str, params : Document | dict, database : TinyDB) -> None:

        self.log = logging.getLogger(f'SSC.{sufix}')
        self.name = params['name']
        self.database : TinyDB = database

        self.list_t : List[Any] = []

        if type(params) == dict:
            with LockDB(self.database, 'funcs', True) as table:
                self.document = table.get(doc_id=table.insert(params))
        else:
            self.document = params

    def start(self):
        self.log.debug(f'{self.name} signed to start')
        for t in self.list_t:
            t.start()

    def stop(self):
        self.log.debug(f'{self.name} signed to stop')
        for t in self.list_t:
            t.esta.done = True

    def join(self): 
        # FIXME: implementar um kill depois de um tempo sem resposta
        self.log.debug(f'{self.name} signed to join')
        for t in self.list_t:
            t.join()

    def pause(self):
        self.log.debug(f'{self.name} signed to pause')
        for t in self.list_t:
            t.esta.pause = True

    def resume(self):
        self.log.debug(f'{self.name} signed to resume')
        for t in self.list_t:
            t.esta.pause = False

    def sumary(self) -> dict:

        lista = []
        tot_ok = 0
        tot_err = 0
        for t in self.list_t:
            lista.append({'thread': t.name , 'estat': t.esta.summary()})
            tot_ok += t.esta.tot_ok
            tot_err += t.esta.tot_err

        return {'name':self.name, 'ok': tot_ok, 'err': tot_err,'threads': lista}

    def count_tot(self) -> Tuple[int, int]:

        tot_ok = 0
        tot_err = 0
        for t in self.list_t:
            tot_ok += t.esta.tot_ok
            tot_err += t.esta.tot_err

        return tot_ok, tot_err