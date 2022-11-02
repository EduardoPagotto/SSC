'''
Created on 20221101
Update on 20221102
@author: Eduardo Pagotto
'''

import logging

from typing import List, Tuple

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server.FuncThread import FuncThread
from SSC.subsys.LockDB import LockDB

class FuncCocoon(object):
    def __init__(self, params : Document | dict, database : TinyDB) -> None:

        self.log = logging.getLogger('SSC.function')
        self.name = params['name']
        self.database : TinyDB = database

        self.list_t : List[FuncThread] = []

        if type(params) == dict:
            with LockDB(self.database, 'funcs', True) as table:
                self.document = table.get(doc_id=table.insert(params))
        else:
            self.document = params

        try:
            for c in range(0, params['parallelism']):
                th = FuncThread(c, self.document, self.database)
                self.list_t.append(th)
        except Exception as exp:
            self.log.critical(f'start func error {exp.args[0]}')
            with LockDB(self.database, 'funcs', True) as table:
                table.remove(doc_ids=[self.document.doc_id])

            raise Exception(exp.args)


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
