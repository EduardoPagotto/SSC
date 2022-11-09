'''
Created on 20221108
Update on 20221109
@author: Eduardo Pagotto
'''

import logging

from typing import List, Tuple

from tinydb import TinyDB
from tinydb.table import Document
from SSC.server.SourceThread import SourceThread


from SSC.subsys.LockDB import LockDB

class SourceCocoon(object):
    def __init__(self, params : Document | dict, database : TinyDB) -> None:

        self.log = logging.getLogger('SSC.source')
        self.name = params['name']
        self.database : TinyDB = database

        if type(params) == dict:
            with LockDB(self.database, 'sources', True) as table:
                self.document = table.get(doc_id=table.insert(params))
        else:
            self.document = params

        try:
            self.thr_src = SourceThread(0, self.document, self.database)

        except Exception as exp:

            self.log.critical(f'start source error {exp.args[0]}')
            with LockDB(self.database, 'sources', True) as table:
                table.remove(doc_ids=[self.document.doc_id])

            raise Exception(exp.args)

    def start(self):
        self.log.debug(f'{self.name} signed to start')
        self.thr_src.start()

    def stop(self):
        self.log.debug(f'{self.name} signed to stop')
        self.thr_src.esta.done = True

    def join(self): 
        # FIXME: implementar um kill depois de um tempo sem resposta
        self.log.debug(f'{self.name} signed to join')
        self.thr_src.join()

    def pause(self):
        self.log.debug(f'{self.name} signed to pause')
        self.thr_src.esta.pause = True

    def resume(self):
        self.log.debug(f'{self.name} signed to resume')
        self.thr_src.esta.pause = False

    def sumary(self) -> dict:
        return {'name':self.name, 'ok': self.thr_src.esta.tot_ok, 'err': self.thr_src.esta.tot_err}

    def count_tot(self) -> Tuple[int, int]:
        return self.thr_src.esta.tot_ok, self.thr_src.esta.tot_err
