'''
Created on 20221101
Update on 20221102
@author: Eduardo Pagotto
'''

import logging

from typing import Any, List, Tuple

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server.Cocoon import Cocoon
from SSC.server.FuncThread import FuncThread
from SSC.subsys.LockDB import LockDB


class FuncCocoon(Cocoon):
    def __init__(self, params : Document | dict, database : TinyDB) -> None:

        super().__init__('funcs',params, database)

        try:
            for c in range(0, params['parallelism']):
                th = FuncThread(c, self.document, self.database)
                self.list_t.append(th)
        except Exception as exp:
            self.log.critical(f'start func error {exp.args[0]}')
            with LockDB(self.database, 'funcs', True) as table:
                table.remove(doc_ids=[self.document.doc_id])

            raise Exception(exp.args)
