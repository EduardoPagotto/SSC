'''
Created on 20221108
Update on 20221109
@author: Eduardo Pagotto
'''

import logging

from typing import Tuple

from tinydb import TinyDB
from tinydb.table import Document
from SSC.server.Cocoon import Cocoon
from SSC.server.SourceThread import SourceThread


from SSC.subsys.LockDB import LockDB

class SourceCocoon(Cocoon):
    def __init__(self, params : Document | dict, database : TinyDB) -> None:

        super().__init__('sources', params, database)

        try:
            th = SourceThread(0, self.document, self.database)
            self.list_t.append(th)
        except Exception as exp:

            self.log.critical(f'start source error {exp.args[0]}')
            with LockDB(self.database, 'sources', True) as table:
                table.remove(doc_ids=[self.document.doc_id])

            raise Exception(exp.args)
