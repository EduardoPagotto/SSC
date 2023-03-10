'''
Created on 20221114
Update on 20230310
@author: Eduardo Pagotto
'''

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server.Namespace import Namespace
from SSC.server.Cocoon import Cocoon
from SSC.server.SinkThread import SinkThread
from SSC.subsys.LockDB import LockDB

class SinkCocoon(Cocoon):
    def __init__(self, colection_name : str, params : Document | dict, namespace : Namespace) -> None:

        super().__init__(colection_name,params, namespace)

        try:
            for c in range(0, params['parallelism']):
                th = SinkThread(c, self.document, self.ns)
                self.list_t.append(th)
        except Exception as exp:
            self.log.critical(f'start sink error {exp.args[0]}')
            with LockDB(self.ns.database, self.colection_name, True) as table:
                table.remove(doc_ids=[self.document.doc_id])

            raise Exception(exp.args)
