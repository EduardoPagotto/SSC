'''
Created on 20221006
Update on 20221006
@author: Eduardo Pagotto
'''

import logging
import pathlib
from threading import Lock
from tinydb import TinyDB

class DataBaseCrt(object):
    def __init__(self, path_db : str) -> None:

        path1 = pathlib.Path(path_db)
        path1.mkdir(parents=True, exist_ok=True)

        self.db = TinyDB(str(path1) + '/master.json')
        self.lock_db = Lock()

        self.log = logging.getLogger('SSC.DataBaseCrt')

# class ZeroDbLock(object):

#     serial = 0
#     mutex_serial = threading.Lock()

#     def __init__(self, session : SessionDB, table_name: str):

#         with ZeroDbLock.mutex_serial:

#             self.count = ZeroDbLock.serial
#             ZeroDbLock.serial += 1

#             self.session : SessionDB = session
#             self.table_name : str = table_name
#             self.log = logging.getLogger('ZeroDB')
#             self.log.debug('Transaction %d', self.count)

#     def __enter__(self):
#         self.log.debug('acquire %d', self.count)
#         self.session.lock()
#         self.session.table(self.table_name)
#         self.log.debug('acquired %d', self.count)
#         return self

#     def __exit__(self, type, value, traceback):

#         #if not traceback: # FIXME: ver como se comporta no crash
#         self.session.unlock()
#         self.log.debug('release %d', self.count)
#         return isinstance(value, AbortSignal)

#     def __getattr__(self, funcion_name : str):

#         if funcion_name == '__iter__':
#             return None