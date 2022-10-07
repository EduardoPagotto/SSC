'''
Created on 20221006
Update on 20221007
@author: Eduardo Pagotto
'''

import logging
import pathlib
from threading import Lock
import threading
from tinydb import TinyDB

class DataBaseCrt(object):
    def __init__(self, path_db : str) -> None:

        path1 = pathlib.Path(path_db)
        path1.mkdir(parents=True, exist_ok=True)

        self.db = TinyDB(str(path1) + '/master.json')
        self.lock_db = Lock()

        self.log = logging.getLogger('SSC.DataBaseCrt')


class AbortSignal(Exception):
    pass

def abort():
    raise AbortSignal

class LockDB(object):

    serial = 0
    write_count : int = 0
    mutex_serial = threading.Lock()
    mutex_db = threading.Lock()

    def __init__(self, db : TinyDB, table_name: str, rw : bool = False):

        with LockDB.mutex_serial:

            self.count = LockDB.serial
            LockDB.serial += 1
                    
            self.rw = rw    
            self.db : TinyDB = db
            self.table_name : str = table_name

            self.log = logging.getLogger('SSC.DBLOCK')
            self.log.debug('Transaction %d', self.count)

    def __enter__(self):
        self.log.debug('acquire %d', self.count)

        if self.rw:
            LockDB.write_count += 1

        if LockDB.write_count > 0:
            LockDB.mutex_db.lock()
            self.log.debug('acquired %d', self.count)

        return self.db.table(self.table_name)

    def __exit__(self, type, value, traceback):

        if LockDB.write_count > 0:
            LockDB.mutex_db.unlock()
            self.log.debug('release %d', self.count)

        if self.rw:
            LockDB.write_count -= 1

        return isinstance(value, AbortSignal)

    def __getattr__(self, funcion_name : str):

        if funcion_name == '__iter__':
            return None