'''
Created on 20221007
Update on 20221120
@author: Eduardo Pagotto
'''

import logging
import threading
from tinydb import TinyDB

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
            #self.log.debug('Transaction %d', self.count)

    def __enter__(self):
        
        if self.rw:
            LockDB.write_count += 1

        if LockDB.write_count > 0:
            LockDB.mutex_db.acquire()
            #self.log.debug('acquire %d', self.count)

        return self.db.table(self.table_name)

    def __exit__(self, type, value, traceback):

        if LockDB.write_count > 0:
            LockDB.mutex_db.release()
            #self.log.debug('release %d', self.count)

        if self.rw:
            LockDB.write_count -= 1
            if LockDB.write_count < 0:
                LockDB.write_count = 0
                self.log.critical('Falha critica no unlock')

        return isinstance(value, AbortSignal)

    def __getattr__(self, funcion_name : str):

        if funcion_name == '__iter__':
            return None