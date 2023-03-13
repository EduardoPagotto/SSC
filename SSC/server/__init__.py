'''
Created on 20221007
Update on 20221102
@author: Eduardo Pagotto
'''

from typing import List, Tuple

from tinydb import TinyDB, Query
from tinydb.table import Document

from SSC.subsys.LockDB import LockDB

class EstatData(object):
    def __init__(self) -> None:
        self.tot_ok = 0
        self.tot_err = 0
        self.pause = False
        self.done = False

    def summary(self):
        return {'ok' : self.tot_ok, 'err': self.tot_err, 'pause':str(self.pause)}
