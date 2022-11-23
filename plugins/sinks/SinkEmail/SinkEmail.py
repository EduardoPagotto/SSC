'''
Created on 20221114
Update on 20221123
@author: Eduardo Pagotto
'''

import json
import logging
from typing import Any
from tinydb import TinyDB
from tinydb.table import Document
from SSC.Message import Message
from SSC.Sink import Sink

from SSC.subsys.SenderSMTP import SenderSMTP

class SinkEmail(Sink):
    def __init__(self) -> None:
        super().__init__()
        self.log = logging.getLogger('SinkWriterFiles')

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.config = doc['config']['configs']
        self.email = SenderSMTP(self.config)

        return self.config['delay'] if 'delay' in self.config else 5

    def process(self, content : Message) -> None:

        prop = content.properties()

        if 'subject' not in prop:
            raise Exception('Missing subject in properties')

        if 'body' not in prop:
            raise Exception('Missing body in properties')

        #val = {'subject':prop['subject'], 'body':prop['body']}

        status, msg = self.email.send_mail(prop, False)
        if status is True:
            self.log.debug(msg)
        else:
            self.log.error(msg)


