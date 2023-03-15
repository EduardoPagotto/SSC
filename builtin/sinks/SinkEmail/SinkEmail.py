'''
Created on 20221114
Update on 20230314
@author: Eduardo Pagotto
'''

import json
import logging
from typing import Any
from tinydb import TinyDB
from tinydb.table import Document
#from SSC.Message import Message
from SSC.Function import Function
from SSC.Context import Context

from SSC.subsys.SenderSMTP import SenderSMTP

class SinkEmail(Function):
    def __init__(self) -> None:
        super().__init__()
        self.config : dict = {}
        self.ready : bool = False
        self.log = logging.getLogger('SinkWriterFiles')

    def start(self, params : Document):
        self.config = params['config']['configs']
        self.email = SenderSMTP(self.config)
        self.ready = True

    def process(self, input : str, context : Context) -> None:

        if not self.ready:
            self.start(context.params)

        prop = context.get_message_properties() #content.properties()

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


