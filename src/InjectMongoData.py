#!/usr/bin/env python3
'''
Created on 20220922
Update on 20221015
@author: Eduardo Pagotto
 '''

import logging
import json
import os
from typing import Optional
#from pulsar import Function
from SSC.Context import Context
from SSC.Function import Function
from pymongo import MongoClient

class InjectMongoData(Function):
  def __init__(self):

    self.topico_erro = "rpa/manifesto/q99Erro"
    self.mongo : Optional[MongoClient] = None

    print('InjectMongoData v1.0.0 ' + os.getcwd())

  def initialize(self, log : logging.Logger, context: Context) -> bool:
    try:
      urls = context.get_user_config_value('urls')
      self.mongo = MongoClient(urls['mongoUrl'])
      log.info('Config mongo')
      return True

    except Exception as exp:
      log.error(f'Falha no config mongo: {exp.args[0]}')

    return False

  def process(self, input, context):

    registro : dict = {}
    log : logging = context.get_logger()
    #log.debug('Key: ' + str(context.get_message_key()) + ' Linha: ' + input)

    if not self.mongo:
      if not self.initialize(log, context):
        return

    try:
      registro = json.loads(input)
      db = self.mongo['rpa_manifestacao']
      collection = db['colection_' + registro['estado']]

      collection.update_one({'numero_cnj':registro['numero_cnj']},  {'$set':registro}, upsert=True)
      return 
      
    except Exception as e:
      registro['erros'].append(str(e.args[0]))
      log.error("Falha " + str(registro))

    return json.dumps(registro)
