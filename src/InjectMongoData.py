#!/usr/bin/env python3
'''
Created on 20220922
Update on 20221011
@author: Eduardo Pagotto
 '''

import logging
import json
#from pulsar import Function
from SSC.Function import Function
from pymongo import MongoClient

class InjectMongoData(Function):
  def __init__(self):

    self.topico_erro = "rpa/manifesto/q99Erro"
    print('InjectMongoData v0.0.2 OK!!')
    self.client = MongoClient('mongodb://rpaadmin:Zaq12wsX@192.168.122.1:27017/?authSource=admin&authMechanism=SCRAM-SHA-1')

  def process(self, input, context):

    registro : dict = {}
    log : logging = context.get_logger()
    #log.debug('Key: ' + str(context.get_message_key()) + ' Linha: ' + input)

    try:
      registro = json.loads(input)
      db = self.client['rpa_manifestacao']
      collection = db['colection_' + registro['estado']]

      collection.update_one({'numero_cnj':registro['numero_cnj']},  {'$set':registro}, upsert=True)
      return 
      
    except Exception as e:
      registro['erros'].append(str(e.args[0]))
      log.error("Falha " + str(registro))

    return json.dumps(registro)
