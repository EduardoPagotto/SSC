#!/usr/bin/env python3
'''
Created on 20220922
Update on 20221015
@author: Eduardo Pagotto
 '''

from datetime import datetime
import logging
import json
import os
import tempfile
import pathlib
from typing import Any, Dict, Optional
#from pulsar import Function
import regex as re

from pymongo import MongoClient

from SSC.Context import Context
from SSC.Function import Function
from SSF.ClientSSF  import ClientSSF

class ConvertTxt2Dic(Function):
  def __init__(self):

    self.topico_erro = "rpa/manifesto/q99Erro"
    self.topico_db = "rpa/manifesto/q02InjectMongo"
    self.mongo : Optional[MongoClient] = None
    self.nva : Optional[ClientSSF] = None

    self.cache_estado : Dict[int, dict] = {}

    print('ConvertTxt2Dic v1.0.0 ' + os.getcwd())

  def initialize(self, log : logging.Logger, context: Context) -> bool:
    try:
      urls = context.get_user_config_value('urls')
      self.nva = ClientSSF(urls['ssfUrl'])
      self.mongo = MongoClient(urls['mongoUrl'])
      log.info('Config nva/mongo: ' + urls['ssfUrl'])
      return True

    except Exception as exp:
      log.error('Falha no config nva/mongo')

    return False

  def localiza_estado(self, code : int) -> Any:

    for k, v in self.cache_estado.items():
      if k == code:
        return v

    if self.mongo:
      db = self.mongo['rpa_manifestacao']
      coll = db['config_estado']
      val = coll.find_one({'codeId': code})
      if val:
        self.log.info(f'Estado adicionado ao cache: {val["name"]}')
        self.cache_estado[code] = val
        return val
      else:
        self.log.critical('Estado nao cadastrado ' + str(code))

    return None

  def process(self, input, context):

    registro : dict = {}
    log : logging = context.get_logger()

    if not self.mongo:
      if not self.initialize(log, context):
        return

    try:
      registro = json.loads(input)
      id = registro['id']

      with tempfile.TemporaryDirectory() as tmp:

        valid, msg = self.nva.download(id, tmp)
        if valid is False:
          raise Exception(msg)

        path_data = pathlib.Path(msg)
        if path_data.suffix != ".txt":
          raise Exception('Nao e um txt: ' + str(path_data))

        estado_data = self.localiza_estado(registro['codeId'])
        if not estado_data:
          return

        processosx = []
        numero_processo = []
        
        with open(str(path_data), 'r') as arq:

          ler = arq.read()
          for match in re.finditer(estado_data['filter'], ler):
            processos = (match.group(0))
            processosx.append(processos)
                
          log.info(f"{path_data.name} encontrados " + str(len(processosx)))
          for da in processosx:
            if da not in numero_processo:
              numero_processo.append(da)
              data = {'estado': estado_data['name'],
                      'diario': 'Diário Oficial do Poder Judiciário Estadual',
                      'numero_cnj': da,
                      'data_arquivo': registro['create'],
                      'data_processamento': datetime.today().strftime("%d/%m/%Y")}

              #log.info('numero_cnj: ' + da)
              context.publish(self.topico_db, json.dumps(data))  

        self.nva.remove(id)
        return

    except Exception as e:
      registro['erros'].append(str(e.args[0]))
      log.error("Falha " + str(registro))

    return json.dumps(registro)
