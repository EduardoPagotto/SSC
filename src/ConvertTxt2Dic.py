#!/usr/bin/env python3
'''
Created on 20220922
Update on 20221015
@author: Eduardo Pagotto
 '''

from datetime import datetime, timezone
import logging
import json
import os
import tempfile
import pathlib
#from pulsar import Function
import regex as re

from pymongo import MongoClient

from SSC.Function import Function
from SSF.ClientSSF  import ClientSSF

class ConvertTxt2Dic(Function):
  def __init__(self):
    self.topico_erro = "rpa/manifesto/q99Erro"
    self.topico_db = "rpa/manifesto/q02InjectMongo"
    self.client = MongoClient('mongodb://rpaadmin:Zaq12wsX@192.168.122.1:27017/?authSource=admin&authMechanism=SCRAM-SHA-1')
    self.nva = ClientSSF('http://192.168.122.1:5151') # TODO: colocar no json  de carga
    print('ConvertTxt2Dic v0.1.0 OK!!')
    
  def process(self, input, context):

    registro : dict = {}
    log : logging = context.get_logger()
    #log.info('Key: ' + str(context.get_message_key()) + ' Linha: ' + input)

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

        db =  self.client['rpa_manifestacao']
        coll = db['config_estado']
        estado_data =  coll.find_one({'codeId': registro['codeId']})

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
