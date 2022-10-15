#!/usr/bin/env python3
'''
Created on 20220922
Update on 20221011
@author: Eduardo Pagotto
 '''

import logging
import json
import os
import tempfile
import pathlib
from SSC.Function import Function
from SSF.ClientSSF  import ClientSSF

class ConvertePDF2TXT(Function):
  def __init__(self):
    self.topico_erro = "rpa/manifest/q99Erro"
    self.nva = ClientSSF('http://192.168.122.1:5151') # TODO: colocar no json  de carga
    print('ConvertePDF2TXT v0.1.0 OK ' + os.getcwd())

  def process(self, input, context):

    registro : dict = {}
    log : logging = context.get_logger()

    try:
      registro = json.loads(input)
      id = registro['id']
      if id >= 0:

        with tempfile.TemporaryDirectory() as tmp:
          valid, msg = self.nva.download(id, tmp)

          if valid is False:
            raise Exception(msg)

          arquivo = pathlib.Path(msg)

          if arquivo.suffix != ".pdf":
            return json.dumps(registro)

          caminho_modificado = str(arquivo).replace('.pdf', '.txt')

          if os.system('pdftotext {0} {1}'.format(str(arquivo), caminho_modificado)) == 0:
            #valid, msg, registro['id'] = self.nva.upload(caminho_modificado)
            novo_id, msg = self.nva.upload(caminho_modificado)

            if novo_id > 0:
              registro['id'] = novo_id
              self.nva.remove(id)
              log.info('Novo arquivo: ' + caminho_modificado)
              return json.dumps(registro)

            else:
              raise Exception(msg)

          else:
            raise Exception('PDF falha na conversao')

      else:
        raise Exception('ID invalido')

    except Exception as e:
      registro['erros'].append(str(e.args[0]))
      context.publish(self.topico_erro, json.dumps(registro))
      log.error("Falha " + str(registro))

