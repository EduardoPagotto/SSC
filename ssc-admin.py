#!/usr/bin/env python3
'''
Created on 20220917
Update on 20220927
@author: Eduardo Pagotto
'''

import argparse
import json
import logging
import requests

from sJsonRpc.ConnectionControl import ConnectionControl
from sJsonRpc.ProxyObject import ProxyObject

class ConnectionRestApiAdmin(ConnectionControl):
    def __init__(self, addr : str):
        super().__init__(addr)

    def exec(self, input_rpc : dict, *args, **kargs) -> dict:
        url : str
        headers : dict= {'rpc-Json': json.dumps(input_rpc)}
        payload : dict ={}

        # comandos rpc's
        url = self.getUrl() + "/admin"
        files = None
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        if response.status_code != 201:
            raise Exception(response.text)

        return json.loads(response.text) # dict do rpcjson

class Admin(object):
    def __init__(self, s_address: str):
        self.restAPI = ConnectionRestApiAdmin(s_address)

    def __rpc(self):
        """Internal method to call server

        Returns:
            _type_: Connection controller
        """

        return ProxyObject(self.restAPI)

    def topics_create(self, topic : str) -> str:
        msg : str = self.__rpc().topics_create(topic)
        return msg

    def topics_delete(self, topic : str) -> str:
        msg : str = self.__rpc().topics_delete(topic, True)
        return msg


    def function_create(self, params) -> str:
        msg : str = self.__rpc().function_create(params)
        return msg

def main():

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(message)s'
    )

    logging.getLogger('werkzeug').setLevel(logging.CRITICAL) 
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    log = logging.getLogger('ssc-admin')

    admin = Admin('http://127.0.0.1:5151')

    try:
        parser_val = argparse.ArgumentParser(description='Admin command')
        zz_args = parser_val.parse_known_args()
        args = zz_args[1]

        if args[0] == 'topics':
            if args[1] == 'create':
                log.info(admin.topics_create(args[2]))
            elif args[1] == 'delete':
                log.info(admin.topics_delete(args[2]))
            else:
                log.error(f'Comando invalido: {args[2]}')
        elif args[0] == 'functions':
            if args[1] == 'create':

                parm = {'name': args[2], 'pgm':args[3], 'class':args[4], 'input':args[5], 'output':args[6]}
                log.info(admin.function_create(parm))


            elif args[1] == 'delete':
                log.info(admin.function_delete(args[2]))

            else:
                log.error(f'Comando invalido: {args[2]}')



    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        
if __name__ == '__main__':
    main()