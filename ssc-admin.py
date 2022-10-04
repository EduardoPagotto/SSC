#!/usr/bin/env python3
'''
Created on 20220917
Update on 20220927
@author: Eduardo Pagotto
'''

import argparse
import json
import logging
from typing import List
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

    def topics_list(self) -> List[str]:
        msg : List[str] = self.__rpc().topics_list()
        return msg

    def function_create(self, params) -> str:
        msg : str = self.__rpc().function_create(params)
        return msg

    def function_delete(self, name : str) -> str:
        msg : str = self.__rpc().function_delete(name)
        return msg

    def functions_list(self) -> List[str]:
        msg : List[str] = self.__rpc().functions_list()
        return msg

def main():

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(message)s'
    )

    logging.getLogger('werkzeug').setLevel(logging.CRITICAL) 
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    log = logging.getLogger('ssc-admin')

    admin = Admin('http://127.0.0.1:5152')

    try:
        parser = argparse.ArgumentParser(description='Admin command')
        subparser = parser.add_subparsers(dest='command')

        topic = subparser.add_parser('topics')
        topic.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        topic.add_argument('queue', type=str, help='nome da queue', default='')

        funcions = subparser.add_parser('functions')
        funcions.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        funcions.add_argument('--name', type=str, help='nome da thread')
        funcions.add_argument('--py', type=str, help='python script pathfile')
        funcions.add_argument('--classname', type=str, help='Nome da classe')
        funcions.add_argument('--inputs', type=str, help='queue input')
        funcions.add_argument('--output', type=str, help='queue output')

        args = parser.parse_args()

        if args.command == 'topics':
            if args.opp == 'create':
                log.info(admin.topics_create(args.queue))
            elif args.opp == 'delete':
                log.info(admin.topics_delete(args.queue))
            elif args.opp == 'list':
                log.info(admin.topics_list())
            else:
                log.error(f'Opp invalida: {args.opp}')
        elif args.command == 'functions':
            if args.opp == 'create':

                parm = {'name': args.name, 
                        'pgm':args.py,
                        'class':args.classname,
                        'input':args.inputs,
                        'output':args.output}

                log.info(admin.function_create(parm))

            elif args.opp == 'delete':
                log.info(admin.function_delete(args.name)) 
            elif args.opp == 'list':
                log.info(admin.functions_list()) 
            else:
                log.error(f'Opp invalida: {args.opp}')

    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        
if __name__ == '__main__':
    main()