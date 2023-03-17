#!/usr/bin/env python3
'''
Created on 20220917
Update on 20230316
@author: Eduardo Pagotto
'''

import argparse
import json
import logging
from typing import List
import requests
import yaml
from pathlib import Path

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

    def namespaces_create(self, name : str) -> str:
        return self.__rpc().namespaces_create(name)

    def namespaces_delete(self, name : str) -> str:
        return self.__rpc().namespaces_delete(name)

    def namespaces_list(self) -> List[str]:
        return self.__rpc().namespaces_list()

    def queues_create(self, queue : str) -> str:
        return self.__rpc().queues_create(queue)

    def queues_delete(self, queue : str) -> str:
        return self.__rpc().queues_delete(queue)

    def queues_list(self, ns : str) -> List[str]:
        return self.__rpc().queues_list(ns)

    def function_create(self, params) -> str:
        return self.__rpc().function_create(params)

    def function_delete(self, name : str) -> str:
        return self.__rpc().function_delete(name)

    def function_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.__rpc().function_pause_resume(name, is_pause)

    def functions_list(self, ns : str) -> List[str]:
        return self.__rpc().functions_list(ns)

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

        namespaces = subparser.add_parser('namespaces')
        namespaces.add_argument('opp', type=str, help='Comando tipo (create|delete|list|pause|resume)')
        namespaces.add_argument('name', type=str, help='Nome do namespace')

        queue = subparser.add_parser('queues')
        queue.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        queue.add_argument('name', type=str, help='nome da queue', default='')

        funcions = subparser.add_parser('functions')
        funcions.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        funcions.add_argument('--name', type=str, help='nome da thread', required=False, default="")
        funcions.add_argument('--namespace', type=str, help='Namespace', required=False, default="")
        funcions.add_argument('--config', type=str, help='other config user', required=False, default="")
        funcions.add_argument('--configfile', type=str, help='other file config user', required=False, default="")
        funcions.add_argument('--py', type=str, help='python script pathfile')
        funcions.add_argument('--classname', type=str, help='Nome da classe')
        funcions.add_argument('--inputs', type=str, help='queue input', required=False, default="")
        funcions.add_argument('--output', type=str, help='queue output', required=False, default="")
        funcions.add_argument('--parallelism', type=int,  help='num of threads', required=False, default=1)
        funcions.add_argument('--timeout', type=float,  help='num of threads', required=False, default=5.0)

        args = parser.parse_args()

        if args.command == 'namespaces':
            if args.opp == 'create':
                log.info(admin.namespaces_create(args.name))
            elif args.opp == 'delete':
                log.info(admin.namespaces_delete(args.name))
            elif args.opp == 'list':
                log.info(admin.namespaces_list())
            else:
                pass

        elif args.command == 'queues':
            if args.opp == 'create':
                log.info(admin.queues_create(args.name))
            elif args.opp == 'delete':
                log.info(admin.queues_delete(args.name))
            elif args.opp == 'list':
                log.info(admin.queues_list(args.name))
            else:
                log.error(f'Opp invalida: {args.opp}')

        elif args.command == 'functions':
            if args.opp == 'create':

                val : dict = {}
                try:
                    if len(args.config) > 0:
                        # load cfg json string
                        val = json.loads(args.configfile)
                    elif len(args.configfile) > 0:
                        # load cfg yaml file
                        val = yaml.safe_load(Path(args.configfile).read_text())
                except FileNotFoundError as err1:
                    raise Exception(f'{err1.filename} fail: {err1.strerror}')
                except Exception as exp:
                    raise Exception(f'config or configfile is not a valid {str(exp.args[0])}')

                param = {'name': args.name, 
                         'namespace' : args.namespace,
                         'py':args.py,
                         'classname':args.classname,
                         'config': val,
                         'timeout': args.timeout,
                         'parallelism': args.parallelism}
                
                if len(args.inputs) > 0:
                    param['inputs'] = args.inputs.replace(' ','').split(',')

                if len(args.output) > 0:
                    param['output'] = args.output

                log.info(admin.function_create(param))

            elif args.opp == 'delete':
                log.info(admin.function_delete(args.namespace + '/' +args.name)) 
            elif args.opp == 'pause':
                log.info(admin.function_pause_resume(args.namespace + '/' + args.name, True)) 
            elif args.opp == 'resume':
                log.info(admin.function_pause_resume(args.namespace + '/' + args.name, False)) 
            elif args.opp == 'list':
                log.info(admin.functions_list(args.namespace)) 
            else:
                log.error(f'Opp invalida: {args.opp}')

        else:
            log.error(f'Comando invalido')

    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        
if __name__ == '__main__':
    main()