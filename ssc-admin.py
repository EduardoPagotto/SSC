#!/usr/bin/env python3
'''
Created on 20220917
Update on 20221104
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

    def topics_create(self, topic : str) -> str:
        return self.__rpc().topics_create(topic)

    def topics_delete(self, topic : str) -> str:
        return self.__rpc().topics_delete(topic)

    def topics_list(self, ns : str) -> List[str]:
        return self.__rpc().topics_list(ns)

    def function_create(self, params) -> str:
        return self.__rpc().function_create(params)

    def function_delete(self, name : str) -> str:
        return self.__rpc().function_delete(name)

    def function_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.__rpc().function_pause_resume(name, is_pause)

    def functions_list(self, tenant_ns : str) -> List[str]:
        return self.__rpc().functions_list(tenant_ns)

    def source_create(self, params) -> str:
        return self.__rpc().source_create(params)

    def source_delete(self, name : str) -> str:
        return self.__rpc().source_delete(name)

    def source_pause_resume(self, name : str, is_pause : bool) -> str:
        return self.__rpc().source_pause_resume(name, is_pause)

    def source_list(self, tenant_ns : str) -> List[str]:
        return self.__rpc().source_list(tenant_ns)

    def tenants_create(self, name : str) -> str:
        return self.__rpc().tenants_create(name)

    def tenants_delete(self, name : str) -> str:
        return self.__rpc().tenants_delete(name)

    def tenants_list(self) -> List[str]:
        return self.__rpc().tenants_list()

    def namespaces_create(self, name : str) -> str:
        return self.__rpc().namespaces_create(name)

    def namespaces_delete(self, name : str) -> str:
        return self.__rpc().namespaces_delete(name)

    def namespaces_list(self, name : str) -> List[str]:
        return self.__rpc().namespaces_list(name)


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
        funcions.add_argument('--tenant', type=str, help='Tenant', required=True)
        funcions.add_argument('--namespace', type=str, help='Namespace', required=True)
        funcions.add_argument('--name', type=str, help='nome da thread', required=True)
        funcions.add_argument('--py', type=str, help='python script pathfile')
        funcions.add_argument('--classname', type=str, help='Nome da classe')
        funcions.add_argument('--inputs', type=str, help='queue input')
        funcions.add_argument('--output', type=str, help='queue output')
        funcions.add_argument('--userconfig', type=str, help='other config user', required=False, default="")
        funcions.add_argument('--userconfigfile', type=str, help='other file config user', required=False, default="")
        funcions.add_argument('--parallelism', type=int,  help='num of threads', required=False, default=1)

        # only connector
        sources = subparser.add_parser('sources')
        sources.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        sources.add_argument('--name', type=str, help='nome da thread', required=False)
        sources.add_argument('--tenant', type=str, help='Tenant', required=False)
        sources.add_argument('--namespace', type=str, help='Namespace', required=False)
        sources.add_argument('--sourceconfigfile', type=str, help='other config connectos', required=False, default="")
        sources.add_argument('--sourceconfig', type=str, help='other config connectos', required=False, default="")
        sources.add_argument('--archive', type=str, help='other config connectos', required=False, default="")
        sources.add_argument('--classname', type=str, help='Nome da classe')
        sources.add_argument('--destinationtopicname', type=str, help='other config connectos', required=False, default="")


        tenants = subparser.add_parser('tenants')
        tenants.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        tenants.add_argument('name', type=str, help='Nome do tenant')

        namespaces = subparser.add_parser('namespaces')
        namespaces.add_argument('opp', type=str, help='Comando tipo (create|delete|list)')
        namespaces.add_argument('name', type=str, help='Nome do namespace')

        args = parser.parse_args()

        if args.command == 'topics':
            if args.opp == 'create':
                log.info(admin.topics_create(args.queue))
            elif args.opp == 'delete':
                log.info(admin.topics_delete(args.queue))
            elif args.opp == 'list':
                log.info(admin.topics_list(args.queue))
            else:
                log.error(f'Opp invalida: {args.opp}')
        elif args.command == 'functions':
            if args.opp == 'create':

                val : dict = {}
                try:
                    if len(args.userconfig) > 0:
                        # load cfg json string
                        val = json.loads(args.userconfig)
                    elif len(args.userconfigfile) > 0:
                        # load cfg yaml file
                        val = yaml.safe_load(Path(args.userconfigfile).read_text())
                except FileNotFoundError as err1:
                    raise Exception(f'{err1.filename} fail: {err1.strerror}')
                except Exception as exp:
                    raise Exception(f'userconfig or userconfigfile is not a valid {str(exp.args[0])}')

                param = {'name': args.name, 
                         'tenant': args.tenant,
                         'namespace' : args.namespace,
                         'py':args.py,
                         'classname':args.classname,
                         'inputs':args.inputs.replace(' ','').split(','),
                         'output':args.output,
                         'useConfig': val,
                         'parallelism': args.parallelism}

                log.info(admin.function_create(param))

            elif args.opp == 'delete':
                log.info(admin.function_delete(args.tenant + '/' + args.namespace + '/' +args.name)) 
            elif args.opp == 'pause':
                log.info(admin.function_pause_resume(args.tenant + '/' + args.namespace + '/' + args.name, True)) 
            elif args.opp == 'resume':
                log.info(admin.function_pause_resume(args.tenant + '/' + args.namespace + '/' + args.name, False)) 
            elif args.opp == 'list':
                log.info(admin.functions_list(args.tenant + '/' + args.namespace)) 
            else:
                log.error(f'Opp invalida: {args.opp}')

        elif args.command == 'sources':

            if args.opp == 'create':

                val : dict = {}
                try:
                    if len(args.sourceconfig) > 0:
                        # load cfg json string
                        val = json.loads(args.sourceconfig)
                    elif len(args.sourceconfigfile) > 0:
                        # load cfg yaml file
                        val = yaml.safe_load(Path(args.sourceconfigfile).read_text())
                except FileNotFoundError as err1:
                    raise Exception(f'{err1.filename} fail: {err1.strerror}')
                except Exception as exp:
                    raise Exception(f'userconfig or sourceconfigfile is not a valid {str(exp.args[0])}')

                param = {'name': args.name, 
                         'tenant': args.tenant,
                         'namespace' : args.namespace,
                         'archive': args.archive,
                         'classname':args.classname,
                         'output' : args.destinationtopicname,
                         'config': val}

                log.info(admin.source_create(param))

            elif args.opp == 'delete':
                log.info(admin.source_delete(args.tenant + '/' + args.namespace + '/' +args.name)) 
            elif args.opp == 'pause':
                log.info(admin.source_pause_resume(args.tenant + '/' + args.namespace + '/' + args.name, True)) 
            elif args.opp == 'resume':
                log.info(admin.source_pause_resume(args.tenant + '/' + args.namespace + '/' + args.name, False)) 
            elif args.opp == 'list':
                log.info(admin.source_list(args.tenant + '/' + args.namespace)) 

        elif args.command == 'sinks':

            if args.opp == 'create':
                pass # TODO:
            elif args.opp == 'delete':
                pass # TODO:
            elif args.opp == 'pause':
                pass # TODO:
            elif args.opp == 'resume':
                pass # TODO:
            elif args.opp == 'list':
                pass # TODO:

        elif args.command == 'tenants':
            if args.opp == 'create':
                log.info(admin.tenants_create(args.name))
            elif args.opp == 'delete':
                log.info(admin.tenants_delete(args.name))
            elif args.opp == 'list':
                log.info(admin.tenants_list())
            else:
                pass

        elif args.command == 'namespaces':
            if args.opp == 'create':
                log.info(admin.namespaces_create(args.name))
            elif args.opp == 'delete':
                log.info(admin.namespaces_delete(args.name))
            elif args.opp == 'list':
                log.info(admin.namespaces_list(args.name))
            else:
                pass
        else:
            log.error(f'Comando invalido')

    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        
if __name__ == '__main__':
    main()