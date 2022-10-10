'''
Created on 20220924
Update on 20221010
@author: Eduardo Pagotto
'''

import logging
import time

from typing import Any, Optional
from threading import  Thread
from typing import Any, List

from  sJsonRpc.RPC_Responser import RPC_Responser

from SSC.server.Tenant import Tenant
from SSC.server.FunctionCrt import FunctionCrt
from SSC.server.TopicCrt import TopicsCrt

from SSC.__init__ import __version__ as VERSION
from SSC.__init__ import __date_deploy__ as DEPLOY

class DRegistry(RPC_Responser):
    def __init__(self, topic_crt : TopicsCrt, function_crt : FunctionCrt, tenant : Tenant) -> None:
        super().__init__(self)

        self.topic_crt = topic_crt
        self.function_crt = function_crt
        self.tenant = tenant
        self.done : bool = False
        self.ticktack = 0
        self.log = logging.getLogger('SSC.DRegistry')

        self.t_cleanner : Thread = Thread(target=self.cleanner, name='cleanner_files')
        self.t_cleanner.start()

    def sumario(self) -> str:

        lista = self.topic_crt.summario()
        res = ''
        for i in lista:
            res += str(i) + '<p>'

        return f'>>>>>> SSC v-{VERSION} ({DEPLOY})<p> Topics: <p> {res}'


    def cleanner(self) ->None:
        """[Garbage collector of files]
        """

        time.sleep(10)
        self.log.info('thread cleanner_files start')

        while self.done is False:

            procs, errors = self.function_crt.execute()

            self.log.debug(f'Tick-Tack {self.ticktack} [{procs}  / {errors}]... ')
            self.ticktack += 1
            time.sleep(5)

        self.function_crt.stop_func_all()
        self.log.info('thread cleanner_files stop')

    # ClientQueue
    def create_producer(self, topic_name : str) -> int:
        return self.topic_crt.find_and_load(topic_name).id
        
    # ClientQueue
    def subscribe(self, topic_name) -> int:
        return self.topic_crt.find_and_load(topic_name).id

    # Producer
    def send_producer(self, id : int, msg : str):
        self.topic_crt.push_id(id, msg)

    # Subscribe
    def subscribe_receive(self, id: int, timeOut: int) -> Optional[Any]:
        return self.topic_crt.pop_id(id, timeOut)

    # Admin
    def topics_create(self, topic_name : str) -> str:

        doc = self.tenant.find_tenant_params(topic_name)

        topic  = self.topic_crt.create(topic_name)
        return f'success create {topic_name} id {topic.id}'

    # Admin
    def topics_delete(self, topic_name : str) -> str:

        doc = self.tenant.find_tenant_params(topic_name)

        self.topic_crt.delete(topic_name)
        return f'success delete {topic_name}'

    # Admin
    def topics_list(self, ns : str) -> List[str]:
        return self.topic_crt.list_all(ns)

    # Admin
    def function_create(self, params: dict) -> str:

        topic_name = params['tenant'] + '/' + params['namespace']
        doc = self.tenant.find_tenant_params(topic_name)

        return self.function_crt.create(params)
        
    # Admin
    def function_delete(self, name: str):
        self.function_crt.delete(name)
        return f'success delete {name}'
    
    # Admin
    def functions_list(self, tenant_ns : str) -> List[str]:
        return self.function_crt.list_all(tenant_ns)

    # Admin
    def tenants_create(self, name : str) -> str:
        return self.tenant.create(name)

    # Admin
    def tenants_delete(self, name : str) -> str:
        return self.tenant.delete(name)

    # Admin
    def tenants_list(self) -> List[str]:
        return self.tenant.list_all()

    # Admin
    def namespaces_create(self, name : str) -> str:
        return self.tenant.create_namespace(name)

    # Admin
    def namespaces_delete(self, name : str) -> str:
        return self.tenant.delete_namespace(name)

    # Admin
    def namespaces_list(self, name : str) -> List[str]:
        return self.tenant.list_all_namespace(name)

        #--user-config '{"FileCfg":"aaaaa"}'
        #--user-config-file "/pulsar/host/etc/func1.yaml"

        # /pulsar/bin/pulsar-admin functions create \
        #   --name ConvertTxt2Dic \
        #   --py /var/app/src/ConvertTxt2Dic.py \
        #   --classname ConvertTxt2Dic.ConvertTxt2Dic \
        #   --inputs "persistent://rpa/manifesto/q01DecodeTxt"  \
        #   --output "persistent://rpa/manifesto/q99Erro" \
        #   --parallelism 1 