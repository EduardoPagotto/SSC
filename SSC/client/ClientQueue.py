#!/usr/bin/env python3
'''
Created on 20220917
Update on 20230313
@author: Eduardo Pagotto
'''

import json
import requests

from sJsonRpc.ConnectionControl import ConnectionControl
from sJsonRpc.ProxyObject import ProxyObject

from .Producer import Producer
from .Subscribe import Subscribe

class ConnectionRestApiQueue(ConnectionControl):
    def __init__(self, addr : str):
        super().__init__(addr)

    def exec(self, input_rpc : dict, *args, **kargs) -> dict:
        url : str
        headers : dict= {'rpc-Json': json.dumps(input_rpc)}
        payload : dict ={}

        # comandos rpc's
        url = self.getUrl() + "/client-queue"
        files = None
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        if response.status_code != 201:
            raise Exception(response.text)

        return json.loads(response.text) # dict do rpcjson

class ClientQueue(object):
    def __init__(self, s_address: str):
        self.restAPI = ConnectionRestApiQueue(s_address)

    def __rpc(self):
        """Internal method to call server

        Returns:
            _type_: Connection controller
        """

        return ProxyObject(self.restAPI)

    def create_producer(self, queue_name_full : str) -> Producer:
        self.__rpc().create_producer(queue_name_full)
        return Producer(self.restAPI.getUrl(), queue_name_full)

    def subscribe(self, queue_name_full : str) -> Subscribe:
        self.__rpc().create_subscribe(queue_name_full)
        return Subscribe(self.restAPI.getUrl(), queue_name_full)

    def close(self):
        self.__rpc().close()
