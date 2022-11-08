#!/usr/bin/env python3
'''
Created on 20220917
Update on 20221108
@author: Eduardo Pagotto
'''

import json
from typing import List
import requests
from SSC.topic import Producer, Consumer
from SSC.topic.QueueProdCons import QueueProducer, QueueConsumer

from sJsonRpc.ConnectionControl import ConnectionControl
from sJsonRpc.ProxyObject import ProxyObject

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

    def create_producer(self, topic : str, producer : str = '') -> Producer:
        conn : dict = self.__rpc().create_producer(topic)
        return QueueProducer(conn['urlRedis'], conn['queue'], producer)

    def subscribe(self, topics : List[str]) -> Consumer:
        conn : dict = self.__rpc().create_subscribe(topics)
        return QueueConsumer(conn['urlRedis'], conn['queue'])

    def close(self):
        self.__rpc().close()
