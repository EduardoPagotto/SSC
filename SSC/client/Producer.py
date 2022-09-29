

import json
import pathlib
import requests

from sJsonRpc.ProxyObject import ProxyObject
from sJsonRpc.ProxyObject import ConnectionControl

class ConnectionRestApiProducer(ConnectionControl):
    def __init__(self, addr : str):
        super().__init__(addr)

    def exec(self, input_rpc : dict, *args, **kargs) -> dict:
        url : str
        headers : dict= {'rpc-Json': json.dumps(input_rpc)}
        payload : dict ={}

        # comandos rpc's
        url = self.getUrl() + "/client-producer"
        files = None
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        if response.status_code != 201:
            raise Exception(response.text)

        return json.loads(response.text) # dict do rpcjson


class Producer(object):
    def __init__(self, id : int, s_address : str, topic : str) -> None:
        self.restAPI = ConnectionRestApiProducer(s_address)
        self.topic = topic
        self.id = id

    def __rpc(self):
        return ProxyObject(self.restAPI)

    def send(self, data : str):
        self.__rpc().send_producer(self.id, data)

    def close(self):
        self.__rpc().close()


