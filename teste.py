'''
Created on 20221103
Update on 20221103
@author: Eduardo Pagotto
'''


import dataclasses
import json
from SSC.Message import Message

if __name__ == '__main__':

    for i in range(10, 20):
        msg = Message(seq_id=i, payload="payload",topic="topic")
        print(msg)

        print(json.dumps(dataclasses.asdict(msg)))