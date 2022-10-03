#!/usr/bin/env python3
'''
Created on 20220917
Update on 20220927
@author: Eduardo Pagotto
'''


import argparse
import logging
import time

from SSC.client.ClientQueue import ClientQueue
from SSC.subsys.GracefulKiller import GracefulKiller

def main():

    killer = GracefulKiller()

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(message)s'
    )

    logging.getLogger('werkzeug').setLevel(logging.CRITICAL) 
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    log = logging.getLogger('ssc-client')

    client = ClientQueue('http://127.0.0.1:5151')

    try:
        parser_val = argparse.ArgumentParser(description='Admin command')
        zz_args = parser_val.parse_known_args()
        args = zz_args[1]

        if args[0] == 'produce':
            queue_name  = args[1] 
            # args[2] = '-m'
            msg = args[3]
            producer = client.create_producer(queue_name)
            producer.send(msg)
            producer.close()

        elif args[0] == 'consume':

            queue_name = args[1]

            consume = client.subscribe(queue_name)

            while killer.kill_now is False:
                val = consume.receive()
                if val != None:
                    log.info(str(val))
                    continue
                else:
                    time.sleep(5)
                
            consume.close()


    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        


if __name__ == '__main__':
    main()