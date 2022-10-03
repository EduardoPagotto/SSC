#!/usr/bin/env python3
'''
Created on 20220917
Update on 20220927
@author: Eduardo Pagotto
'''


import argparse
import logging
import numbers
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
        parser_val = argparse.ArgumentParser(description='client command')

        parser_val.add_argument('comando', type=str, help='comando tipo (produce|consume)')
        parser_val.add_argument('queue_name', type=str, help='Queue to process')
        parser_val.add_argument('-m', '--message', type=str, help='message a enviar', required=False)
        parser_val.add_argument('-n', '--number', type=int, help='numero repeticoes', required=False, default=1)
        args = parser_val.parse_args()

        if args.comando == "produce":
            producer = client.create_producer(args.queue_name)
            for i in range(0, args.number):
                producer.send(args.message)

            #producer.close()

        elif args.comando == "consume":

            consume = client.subscribe(args.queue_name)
            c = 0
            while killer.kill_now is False:
                val = consume.receive()
                if val != None:
                    log.info(str(val))
                    c += 1
                    if c == args.number:
                        break

                    continue
                else:
                    time.sleep(5)
            
            #consume.close()            

        else:
            log.error('Comando invalido')

    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        
if __name__ == '__main__':
    main()