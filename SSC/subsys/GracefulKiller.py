#!/usr/bin/env python3
'''
Created on 20180208
Update on 20190917
@author: Eduardo Pagotto
'''

import signal
from .Singleton import Singleton

class GracefulKiller(metaclass=Singleton):
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.kill_now = False

    def force_hand(self):
        self.kill_now = True

    def exit_gracefully(self,signum, frame):
        self.kill_now = True

# if __name__ == '__main__':
    
#     k1 = GracefulKiller()
#     k2 = GracefulKiller()

#     print(k1.kill_now)
#     print(k2.kill_now)

#     k1.force_hand()

#     print(k1.kill_now)
#     print(k2.kill_now)

#       killer = GracefulKiller()
#   while True:
#     time.sleep(1)
#     print("doing something in a loop ...")
#     if killer.kill_now:
#       break

#   print "End of the program. I was killed gracefully :)"


# class GracefulKiller:
#   kill_now = False
#   def __init__(self):
#     signal.signal(signal.SIGINT, self.exit_gracefully)
#     signal.signal(signal.SIGTERM, self.exit_gracefully)

#   def exit_gracefully(self,signum, frame):
#     self.kill_now = True

# if __name__ == '__main__':
#   killer = GracefulKiller()
#   while True:
#     time.sleep(1)
#     print("doing something in a loop ...")
#     if killer.kill_now:
#       break

#   print "End of the program. I was killed gracefully :)"