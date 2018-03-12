import signal
import sys
import time


def sigterm_handler(signal, frame):
    print('SIGTERM caught, exiting program')
    sys.stdout.flush()
    

signal.signal(signal.SIGINT, sigterm_handler)
signal.signal(signal.SIGTERM, sigterm_handler)

while True:
    time.sleep(1)
    print('sleeping')
    sys.stdout.flush()




