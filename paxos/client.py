import random
import socket
import threading
import time

from paxos.connection import Messenger
from paxos.message import *
from paxos.tools import IncrBy

incr = IncrBy()


def start(messenger, port):
    i = 0
    while i < 3:
        messenger.send_to((socket.gethostname(), port), Message(MSG_START, '001', proposal_value=incr.incrBy()))
        time.sleep(random.Random().randint(1, 3))
        i+=1


if __name__ == "__main__":
    config = {
        "host": socket.gethostname(),
        "port": 9000,
        "uid": "001"
    }

    messenger = Messenger(config)
    threading.Thread(target=start, args=(messenger, 10000)).start()
    threading.Thread(target=start, args=(messenger, 10001)).start()
    threading.Thread(target=start, args=(messenger, 10002)).start()
    time.sleep(1000)


