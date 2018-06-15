from paxos.connection import Messenger
import socket

from paxos.message import *

if __name__ == "__main__":
    config = {
        "host": socket.gethostname(),
        "port": 9000,
        "uid": "001"
    }

    messenger = Messenger(config)
    messenger.send_to((socket.gethostname(), 10000), Message(MSG_START, '001'))
    messenger.send_to((socket.gethostname(), 10001), Message(MSG_START, '001'))
    messenger.send_to((socket.gethostname(), 10002), Message(MSG_START, '001'))
