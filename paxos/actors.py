from collections import namedtuple

from paxos.connection import Messenger
from paxos.message import *
from paxos.tools import ProposalID
from random import Random
import socket
import threading
import pickle


# MSG_PREPARE = namedtuple('prepare', ['uid', 'proposal_id'])
# MSG_ACCEPT = namedtuple('accept', ['uid', 'proposal_id', 'proposal_value'])
# MSG_PROMISE = namedtuple('promise', ['uid', 'proposal_id', 'accepted_id', 'accepted_value'])
# MSG_ACCEPTED = namedtuple('accepted', ['uid', 'proposal_id', 'accepted_id', 'accepted_value'])


class Registry:
    nodes = None
    acceptor = 0

    def __init__(self):
        self.nodes = []

    def register(self, node):
        self.nodes.append(node)
        if type(node.role) == Acceptor:
            self.acceptor += 1
        print(self.acceptor)


class Node:
    uid = None
    role = None
    registry = None
    config = {'host': socket.gethostname(), 'port': Random().randint(10000, 15000)}
    messenger = None

    def __init__(self, registry, config=None):
        self.uid = Random().randint(0, 1000)
        self.registry = registry
        if config:
            self.config = config
        self.config['uid'] = self.uid
        self.messenger = Messenger(self.config)

    def register(self, role):
        self.role = role
        self.role.set_node(self)
        self.registry.register(self)
        self.messenger.register_callback(self.role.receive)

    def broadcast(self, msg):
        for node in self.registry.nodes:
            self.messenger.send_to((node.config['host'], node.config['port']), msg)

    def send_to(self, uid, msg):
        self.messenger.send_to(self.find_addr(uid), msg)

    def start(self):
        self.messenger.listen()

    def find_addr(self, uid):
        for node in self.registry.nodes:
            if node.uid == uid:
                return node.config['host'], node.config['port']


class Role:
    node = None

    def set_node(self, node):
        self.node = node

    def receive(self, msg):
        pass


class Proposer(Role):
    messenger = None
    proposal_id = -1
    proposal_value = None
    last_accepted_id = -1
    promise_set = None
    half = 99
    status = 0

    def __init__(self):
        pass

    def send_prepare(self):
        self.promise_set = set()
        self.proposal_id = ProposalID.incr_proposal_id()
        # msg = MSG_PREPARE(self.node.uid, self.proposal_id)
        msg = Message(MSG_PREPARE, self.node.uid, proposal_id=self.proposal_id)
        self.node.broadcast(msg)
        self.status = 1

    def send_accept(self):
        # msg = MSG_ACCEPT(self.node.uid, self.proposal_id, self.proposal_value)
        msg = Message(MSG_ACCEPT, self.node.uid, proposal_id=self.proposal_id,
                      proposal_value=self.proposal_value)
        self.node.broadcast(msg)
        self.status = 2

    def receive_promise(self, msg):
        if msg.proposal_id != self.proposal_id or msg.uid in self.promise_set:
            return
        self.promise_set.add(msg.uid)
        if msg.accepted_id > self.last_accepted_id:
            self.last_accepted_id = msg.accepted_id
            if msg.accepted_value is not None:
                self.proposal_value = msg.accepted_value

        if self.status == 1 and self.over_half():
            print("[%s]prepare超过半数，开始发送accept" % self.node.uid)
            self.send_accept()


    def over_half(self):
        return len(self.promise_set) > self.node.registry.acceptor / 2


    def receive(self, msg):
        if msg.type == MSG_START:
            self.send_prepare()
        elif msg.type == MSG_PROMISE or msg.type == MSG_ACCEPTED:
            self.receive_promise(msg)


class Acceptor(Role):
    messenger = None
    promise_id = -1
    accepted_id = -1
    accepted_value = None

    def receive_prepare(self, from_uid, msg):
        if msg.proposal_id > self.promise_id:
            self.promise_id = msg.proposal_id
        # msg = MSG_PROMISE(self.node.uid, self.promise_id, self.accepted_id, self.accepted_value)
        msg = Message(MSG_PROMISE, self.node.uid, proposal_id=self.promise_id,
                      accepted_id=self.accepted_id, accepted_value=self.accepted_value)
        self.node.send_to(from_uid, msg)

    def receive_accept(self, from_uid, msg):
        if msg.proposal_id >= self.promise_id:
            self.promise_id = msg.proposal_id
            self.accepted_id = msg.proposal_id
            self.accepted_value = msg.proposal_value
            # msg = MSG_ACCEPTED(self.node.uid, msg.proposal_id, msg.proposal_value)
            msg = Message(MSG_ACCEPTED, self.node.uid, proposal_id=msg.proposal_id,
                          accepted_id=self.accepted_id, accepted_value=self.accepted_value)
            print("[%s]接收id:%s" % (self.node.uid, self.promise_id))
            self.node.send_to(from_uid, msg)

    def receive(self, msg):
        if msg.type == MSG_PREPARE:
            self.receive_prepare(msg.uid, msg)
        elif msg.type == MSG_ACCEPT:
            self.receive_accept(msg.uid, msg)


if __name__ == "__main__":
    host = socket.gethostname()
    port = 10000
    reg = Registry()
    for i in range(3):
        p = Node(reg, {'host': host, 'port': port})
        p.register(Proposer())
        port = port + 1

    for i in range(5):
        a = Node(reg, {'host': host, 'port': port})
        a.register(Acceptor())
        port = port + 1

    print(reg.nodes)
    for node in reg.nodes:
        node.start()
