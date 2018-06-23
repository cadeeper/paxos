import logging
import socket
import time
from random import Random

from paxos.connection import Messenger
from paxos.message import *
from paxos.tools import ProposalID


# MSG_PREPARE = namedtuple('prepare', ['uid', 'proposal_id'])
# MSG_ACCEPT = namedtuple('accept', ['uid', 'proposal_id', 'proposal_value'])
# MSG_PROMISE = namedtuple('promise', ['uid', 'proposal_id', 'accepted_id', 'accepted_value'])
# MSG_ACCEPTED = namedtuple('accepted', ['uid', 'proposal_id', 'accepted_id', 'accepted_value'])


class Registry:
    """
    注册中心，所有的节点在此注册
    """
    nodes = None  # 节点列表
    acceptor = 0  # acceptor数量

    def __init__(self):
        self.nodes = []

    def register(self, node):
        """
        注册节点
        :param node:    节点
        :return:
        """
        self.nodes.append(node)
        if type(node.role) == Acceptor:
            self.acceptor += 1
        logging.debug(self.acceptor)


class Node:
    """
    节点类
    """
    # 节点唯一id
    uid = None
    # 节点角色
    role = None
    # 注册中心
    registry = None
    # 配置
    config = {'host': socket.gethostname(), 'port': Random().randint(10000, 15000)}
    # 消息处理者
    messenger = None

    def __init__(self, registry, config=None):
        self.registry = registry
        if config:
            self.config = config
        self.uid = config['port']
        self.config['uid'] = self.uid
        self.messenger = Messenger(self.config)

    def bind(self, role):
        """
        绑定角色，并注册到注册中心
        :param role:    角色
        :return:
        """
        self.role = role
        self.role.set_node(self)
        self.registry.register(self)
        self.messenger.register_callback(self.role.receive)

    def broadcast(self, msg):
        """
        广播消息到所有节点
        :param msg:     数据
        :return:
        """
        for node in self.registry.nodes:
            self.messenger.send_to((node.config['host'], node.config['port']), msg)

    def send_to(self, uid, msg):
        """
        发送消息到指定节点
        :param uid:     节点uid
        :param msg:     数据
        :return:
        """
        self.messenger.send_to(self.find_addr(uid), msg)

    def start(self):
        """
        启动节点
        :return:
        """
        self.messenger.listen()

    def find_addr(self, uid):
        """
        根据节点uid查询地址
        :param uid: 节点uid
        :return:
        """
        for node in self.registry.nodes:
            if node.uid == uid:
                return node.config['host'], node.config['port']


class Role:
    """
    角色类
    """
    node = None

    def set_node(self, node):
        self.node = node

    def receive(self, msg):
        pass


class Proposer(Role):
    """
    Proposer，继承自角色类
    """
    messenger = None

    # 提案id
    proposal_id = -1

    # 提案值
    proposal_value = None

    # 最后一次接受的提案id
    last_accepted_id = -1

    # proposal阶段时响应promise的节点集合
    promise_set = None

    # proposal阶段被拒绝的节点集合
    reject_proposal_set = None

    # accept阶段响应accepted的节点集合
    accepted_set = None

    # accept阶段被拒绝的节点集合
    reject_accepted_set = None

    # 节点的半数
    half = 99

    # 当前状态， 1: prepare阶段，2: accept阶段， 3: 提案结束
    status = 0

    STATUS_START = 0

    STATUS_PREPARE = 1

    STATUS_ACCEPT = 2

    STATUS_FINISH = 3

    def __init__(self):
        pass

    def send_prepare(self):
        """
        P1.a 发起prepare请求：向所有acceptor发起第一阶段的prepare请求，并带上自己的proposalId
        :return:
        """
        if self.status == self.STATUS_FINISH:
            logging.debug("提议已经结束, 不再发起")
            return

        self.promise_set = set()
        self.reject_proposal_set = set()
        self.proposal_id = ProposalID.incr_proposal_id()
        msg = Message(MSG_PREPARE, self.node.uid, proposal_id=self.proposal_id)
        logging.info("[%s]发起提议:提议id[%s]" % (self.node.uid, self.proposal_id))
        # 状态更改为 prepare
        self.status = self.STATUS_PREPARE
        self.node.broadcast(msg)

    def send_accept(self):
        """
        P2.a 发起accept请求：证明prepare阶段已经获取到了超过半数acceptor的promise，
            向所有acceptor发起第二阶段的accept请求，并带上自己的proposalId和proposalValue
        :return:
        """
        self.reject_accepted_set = set()
        msg = Message(MSG_ACCEPT, self.node.uid, proposal_id=self.proposal_id,
                      proposal_value=self.proposal_value)
        # 状态更改为 accept
        self.status = self.STATUS_ACCEPT
        self.node.broadcast(msg)

    def receive_promise(self, msg):
        """
        P1.b 接收第一阶段prepare的响应，如acceptor的promise超过半数，则进入第二阶段，发起accept请求。
            否则重新发起第一阶段的prepare请求。
            如果acceptor已经accept其它proposalValue，则在promise中会带上已经accept的proposalValue: Vx，
            此时在发起和二阶段的accept请求时，需要将proposalValue修改为Vx，如果acceptor未accept任何proposalValue，
            则proposalValue可以使用自己的Vn
        :param msg:
        :return:
        """
        if msg.proposal_id is None:
            """
                proposal被拒绝超过半数，重新开始流程
            """
            self.reject_proposal_set.add(msg.uid)
            if self.over_half(self.reject_proposal_set):
                logging.info(
                    "[%s]发起提议[%s]被拒绝超过半数[%s]，重新发起" % (
                        self.node.uid, self.proposal_id, self.reject_proposal_set))
                self.send_prepare()
            return

        if msg.proposal_id != self.proposal_id or msg.uid in self.promise_set:
            """
                接收到的响应proposalId不等于当前proposalId，可能是之前的响应，忽略
            """
            return

        self.promise_set.add(msg.uid)
        if msg.accepted_id > self.last_accepted_id:
            """
                如果acceptor已经promise其它的proposalId，则取proposalId最高的proposalValue作为当前的proposalValue
            """
            self.last_accepted_id = msg.accepted_id
            if msg.accepted_value is not None:
                self.proposal_value = msg.accepted_value

        if self.status == self.STATUS_PREPARE and self.over_half(self.promise_set):
            """
                promise的acceptor超过半数，发起第二阶段的accept流程
            """
            self.status = self.STATUS_ACCEPT
            logging.info("[%s]发起的提议[%s][%s]超过半数，开始发送accept.%s" % (
                self.node.uid, self.proposal_id, self.proposal_value, str(self.promise_set)))
            # time.sleep(Random().randint(1, 3))
            self.accepted_set = set()
            self.send_accept()

    def receive_accepted(self, msg):
        """
        P2.b 接收accept响应，如超过半数的acceptor接受了proposalValue，则结束流程
        :param msg:
        :return:
        """
        if msg.proposal_id is None:
            """
                accept被拒绝超过了半数，重新开始流程
            """
            self.reject_accepted_set.add(msg.uid)
            if self.over_half(self.reject_accepted_set):
                logging.info("[%s]发起accepted[%s][%s]被拒绝超过半数[%s]，重新发起" % (
                    self.node.uid, self.proposal_id, self.proposal_value, self.reject_accepted_set))
                self.reject_accepted_set = set()
                self.send_prepare()
            return

        if msg.proposal_id != self.proposal_id or msg.uid in self.accepted_set:
            return

        logging.info("%s接收提议:%s, %s" % (self.node.uid, msg.accepted_id, msg.accepted_value))
        self.accepted_set.add(msg.uid)
        if self.over_half(self.accepted_set):
            logging.info("%s确定最终提议:%s, %s" % (self.node.uid, msg.accepted_id, msg.accepted_value))
            self.status = self.STATUS_FINISH

    def over_half(self, p_set):
        return len(p_set) > self.node.registry.acceptor / 2

    def receive(self, msg):

        if msg.type == MSG_START:
            self.proposal_value = msg.proposal_value
            logging.debug("收到发起请求, proposerValue:%s" % self.proposal_value)
            self.status = self.STATUS_START
            self.send_prepare()

        if self.status == self.STATUS_FINISH:
            logging.debug("提议已经结束, 不再发起")
            return

        elif msg.type == MSG_PROMISE:
            self.receive_promise(msg)

        elif msg.type == MSG_ACCEPTED:
            self.receive_accepted(msg)


class Acceptor(Role):
    """
        Acceptor
    """
    messenger = None
    promise_id = -1
    accepted_id = -1
    accepted_value = None

    def receive_prepare(self, from_uid, msg):
        """
        P1.b 接收prepare请求：
            1. 如proposerId大于已接收到的最大proposalId：
                1.1 如之前未接受任何proposalValue，则返回promise
                1.2 如之前已接受任何proposalValue，则返回promise并带上proposalValue
        :param from_uid:
        :param msg:
        :return:
        """
        if msg.proposal_id > self.promise_id:
            self.promise_id = msg.proposal_id
            msg = Message(MSG_PROMISE, self.node.uid, proposal_id=self.promise_id,
                          accepted_id=self.accepted_id, accepted_value=self.accepted_value)
            self.node.send_to(from_uid, msg)
        else:
            self.node.send_to(from_uid, Message(MSG_PROMISE, self.node.uid))

    def receive_accept(self, from_uid, msg):

        if msg.proposal_id >= self.promise_id:
            self.promise_id = msg.proposal_id
            self.accepted_id = msg.proposal_id
            self.accepted_value = msg.proposal_value
            msg = Message(MSG_ACCEPTED, self.node.uid, proposal_id=msg.proposal_id,
                          accepted_id=self.accepted_id, accepted_value=self.accepted_value)
            logging.info(
                "[%s]接受提议[%s],值为[%s]" % (
                    self.node.uid,
                    self.accepted_id, self.accepted_value))
            self.reset()

            self.node.send_to(from_uid, msg)
        else:
            self.node.send_to(from_uid, Message(MSG_ACCEPTED, self.node.uid))

    def reset(self):
        self.accepted_value = None

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
        p.bind(Proposer())
        port = port + 1

    for i in range(5):
        a = Node(reg, {'host': host, 'port': port})
        a.bind(Acceptor())
        port = port + 1

    for node in reg.nodes:
        node.start()
