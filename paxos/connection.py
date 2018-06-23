import socket
import threading
import pickle
import logging

class Messenger:
    receive_callback = None
    server_socket = None
    config = None
    addr = None
    running = True

    def __init__(self, config):
        self.server_socket = socket.socket()
        self.config = config
        self.receive_callback = self.receive
        self.addr = (self.config['host'], self.config['port'])

    def listen(self):
        self.server_socket.bind(self.addr)
        self.server_socket.listen(5)
        threading.Thread(target=self.accept).start()

    def accept(self):
        logging.info('start listen : %s' % str(self.addr))
        while self.running:
            client, addr = self.server_socket.accept()
            logging.debug('%s connect from %s' % (str(self.config), str(addr)))
            threading.Thread(target=self.do_accept, args=(client, addr)).start()


    def do_accept(self, client, addr):
        data = client.recv(2048)
        if data:
            msg = pickle.loads(data)
            logging.debug('[%s]接收消息:[%s],来源地址:%s' % (self.config['uid'], str(msg), addr))
            self.receive_callback(msg)
        client.close()


    def send_to(self, addr, msg):
        logging.debug('[%s]发送消息:[%s],接收地址:%s' % (self.config['uid'], str(msg), str(addr)))
        try:
            client_socket = socket.socket()
            client_socket.connect(addr)
            data = pickle.dumps(msg)
            client_socket.send(data)
        except Exception as e:
            logging.error(e)

    def register_callback(self, callback):
        self.receive_callback = callback

    def receive(self, msg):
        logging.debug(msg)
