import socket
import threading
import pickle


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
        print('start listen : %s' % str(self.addr))
        while self.running:
            client, addr = self.server_socket.accept()
            print('%s connect from %s' % (str(self.config), str(addr)))
            data = client.recv(2048)
            if data:
                msg = pickle.loads(data)
                print('[%s]接收消息:[%s],来源地址:%s' % (self.config['uid'], str(msg), addr))
                self.receive_callback(msg)
            client.close()

    def send_to(self, addr, msg):
        print('[%s]发送消息:[%s],接收地址:%s' % (self.config['uid'], str(msg), str(addr)))
        client_socket = socket.socket()
        client_socket.connect(addr)
        data = pickle.dumps(msg)
        client_socket.send(data)

    def register_callback(self, callback):
        self.receive_callback = callback

    def receive(self, msg):
        print(msg)
