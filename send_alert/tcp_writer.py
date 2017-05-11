import logging
import socket
import json


class Tcp(object):
    def __init__(self, ip_address='', port=8080):
        self.ip_address = ip_address
        self.port = port

    def send_message(self, massage):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.ip_address, self.port))
            sock.send(json.dumps(massage))
            sock.close()

        except Exception, e:
            logging.error("TCP Sock Error " + e.__str__())
