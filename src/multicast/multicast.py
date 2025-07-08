import socket
import struct
import json
import logging
from pyee import EventEmitter
from threading import Thread

logger = logging.getLogger('middleware')

def setup_multicast_socket(ip_multicast: str, port: int):
    mult_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    mult_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    mult_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    mult_sock.bind(('', port))
    mult_config = struct.pack('4sl', socket.inet_aton(ip_multicast), socket.INADDR_ANY)
    mult_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mult_config)
    return mult_sock

class MulticastChannel(EventEmitter):
    '''
        Interface que abstrai a comunicação multicast
    '''
    def __init__(self, ip: str, port: int, buffer_size=1024):
        '''
            Args:
                ip (str): IP Multicast
                port (int): Porta multicast
        '''
        super().__init__()
        self.address = (ip, port)
        self.socket = setup_multicast_socket(ip, port)
        self.buffer_size = buffer_size
        self.running = False
        self.receiver_thread: Thread | None = None
    
    def start(self):
        logger.info(f'MulticastChannel: abrindo canal multicast com endereço {self.address[0]}:{self.address[1]}')
        self.running = True
        self.receiver_thread = Thread(target=self._receive_loop)
        self.receiver_thread.start()
    
    def stop(self):
        self.running = False
        self.socket.close()
    
    def send(self, message: str):
        try:
            self.socket.sendto(message.encode(), self.address)
        except Exception as e:
            logger.error(f'MulticastChannel: erro ao enviar mensagem multicast ({e})')
            
    def _receive_loop(self):
        while self.running:
            message = self._receive()
            self.emit('message', message)
    
    def _receive(self):
        try:
            data, _ = self.socket.recvfrom(self.buffer_size)
            return data.decode()
        except Exception as e:
            logger.error(f'MulticastChannel: erro ao receber mensagem multicast: {e}')