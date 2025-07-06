import socket
import struct

def setup_multicast_socket(ip_multicast: str, port: int):
    mult_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    mult_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    mult_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    mult_sock.bind(('', port))
    mult_config = struct.pack('4sl', socket.inet_aton(ip_multicast), socket.INADDR_ANY)
    mult_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mult_config)
    return mult_sock

class MulticastChannel:
    '''
        Interface que abstrai a comunicação multicast
    '''
    def __init__(self, ip: str, port: int, buffer_size=1024):
        '''
            Args:
                ip (str): IP Multicast
                port (int): Porta multicast
        '''
        self.address = (ip, port)
        self.socket = setup_multicast_socket(ip, port)
        self.buffer_size = buffer_size
    
    def send(self, message):
        try:
            self.socket.sendto(message, self.address)
        except Exception as e:
            print(f'[-] Erro ao enviar mensagem: {e}')
    
    def receive(self) -> str:
        try:
            data, _ = self.socket.recvfrom(self.buffer_size)
            return data
        except Exception as e:
            print(f'[-] Erro ao receber mensagem: {e}')
