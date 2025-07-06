from src.heartbeat import HeartbeatBeacon, HeartbeatMonitor
from src.multicast import MulticastChannel
from src.p2p_channel import P2PChannel
from pyee import EventEmitter

"""
    1 - Escrever um canal de mensagens ponto-a-ponto (id:mensagem -> socket TCP, id:mensagem <- socket TCP)
    3 - O nó precisa ignorar as mensagens dele mesmo? (provavelmente)
"""

class NetworkNode(EventEmitter):
    '''
        Representa um nó da rede
        
        Args:
            id (hashable): um identificador único para o nó
            tcp_addr (tuple(str, int)): um par ip-porta para a comunicação ponto-a-ponto
            message_parser: (function(bytes)): uma função que recebe uma mensagem em bytes e retorna um dicionário do tipo
            { 
                'id': hashable,
                'type': 'election' | 'coordinator' | 'heartbeat',
                'tcp_addr': tuple(str, int),
                'status': 1 | 0,
            } 
    '''
    def __init__(self, id, tcp_addr, message_parser: function, p2p_channel: P2PChannel, multicast_channel: MulticastChannel, heartbeat_monitor: HeartbeatMonitor, heartbeat_beacon: HeartbeatBeacon):
        self.members = {
            id: { 'tcp_addr': tcp_addr, 'status': 1 }
        }
        self.heartbeat_monitor = heartbeat_monitor
        self.heartbeat_beacon = heartbeat_beacon
        self.message_parser = message_parser
        self.id = id
        self.leader_id = None
    
        heartbeat_monitor.on('timeout', self._on_member_timeout)
        heartbeat_monitor.on('heartbeat', self._on_heartbeat)
        
        heartbeat_monitor.start(multicast_channel)
        heartbeat_beacon.start(multicast_channel)

    def _on_member_timeout(self, id):
        member = self.members[id]
        member['status'] = 0
        
    def _on_heartbeat(self, message: bytes):
        parsed_msg: dict = self.message_parser(message)
        id = parsed_msg['id']
        received_tcp_addr = (parsed_msg['ip_tcp'], parsed_msg['port_tcp'])

        member: dict = self.members.setdefault(id, {
            'id': id,
            'tcp_addr': received_tcp_addr,
            'status': 1
        })
        
        if member['status'] == 0:
            member['status'] = 1
            # reabrir conexão
        
        if member['tcp_addr'] != received_tcp_addr:
            member['tcp_addr'] = received_tcp_addr
            # reabrir conexão
            
        print(f'Heartbeat do processo {id}')