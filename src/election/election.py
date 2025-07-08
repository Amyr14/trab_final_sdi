from src.p2p_channel import P2PChannel
from pyee import EventEmitter
from threading import Timer
import requests
import logging
import json

logger = logging.getLogger('middleware')

class ElectionManager(EventEmitter):
    def __init__(self, leader_message_timeout):
        super().__init__()
        self.leader_id = None
        self.election = False
        self.leader_message_timeout = leader_message_timeout
        self.leader_timer: Timer = None
        
        # Silenciando os logs do servidor
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)

    def start_election(self, host_info, active_members, p2p_channel: P2PChannel):
        if self.election:
            return
    
        logger.info('ElectionManager: iniciando eleição...')
        self.election = True
        ip, port = host_info['address']
        message = {
            'id': host_info['id'],
            'type': 'election',
            'ip': ip,
            'port': port
        }

        higher_priority_active_members = [member_info for member_info in active_members if member_info['id'] > host_info['id']]
        hadResponse = False

        for member in higher_priority_active_members:
            toAddress = member['address']
            try:
                p2p_channel.send(json.dumps(message), toAddress)
                hadResponse = True
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f'ElectionManager: comunicação com o membro {member['id']} falhou')
        
        if not hadResponse:
            for member in active_members:
                if member['id'] == host_info['id']:
                    continue
                toAddress = member['address']
                p2p_channel.send(json.dumps({**message, 'type': 'leader'}), toAddress)
            self.new_leader(host_info['id'])
        else:
            self.leader_timer = Timer(self.leader_message_timeout, lambda: self._leader_message_timeout)
            self.leader_timer.start()

    def _leader_message_timeout(self):
        if not self.election:
            logger.debug("ElectionManager: ignorando timeout — lider já foi eleito.")
        logger.warning("ElectionManager: timeout — mensagem de líder não recebida, reiniciando eleição.")
        self.emit('leader_message_timeout')

 
    def new_leader(self, leader_id):
        if self.leader_timer:
            self.leader_timer.cancel()
            self.leader_timer = None

        logger.info(f'ElectionManager: {leader_id} é o novo líder')
        self.leader_id = leader_id
        self.election = False
            
    def is_leader(self, id):
        return self.leader_id == id
        
    def get_leader(self):
        return self.leader_id