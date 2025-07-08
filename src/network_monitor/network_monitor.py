from threading import Timer
from pyee import EventEmitter
import logging

logger = logging.getLogger('middleware')

class NetworkMonitor(EventEmitter):
    '''
        Gerencia o estado da rede
    '''
    def __init__(self, host_info: dict, timeout: float):
        super().__init__()
        self.timeout = timeout
        self.host_id = host_info['id']
        self.host_info = host_info
        self.members = { host_info['id']: host_info }
        self.timer_table = {}

    def heartbeat(self, message):
        sender_id = message['id']
     
        if sender_id == self.host_id:
            return
        
        # logger.debug(f'NetworkMonitor: heartbeat recebido de {message['id']}')
        timer: Timer = self.timer_table.get(sender_id)
        
        # Reseta o timer
        if timer is not None:
            timer.cancel()

        self._update_member(message)
        new_timer = Timer(self.timeout, lambda: self._on_timeout(sender_id))
        self.timer_table[sender_id] = new_timer
        new_timer.start()
        
    def get_active_members(self):
        return [member_info for _, member_info in self.members.items() if member_info['status'] == 1]
    
    def get_host_info(self):
        return self.host_info
        
    def _update_member(self, message):
        id = message['id']
        received_address = (message['ip'], message['port'])
        member = self.members.get(id)
        if member is None:
            logger.info(f'NetworkMonitor: novo membro detectado: {id}')
            member = {
                'id': id,
                'address': received_address,
                'status': 1
            }
            self.members[id] = member
        
        if member['status'] == 0:
            member['status'] = 1
        
        if member['address'] != received_address:
            member['address'] = received_address
        
    def _on_timeout(self, id):
        logger.info(f'NetworkMonitor: falha detectada no membro {id}')
        member = self.members[id]
        member['status'] = 0
        self.emit('member_failed', id)