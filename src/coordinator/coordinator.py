from src.heartbeat import Heartbeat
from src.network_monitor import NetworkMonitor
from src.election import ElectionManager
from src.multicast import MulticastChannel
from src.p2p_channel import P2PChannel
from threading import Timer
import json
import logging

logger = logging.getLogger('middleware')

class Coordinator:
    def __init__(self, startup_grace_period: float, p2p_channel: P2PChannel, multicast_channel: MulticastChannel, heartbeat: Heartbeat, network_monitor: NetworkMonitor, election_manager: ElectionManager):
        self.multicast_channel = multicast_channel
        self.p2p_channel = p2p_channel
        self.network_monitor = network_monitor
        self.election_manager = election_manager
        self.heartbeat = heartbeat
        self.startup_grace_period = startup_grace_period
        self.running = False

    def start(self):
        self.running = True
        self.network_monitor.on('member_failed', self._on_member_failed)
        self.election_manager.on('leader_message_timeout', self._start_election)
        self.multicast_channel.on('message', self._on_multicast_message)
        self.p2p_channel.on('message', self._on_p2p_message)

        self.multicast_channel.start()
        self.p2p_channel.start()
        self.heartbeat.set_multicast_channel(self.multicast_channel)
        self.heartbeat.start()

        if self.startup_grace_period > 0:
            Timer(self.startup_grace_period, self._start_election).start()
        
        else:
            self._start_election()
    
    # Fazer um método de encerramento
    
    def _on_p2p_message(self, message):
        parsed_msg = json.loads(message)
        sender_id = parsed_msg['id']
        message_type = parsed_msg['type']
        
        if message_type == 'election':
            self._start_election()
            
        if message_type == 'leader':
            self.election_manager.new_leader(sender_id)
    
    def _on_multicast_message(self, message):
        parsed_msg = json.loads(message)
        if parsed_msg['type'] == 'heartbeat':
            self.network_monitor.heartbeat(parsed_msg)
        
    def _start_election(self):
        active_members = self.network_monitor.get_active_members()
        host_info = self.network_monitor.get_host_info()
        self.election_manager.start_election(host_info, active_members, self.p2p_channel)

    def _on_member_failed(self, id):
        if self.election_manager.is_leader(id):
            self._start_election()