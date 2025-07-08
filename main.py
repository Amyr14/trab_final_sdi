from src.coordinator import Coordinator
from src.network_monitor import NetworkMonitor
from src.heartbeat import Heartbeat
from src.multicast import MulticastChannel
from src.p2p_channel import P2PChannel
from src.election import ElectionManager
import os
from dotenv import load_dotenv
import uuid 
import socket
import json
import logging

def find_available_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

logging_level_table = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'error': logging.ERROR
}

if __name__ == '__main__':
    load_dotenv()

    # Get general network configuration from .env
    logging_level = os.getenv('LOGGING_LEVEL')
    multicast_ip = os.getenv('MULTICAST_IP')
    multicast_port = int(os.getenv('MULTICAST_PORT'))
    heartbeat_freq = float(os.getenv('HEARTBEAT_FREQ'))
    network_monitor_timeout = float(os.getenv('NETWORK_MONITOR_TIMEOUT'))
    p2p_message_timeout = float(os.getenv('P2P_MESSAGE_TIMEOUT'))
    election_leader_message_timeout = float(os.getenv('ELECTION_LEADER_MESSAGE_TIMEOUT'))
    startup_grace_period = float(os.getenv('STARTUP_GRACE_PERIOD'))

    # Configurando logger
    logger = logging.getLogger("middleware")
    logger.setLevel(logging_level_table[logging_level])
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    host_id = str(uuid.uuid4())
    host_port = find_available_port()
    host_info = {
        'id': host_id,
        'address': ('127.0.0.1', host_port),
        'status': 1
    }

    # Initialize components
    multicast_channel = MulticastChannel(multicast_ip, multicast_port)
    p2p_channel = P2PChannel(host_info['address'], p2p_message_timeout)
    
    heartbeat_message = json.dumps({
        'id': host_id,
        'ip': '127.0.0.1',
        'port': host_port,
        'type': 'heartbeat',
    })
    heartbeat = Heartbeat(heartbeat_freq, heartbeat_message)
    network_monitor = NetworkMonitor(host_info, network_monitor_timeout)
    election_manager = ElectionManager(election_leader_message_timeout)
    
    coordinator = Coordinator(
        startup_grace_period,
        p2p_channel,
        multicast_channel,
        heartbeat,
        network_monitor,
        election_manager
    )

    print(f"Node {host_id} (Port: {host_port}) starting...")
    coordinator.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print(f"Node {host_id} (Port: {host_port}) stopping...")
        coordinator.stop()
        print(f"Node {host_id} (Port: {host_port}) stopped.")