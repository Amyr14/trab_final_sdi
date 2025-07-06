from threading import Thread, Timer
from src.multicast import MulticastChannel
from pyee import EventEmitter

class HeartbeatBeacon():
    '''
        Gerencia o envio de heartbeats

        Args:
            freq (float): intervalo de envio do heartbeat em segundos
            message (bytes): mensagem que deve ser enviada no payload do heartbeat
    '''
    def __init__(self, freq: float, message: bytes):
        self.is_running = False
        self.freq = freq
        self.message = message
        self.sender_timer = None
        self.multicast_channel = None

    def start(self, multicast_channel: MulticastChannel):
        '''
            Começa a enviar heartbeats no canal multicast fornecido
        '''
        self.multicast_channel = multicast_channel
        self.is_running = True
        self._send()
        
    def stop(self):
        '''
            Para de enviar heartbeats
        '''
        self.is_running = False
        
    def _send(self):
        if self.is_running:
            self.multicast_channel.send(self.message)
            self.sender_timer = Timer(self.freq, self._send)
            self.sender_timer.start()

class HeartbeatMonitor(EventEmitter):
    '''
        Gerencia o recebimento de heartbeats

        Args:
            timeout (float): timeout em segundos
            id_parser (function(bytes) -> hashable): callback que recebe a mensagem recebida e retorna o identificador do processo remetente
        Events:
            heartbeat(message: bytes)
                - `message`: a mensagem recebida no payload do heartbeat
            timeout(id: hashable):
                - `id`: o id do membro que sofreu timeout
    '''
    def __init__(self, timeout: float, id_parser: function):
        self.timeout = timeout
        self.id_parser = id_parser
        self.timer_table = {}
        self.multicast_channel = None
        self.running = False

    def start(self, multicast_channel: MulticastChannel):
        '''
            Começa a monitorar heartbeats no canal multicast fornecido
        '''
        self.multicast_channel = multicast_channel
        self.running = True

        def receive_loop():
            while self.running:
                recv_message = self.multicast_channel.receive()
                id = self.id_parser(recv_message)
                timer: Timer = self.timer_table.get(id)
                
                if timer is not None:
                    timer.cancel()
                
                self.timer_table[id] = Timer(self.timeout, lambda: self.emit('timeout', id))
                self.emit('heartbeat', recv_message)
        
        self.receiver_thread = Thread(target=receive_loop)
        self.receiver_thread.start()
        
    def stop(self):
        '''
            Para de receber heartbeats
        '''
        self.running = False