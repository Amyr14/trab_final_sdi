from threading import Timer
from src.multicast import MulticastChannel
from pyee import EventEmitter
import os
import logging

logger = logging.getLogger('middleware')

class Heartbeat():
    '''
        Gerencia o envio de heartbeats

        Args:
            freq (float): intervalo de envio do heartbeat em segundos
            message (str): mensagem que deve ser enviada no payload do heartbeat
    '''
    def __init__(self, freq: float, message: str):
        self.is_running = False
        self.freq = freq
        self.message = message
        self.sender_timer = None
        self.multicast_channel = None
        
    def set_multicast_channel(self, channel: MulticastChannel):
        self.multicast_channel = channel

    def start(self):
        '''
            Começa a enviar heartbeats no canal multicast fornecido
        '''
        if not self.multicast_channel:
            logger.error('Heartbeat: canal multicast não setado')
            return
        
        logger.info('Heartbeat: iniciando...')
        self.is_running = True
        self._send()
        
    def stop(self):
        '''
            Para de enviar heartbeats
        '''
        self.is_running = False
        
    def _send(self):
        if self.is_running:
            # logger.debug('Heartbeat: heartbeat enviado')
            self.multicast_channel.send(self.message)
            self.sender_timer = Timer(self.freq, self._send)
            self.sender_timer.start()