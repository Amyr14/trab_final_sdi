from flask import Flask, request
from pyee import EventEmitter
from threading import Thread
import requests
import logging

logger = logging.getLogger('middleware')

class P2PChannel(EventEmitter):
    '''
        Abtrai a comunicação ponto-a-ponto.
        
        Args:
            host_address (tuple(str, int)): o par IP:porta que deve ser utilizado pelo servidor
            message_timeout (float): o timeout de mensagens em segundos
    '''
    def __init__(self, host_address, message_timeout):
        super().__init__()
        self.is_running = False
        self.address = host_address
        self.message_timeout = message_timeout
        
        self.server = Flask('P2PChannel')
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        self.__configure_routes()
        
    def start(self):
        '''
            Abre o canal de comunicação
        '''

        self.is_running = True
        host, port = self.address
        Thread(target=lambda: self.server.run(host=host, port=port)).start()
        logger.info(f'P2PChannel: servidor HTTP ligado no host {host}:{port}')
    
    def stop(self):
        self.is_running
        
    def send(self, message, toAddress):
        if not self.is_running:
            logger.error('P2PChannel: tentativa de comunicação P2P com canal fechado')
            return

        ip, port = toAddress
        url = f'http://{ip}:{port}/message'
        
        logger.debug(f'P2PChannel: enviando {message} para {url}')
        return requests.post(
            url=url,
            json=message,
            timeout=self.message_timeout
        )
        
    def __configure_routes(self):
        @self.server.route('/message', methods=['POST'])
        def receive_message():
            if not request.is_json:
                return ('O request precisa ser um JSON', 400)

            message = request.get_json()
            logger.debug(f'P2PChannel: recebendo {message} de {request.url}')
            self.emit(f'message', message)
            return ('', 200)