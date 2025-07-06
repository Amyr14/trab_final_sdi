from flask import Flask, request
from pyee import EventEmitter
from threading import Thread
import requests
import logging

class P2PChannel(EventEmitter):
    '''
        Abtrai a comunicação ponto-a-ponto.
        
        Args:
            host_address (tuple(str, int)): o par IP:porta que deve ser utilizado pelo servidor
            message_timeout (float): o timeout de mensagens em segundos
    '''
    def __init__(self, host_address, message_timeout):
        self.is_running = False
        self.address = host_address
        self.member_table = None
        self.message_timeout = message_timeout
        self.server = Flask('P2PChannel')
        self.__configure_routes()
        
    def start(self, member_table: dict):
        '''
            Abre o canal de comunicação
            
            Args:
                member_table (dict): dicionário contendo os membros da rede
        '''
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)

        self.is_running = True
        self.member_table = member_table
        host, port = self.address
        Thread(target=lambda: self.server.run(host=host, port=port)).start() # investigar possível bloqueio causado por falha no meio da comunicação
        
    def send(self, fromId, toId, message_type):
        '''
            Envia uma mensagem de um membro para outro
            
            Args:
                fromId (hashable): id to remetente
                toId (hashable): id to destinatário
                message_type: 'election' | 'coordenator'
            
            Throws:
                `RequestException`: algo de errado aconteceu com o envio (timeout, refused, etc)
        '''
        if not self.is_running:
            raise Exception('Canal não inicializado')
        
        payload = { 'id': fromId }
        member = self.member_table.get(toId)
        ip, port = member['address']
        url = f'http://{ip}:{port}/{message_type}'
        
        requests.post(
            url=url,
            json=payload,
            timeout=self.message_timeout
        )
        
    def __configure_routes(self):
        @self.server.route('/message', methods=['POST'])
        def receive_message():
            if not request.is_json():
                return ('O request precisa ser um JSON', 400)

            message: dict = request.get_json()
            id_sender = message.get('id')
            message_type = message.get('type')
            
            self.emit(f'{message_type}', id_sender)
            return ('', 200)