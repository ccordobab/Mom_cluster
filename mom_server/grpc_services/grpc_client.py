# mom_server/grpc_services/grpc_client.py

import grpc
import argparse
import logging
import time
from mom_server.grpc_services import messaging_pb2, messaging_pb2_grpc

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def send_message(address="localhost:50051", topic="alertas", sender="cliente1", content="¡Mensaje de prueba!"):
    """Envía un mensaje a un tópico específico en un servidor gRPC."""
    try:
        options = [
            ('grpc.max_receive_message_length', 1024 * 1024 * 10),
            ('grpc.max_send_message_length', 1024 * 1024 * 10)
        ]
        logger.info(f"Conectando a {address}...")
        channel = grpc.insecure_channel(address, options=options)
        try:
            grpc.channel_ready_future(channel).result(timeout=3)
            logger.info("Canal conectado correctamente")
        except grpc.FutureTimeoutError:
            logger.error(f"Timeout conectando a {address}")
            return False
        
        stub = messaging_pb2_grpc.MessagingServiceStub(channel)
        
        # Listar tópicos existentes
        logger.info("Listando tópicos existentes...")
        list_request = messaging_pb2.EmptyRequest()
        topics_response = stub.ListTopics(list_request, timeout=3)
        logger.info(f"Tópicos disponibles: {topics_response.topics}")
        
        # Si el tópico no existe, crearlo primero
        if topic not in topics_response.topics:
            logger.info(f"Tópico '{topic}' no existe, creándolo...")
            topic_request = messaging_pb2.TopicRequest(name=topic, owner=sender)
            topic_response = stub.CreateTopic(topic_request, timeout=5)
            logger.info(f"Resultado de creación de tópico: {topic_response.status} - {topic_response.message}")
        
        # Enviar el mensaje al tópico
        logger.info(f"Enviando mensaje al tópico '{topic}'...")
        request = messaging_pb2.MessageRequest(topic_name=topic, sender=sender, content=content)
        
        # Intentar hasta 3 veces en caso de error
        for attempt in range(3):
            try:
                response = stub.ReplicateMessage(request, timeout=5)
                logger.info(f"Respuesta: {response.status}")
                return True
            except Exception as e:
                logger.error(f"Error (intento {attempt+1}/3): {str(e)}")
                if attempt < 2:
                    time.sleep(1)
                else:
                    return False
    except Exception as e:
        logger.error(f"Error de conexión: {str(e)}")
        return False

def list_topics(address="localhost:50051"):
    """Lista los tópicos en un servidor gRPC."""
    try:
        options = [
            ('grpc.max_receive_message_length', 1024 * 1024 * 10),
            ('grpc.max_send_message_length', 1024 * 1024 * 10)
        ]
        channel = grpc.insecure_channel(address, options=options)
        grpc.channel_ready_future(channel).result(timeout=3)
        stub = messaging_pb2_grpc.MessagingServiceStub(channel)
        response = stub.ListTopics(messaging_pb2.EmptyRequest(), timeout=3)
        logger.info(f"Tópicos en {address}: {response.topics}")
        return response.topics
    except Exception as e:
        logger.error(f"Error al listar tópicos: {str(e)}")
        return []

def create_topic(address="localhost:50051", topic="nuevo_topico", owner="cliente"):
    """Crea un nuevo tópico en un servidor gRPC."""
    try:
        options = [
            ('grpc.max_receive_message_length', 1024 * 1024 * 10),
            ('grpc.max_send_message_length', 1024 * 1024 * 10)
        ]
        channel = grpc.insecure_channel(address, options=options)
        grpc.channel_ready_future(channel).result(timeout=3)
        stub = messaging_pb2_grpc.MessagingServiceStub(channel)
        request = messaging_pb2.TopicRequest(name=topic, owner=owner)
        response = stub.CreateTopic(request, timeout=5)
        logger.info(f"Creación de tópico {topic}: {response.status} - {response.message}")
        return response.status == "SUCCESS"
    except Exception as e:
        logger.error(f"Error al crear tópico: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cliente gRPC para el sistema de mensajería')
    parser.add_argument('--server', default='localhost:50051', help='Dirección del servidor gRPC')
    parser.add_argument('--action', choices=['send', 'list', 'create'], default='send', help='Acción a realizar')
    parser.add_argument('--topic', default='alertas', help='Nombre del tópico')
    parser.add_argument('--sender', default='cliente1', help='Nombre del remitente')
    parser.add_argument('--message', default='¡Mensaje de prueba!', help='Contenido del mensaje')
    
    args = parser.parse_args()
    
    if args.action == 'send':
        send_message(args.server, args.topic, args.sender, args.message)
    elif args.action == 'list':
        list_topics(args.server)
    elif args.action == 'create':
        create_topic(args.server, args.topic, args.sender)
