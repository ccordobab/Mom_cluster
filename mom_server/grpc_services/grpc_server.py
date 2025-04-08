# mom_server/grpc_services/grpc_server.py

import grpc
from concurrent import futures
from mom_server.grpc_services import messaging_pb2_grpc, messaging_pb2
from mom_server.config import CLUSTER_NODES, api_to_grpc_address, SELF_HOST
import os
import threading
import sys
import logging
import time
import json

# Importar funciones de state (que ahora re-exporta desde los repositorios)
from mom_server.services.state import (
    get_topics, create_topic, delete_topic, add_topic_message,
    get_queues, create_queue, delete_queue, add_queue_message,
    update_state
)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MessagingService(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, self_port, other_nodes):
        self.self_port = self_port
        # Filtrar nodos vacíos y eliminar espacios
        self.other_nodes = [node.strip() for node in other_nodes if node.strip()]
        self.node_lock = threading.Lock()  # Para operaciones seguras en múltiples hilos
        self.replication_history = set()   # Para evitar ciclos de replicación
        
        logger.info(f"Inicializando servicio de mensajería en puerto {self_port}")
        logger.info(f"Nodos conectados: {self.other_nodes}")
        
        # Verificar conexión con otros nodos
        self.check_node_connections()
        # Sincronizar el estado inicial (tópicos y colas)
        self.sync_with_cluster()

    def sync_with_cluster(self):
        """Sincroniza tópicos y colas con otros nodos del clúster."""
        logger.info(f"Iniciando sincronización con el clúster... {self.other_nodes}")
        for node in self.other_nodes:
            try:
                options = [
                    ('grpc.max_receive_message_length', 1024 * 1024 * 10),
                    ('grpc.max_send_message_length', 1024 * 1024 * 10)
                ]
                with grpc.insecure_channel(node, options=options) as channel:
                    try:
                        grpc.channel_ready_future(channel).result(timeout=5)
                        stub = messaging_pb2_grpc.MessagingServiceStub(channel)
                        topics_response = stub.ListTopics(messaging_pb2.EmptyRequest(), timeout=5)
                        logger.info(f"Sincronizando tópicos desde {node}: {topics_response.topics}")
                        
                        # Obtén los tópicos locales y los del nodo remoto
                        local_topics = get_topics()
                        for topic_name in topics_response.topics:
                            if topic_name not in local_topics:
                                logger.info(f"Añadiendo tópico de sincronización: {topic_name}")
                                create_topic(topic_name, "system")
                        
                        queues_response = stub.ListQueues(messaging_pb2.EmptyRequest(), timeout=5)
                        logger.info(f"Sincronizando colas desde {node}: {queues_response.queues}")
                        
                        # Obtén las colas locales y las del nodo remoto
                        local_queues = get_queues()
                        for queue_name in queues_response.queues:
                            if queue_name not in local_queues:
                                logger.info(f"Añadiendo cola de sincronización: {queue_name}")
                                create_queue(queue_name, "system")
                                
                    except grpc.FutureTimeoutError:
                        logger.warning(f"Timeout esperando conectividad con {node} durante sincronización")
            except Exception as e:
                logger.error(f"Error sincronizando con {node}: {str(e)}")

    def check_node_connections(self):
        """Verifica la conexión con otros nodos al iniciar."""
        for node in self.other_nodes:
            try:
                options = [
                    ('grpc.max_receive_message_length', 1024 * 1024 * 10),
                    ('grpc.max_send_message_length', 1024 * 1024 * 10)
                ]
                with grpc.insecure_channel(node, options=options) as channel:
                    try:
                        grpc.channel_ready_future(channel).result(timeout=5)
                        logger.info(f"✅ Conexión con nodo {node} establecida")
                    except grpc.FutureTimeoutError:
                        logger.warning(f"⚠️ Nodo {node} no disponible en este momento")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo establecer conexión con nodo {node}: {e}")

    def ReplicateMessage(self, request, context):
        """Recibe un mensaje para replicar desde otro nodo."""
        message_id = f"{request.topic_name}:{request.sender}:{request.content}"
        with self.node_lock:
            if message_id in self.replication_history:
                return messaging_pb2.MessageResponse(status="ALREADY_PROCESSED")
            self.replication_history.add(message_id)

        logger.info(f"[{self.self_port}] 📥 Recibido: {request.topic_name} - {request.content}")
        
        # Verificar si el tópico existe en la base de datos
        topics = get_topics()
        if request.topic_name not in topics:
            logger.warning(f"[{self.self_port}] ⚠️ Tópico no encontrado: {request.topic_name}")
            return messaging_pb2.MessageResponse(status="TOPIC_NOT_FOUND")
        
        # Agregar el mensaje al tópico en la base de datos
        add_topic_message(request.topic_name, request.sender, request.content)
        
        logger.info(f"[{self.self_port}] 💾 Mensaje guardado en tópico: {request.topic_name}")
        return messaging_pb2.MessageResponse(status="SUCCESS")

    def CreateTopic(self, request, context):
        """Crea un nuevo tópico en el sistema."""
        logger.info(f"[{self.self_port}] 🆕 Solicitud para crear tópico: {request.name}")
        
        topics = get_topics()
        if request.name in topics:
            logger.info(f"[{self.self_port}] ⚠️ Tópico ya existe: {request.name}")
            return messaging_pb2.TopicResponse(status="ERROR", message="Tópico ya existe")
        
        try:
            create_topic(request.name, request.owner)
            logger.info(f"[{self.self_port}] ✅ Tópico creado: {request.name}")
            self.replicate_topic_creation(request)
            return messaging_pb2.TopicResponse(status="SUCCESS", message=f"Tópico {request.name} creado")
        except Exception as e:
            logger.error(f"[{self.self_port}] Error al crear tópico: {str(e)}")
            return messaging_pb2.TopicResponse(status="ERROR", message=f"Error: {str(e)}")

    def DeleteTopic(self, request, context):
        """Elimina un tópico existente."""
        logger.info(f"[{self.self_port}] 🗑️ Solicitud para eliminar tópico: {request.name}")
        
        topics = get_topics()
        if request.name not in topics:
            return messaging_pb2.TopicResponse(status="ERROR", message="Tópico no existe")
        
        if topics[request.name]["owner"] != request.owner and request.owner != "system":
            return messaging_pb2.TopicResponse(status="ERROR", message="No autorizado para eliminar este tópico")
        
        try:
            delete_topic(request.name)
            logger.info(f"[{self.self_port}] ✅ Tópico eliminado: {request.name}")
            self.replicate_topic_deletion(request)
            return messaging_pb2.TopicResponse(status="SUCCESS", message=f"Tópico {request.name} eliminado")
        except Exception as e:
            logger.error(f"[{self.self_port}] Error al eliminar tópico: {str(e)}")
            return messaging_pb2.TopicResponse(status="ERROR", message=f"Error: {str(e)}")

    def ListTopics(self, request, context):
        """Lista todos los tópicos disponibles."""
        logger.info(f"[{self.self_port}] 📋 Solicitud para listar tópicos")
        topics = get_topics()
        topic_list = list(topics.keys())
        return messaging_pb2.TopicsListResponse(topics=topic_list)

    def CreateQueue(self, request, context):
        """Crea una nueva cola en el sistema."""
        logger.info(f"[{self.self_port}] 🆕 Solicitud para crear cola: {request.name}")
        
        queues = get_queues()
        if request.name in queues:
            logger.info(f"[{self.self_port}] ⚠️ Cola ya existe: {request.name}")
            return messaging_pb2.QueueResponse(status="ERROR", message="Cola ya existe")
        
        try:
            create_queue(request.name, request.owner)
            logger.info(f"[{self.self_port}] ✅ Cola creada: {request.name}")
            self.replicate_queue_creation(request)
            return messaging_pb2.QueueResponse(status="SUCCESS", message=f"Cola {request.name} creada")
        except Exception as e:
            logger.error(f"[{self.self_port}] Error al crear cola: {str(e)}")
            return messaging_pb2.QueueResponse(status="ERROR", message=f"Error: {str(e)}")

    def DeleteQueue(self, request, context):
        """Elimina una cola existente."""
        logger.info(f"[{self.self_port}] 🗑️ Solicitud para eliminar cola: {request.name}")
        
        queues = get_queues()
        if request.name not in queues:
            return messaging_pb2.QueueResponse(status="ERROR", message="Cola no existe")
        
        if queues[request.name]["owner"] != request.owner and request.owner != "system":
            return messaging_pb2.QueueResponse(status="ERROR", message="No autorizado para eliminar esta cola")
        
        try:
            delete_queue(request.name)
            logger.info(f"[{self.self_port}] ✅ Cola eliminada: {request.name}")
            self.replicate_queue_deletion(request)
            return messaging_pb2.QueueResponse(status="SUCCESS", message=f"Cola {request.name} eliminada")
        except Exception as e:
            logger.error(f"[{self.self_port}] Error al eliminar cola: {str(e)}")
            return messaging_pb2.QueueResponse(status="ERROR", message=f"Error: {str(e)}")

    def ListQueues(self, request, context):
        """Lista todas las colas disponibles."""
        logger.info(f"[{self.self_port}] 📋 Solicitud para listar colas")
        queues = get_queues()
        queue_list = list(queues.keys())
        return messaging_pb2.QueuesListResponse(queues=queue_list)

    def SendMessageToQueue(self, request, context):
        """Envía un mensaje a una cola específica."""
        logger.info(f"[{self.self_port}] 📤 Solicitud para enviar mensaje a cola: {request.queue_name}")
        
        queues = get_queues()
        if request.queue_name not in queues:
            return messaging_pb2.MessageResponse(status="ERROR")
        
        try:
            add_queue_message(request.queue_name, request.sender, request.content)
            logger.info(f"[{self.self_port}] 💾 Mensaje guardado en cola: {request.queue_name}")
            return messaging_pb2.MessageResponse(status="SUCCESS")
        except Exception as e:
            logger.error(f"[{self.self_port}] Error al enviar mensaje a cola: {str(e)}")
            return messaging_pb2.MessageResponse(status="ERROR", message=f"Error: {str(e)}")

    # --- Funciones de replicación ---
    def replicate_topic_creation(self, request):
        """Replica la creación de un tópico a otros nodos."""
        self_host = os.getenv("SELF_HOST", "localhost:8000")
        for node in CLUSTER_NODES:
            if node == self_host or not node.strip():
                continue
            grpc_address = api_to_grpc_address(node)
            if not grpc_address:
                logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
                continue
                
            # El resto del código sigue igual...
            logger.info(f"[{self_host}] Replicando tópico '{request.name}' a nodo gRPC: {grpc_address}")
            max_retries = 3
            for attempt in range(max_retries):
                logger.info(f"[{self_host}] Intento {attempt+1} de replicar '{request.name}' a {grpc_address}")
                try:
                    options = [
                        ('grpc.max_receive_message_length', 1024*1024*10),
                        ('grpc.max_send_message_length', 1024*1024*10)
                    ]
                    channel = grpc.insecure_channel(grpc_address, options=options)
                    try:
                        grpc.channel_ready_future(channel).result(timeout=5)
                        logger.info(f"[{self_host}] Canal listo en {grpc_address}")
                    except grpc.FutureTimeoutError:
                        logger.error(f"[{self_host}] Timeout esperando canal en {grpc_address}")
                        if attempt == max_retries - 1:
                            break
                        continue
                    stub = messaging_pb2_grpc.MessagingServiceStub(channel)
                    response = stub.CreateTopic(request, timeout=5)
                    if response.status == "ERROR" and "Tópico ya existe" in response.message:
                        logger.info(f"[{self_host}] Tópico '{request.name}' ya existe en {grpc_address}, replicado")
                    else:
                        logger.info(f"[{self_host}] Tópico '{request.name}' replicado a {grpc_address}: {response.status}")
                    with open('replication_record.txt', 'a') as f:
                        f.write(f"{grpc_address}:{request.name}:topic\n")
                    break
                except Exception as e:
                    logger.error(f"[{self_host}] Error replicando tópico a {grpc_address} (intento {attempt+1}): {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                finally:
                    channel.close()

    # El resto de los métodos de replicate_ siguen igual...
    # Las funciones replicate_topic_deletion, replicate_queue_creation, replicate_queue_deletion, etc.

def serve():
    # Obtener parámetros de línea de comandos o variables de entorno
    if len(sys.argv) > 1:
        self_port = sys.argv[1]
        other_nodes = sys.argv[2:] if len(sys.argv) > 2 else []
    else:
        self_port = os.getenv("GRPC_PORT", "50051")
        other_nodes = os.getenv("GRPC_NODES", "").split(",")
    
    server_options = [
        ('grpc.max_send_message_length', 1024*1024*10),
        ('grpc.max_receive_message_length', 1024*1024*10),
        ('grpc.max_concurrent_streams', 100)
    ]
    
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=server_options
    )
    
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        MessagingService(self_port, other_nodes), server
    )
    
    try:
        bind_address = f'[::]:{self_port}'
        portnum = server.add_insecure_port(bind_address)
        if portnum == 0:
            raise RuntimeError(f"No se pudo enlazar al puerto {self_port}. Puede estar ocupado.")
        server.start()
        logger.info(f"🚀 Servidor gRPC escuchando en puerto {self_port}")
        logger.info(f"📡 Nodos conectados: {other_nodes}")
        time.sleep(1)
        server.wait_for_termination()
    except Exception as e:
        logger.error(f"❌ Error al iniciar el servidor gRPC: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    serve()
