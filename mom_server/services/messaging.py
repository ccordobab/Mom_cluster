# mom_server/services/messaging.py

from pydantic import BaseModel
from api.routers.auth import verify_token
import grpc
from mom_server.config import CLUSTER_NODES, api_to_grpc_address, SELF_HOST
from mom_server.grpc_services import messaging_pb2, messaging_pb2_grpc
import os
import logging
import time

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AÑADIR NUEVA FUNCIÓN para particionamiento
def replicate_topic_to_specific_nodes(topic_name: str, owner: str, target_nodes: list):
    """Replica la creación de un tópico a nodos específicos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in target_nodes:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
            
        logger.info(f"[{self_host}] Replicando tópico '{topic_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de replicar '{topic_name}' a {grpc_address}")
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
                req = messaging_pb2.TopicRequest(name=topic_name, owner=owner)
                response = stub.CreateTopic(req, timeout=5)
                if response.status == "ERROR" and "Tópico ya existe" in response.message:
                    logger.info(f"[{self_host}] Tópico '{topic_name}' ya existe en {grpc_address}, replicado")
                else:
                    logger.info(f"[{self_host}] Tópico '{topic_name}' replicado a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error replicando tópico a {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

# AÑADIR NUEVA FUNCIÓN para particionamiento con eliminación de tópicos
def replicate_topic_deletion_to_specific_nodes(topic_name: str, owner: str, target_nodes: list):
    """Replica la eliminación de un tópico a nodos específicos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in target_nodes:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando eliminación de tópico '{topic_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de eliminar réplica de '{topic_name}' en {grpc_address}")
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
                req = messaging_pb2.TopicRequest(name=topic_name, owner=owner)
                response = stub.DeleteTopic(req, timeout=5)
                if response.status == "ERROR" and "Tópico no existe" in response.message:
                    logger.info(f"[{self_host}] Tópico '{topic_name}' ya no existe en {grpc_address}, considerado eliminado")
                else:
                    logger.info(f"[{self_host}] Eliminación de tópico replicada a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error eliminando réplica de tópico en {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

# AÑADIR NUEVA FUNCIÓN para particionamiento de mensajes
def replicate_message_to_specific_nodes(topic_name: str, sender: str, content: str, target_nodes: list):
    """Replica un mensaje a nodos específicos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in target_nodes:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando mensaje de tópico '{topic_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de replicar mensaje de '{topic_name}' a {grpc_address}")
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
                req = messaging_pb2.MessageRequest(topic_name=topic_name, sender=sender, content=content)
                try:
                    response = stub.ReplicateMessage(req, timeout=5)
                    if response.status == "TOPIC_NOT_FOUND":
                        logger.info(f"[{self_host}] Tópico '{topic_name}' no encontrado en {grpc_address}, creando...")
                        create_req = messaging_pb2.TopicRequest(name=topic_name, owner=sender)
                        create_resp = stub.CreateTopic(create_req, timeout=5)
                        logger.info(f"[{self_host}] Creación de tópico en {grpc_address}: {create_resp.status}")
                        response = stub.ReplicateMessage(req, timeout=5)
                        if response.status == "TOPIC_NOT_FOUND":
                            logger.error(f"[{self_host}] Aún no se encontró el tópico '{topic_name}' en {grpc_address} tras creación")
                            continue
                    logger.info(f"[{self_host}] Mensaje replicado a {grpc_address}")
                    break
                except Exception as inner_e:
                    logger.error(f"[{self_host}] Error en llamada RPC: {str(inner_e)}")
                    if attempt == max_retries - 1:
                        try:
                            create_req = messaging_pb2.TopicRequest(name=topic_name, owner=sender)
                            stub.CreateTopic(create_req, timeout=5)
                            response = stub.ReplicateMessage(req, timeout=5)
                            logger.info(f"[{self_host}] Mensaje replicado a {grpc_address} tras crear tópico")
                            break
                        except Exception as final_e:
                            logger.error(f"[{self_host}] Error final al replicar mensaje: {str(final_e)}")
                finally:
                    channel.close()
            except Exception as e:
                logger.error(f"[{self_host}] Error replicando mensaje a {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)

# AÑADIR NUEVA FUNCIÓN para particionamiento de colas
def replicate_queue_to_specific_nodes(queue_name: str, owner: str, target_nodes: list):
    """Replica la creación de una cola a nodos específicos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in target_nodes:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando cola '{queue_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de replicar cola '{queue_name}' a {grpc_address}")
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
                req = messaging_pb2.QueueRequest(name=queue_name, owner=owner)
                response = stub.CreateQueue(req, timeout=5)
                if response.status == "ERROR" and "Cola ya existe" in response.message:
                    logger.info(f"[{self_host}] Cola '{queue_name}' ya existe en {grpc_address}, replicada")
                else:
                    logger.info(f"[{self_host}] Cola '{queue_name}' replicada a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error replicando cola a {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

# AÑADIR NUEVA FUNCIÓN para particionamiento con eliminación de colas
def replicate_queue_deletion_to_specific_nodes(queue_name: str, owner: str, target_nodes: list):
    """Replica la eliminación de una cola a nodos específicos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in target_nodes:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando eliminación de cola '{queue_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de eliminar réplica de cola '{queue_name}' en {grpc_address}")
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
                req = messaging_pb2.QueueRequest(name=queue_name, owner=owner)
                response = stub.DeleteQueue(req, timeout=5)
                if response.status == "ERROR" and "Cola no existe" in response.message:
                    logger.info(f"[{self_host}] Cola '{queue_name}' ya no existe en {grpc_address}, considerada eliminada")
                else:
                    logger.info(f"[{self_host}] Eliminación de cola replicada a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error eliminando réplica de cola en {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

# MANTENER todas las funciones originales
def replicate_topic_to_cluster(topic_name: str, owner: str):
    """Replica la creación de un tópico a todos los nodos del clúster."""
    # Mantener el código original
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in CLUSTER_NODES:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando tópico '{topic_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de replicar '{topic_name}' a {grpc_address}")
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
                req = messaging_pb2.TopicRequest(name=topic_name, owner=owner)
                response = stub.CreateTopic(req, timeout=5)
                if response.status == "ERROR" and "Tópico ya existe" in response.message:
                    logger.info(f"[{self_host}] Tópico '{topic_name}' ya existe en {grpc_address}, replicado")
                else:
                    logger.info(f"[{self_host}] Tópico '{topic_name}' replicado a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error replicando tópico a {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

# Mantener el resto de las funciones originales
def replicate_topic_deletion_to_cluster(topic_name: str, owner: str):
    """Replica la eliminación de un tópico a todos los nodos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in CLUSTER_NODES:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando eliminación de tópico '{topic_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de eliminar réplica de '{topic_name}' en {grpc_address}")
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
                req = messaging_pb2.TopicRequest(name=topic_name, owner=owner)
                response = stub.DeleteTopic(req, timeout=5)
                if response.status == "ERROR" and "Tópico no existe" in response.message:
                    logger.info(f"[{self_host}] Tópico '{topic_name}' ya no existe en {grpc_address}, considerado eliminado")
                else:
                    logger.info(f"[{self_host}] Eliminación de tópico replicada a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error eliminando réplica de tópico en {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

def replicate_queue_to_cluster(queue_name: str, owner: str):
    """Replica la creación de una cola a todos los nodos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in CLUSTER_NODES:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando cola '{queue_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de replicar cola '{queue_name}' a {grpc_address}")
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
                req = messaging_pb2.QueueRequest(name=queue_name, owner=owner)
                response = stub.CreateQueue(req, timeout=5)
                if response.status == "ERROR" and "Cola ya existe" in response.message:
                    logger.info(f"[{self_host}] Cola '{queue_name}' ya existe en {grpc_address}, replicada")
                else:
                    logger.info(f"[{self_host}] Cola '{queue_name}' replicada a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error replicando cola a {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

def replicate_queue_deletion_to_cluster(queue_name: str, owner: str):
    """Replica la eliminación de una cola a todos los nodos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in CLUSTER_NODES:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando eliminación de cola '{queue_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de eliminar réplica de cola '{queue_name}' en {grpc_address}")
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
                req = messaging_pb2.QueueRequest(name=queue_name, owner=owner)
                response = stub.DeleteQueue(req, timeout=5)
                if response.status == "ERROR" and "Cola no existe" in response.message:
                    logger.info(f"[{self_host}] Cola '{queue_name}' ya no existe en {grpc_address}, considerada eliminada")
                else:
                    logger.info(f"[{self_host}] Eliminación de cola replicada a {grpc_address}: {response.status}")
                break
            except Exception as e:
                logger.error(f"[{self_host}] Error eliminando réplica de cola en {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
            finally:
                channel.close()

def replicate_message_to_cluster(topic_name: str, sender: str, content: str):
    """Replica un mensaje a todos los nodos del clúster."""
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    for node in CLUSTER_NODES:
        if node == self_host or not node.strip():
            continue
        grpc_address = api_to_grpc_address(node)
        if not grpc_address:
            logger.error(f"[{self_host}] No se pudo obtener dirección gRPC para {node}")
            continue
        logger.info(f"[{self_host}] Replicando mensaje de tópico '{topic_name}' a nodo gRPC: {grpc_address}")
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"[{self_host}] Intento {attempt+1} de replicar mensaje de '{topic_name}' a {grpc_address}")
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
                req = messaging_pb2.MessageRequest(topic_name=topic_name, sender=sender, content=content)
                try:
                    response = stub.ReplicateMessage(req, timeout=5)
                    if response.status == "TOPIC_NOT_FOUND":
                        logger.info(f"[{self_host}] Tópico '{topic_name}' no encontrado en {grpc_address}, creando...")
                        create_req = messaging_pb2.TopicRequest(name=topic_name, owner=sender)
                        create_resp = stub.CreateTopic(create_req, timeout=5)
                        logger.info(f"[{self_host}] Creación de tópico en {grpc_address}: {create_resp.status}")
                        response = stub.ReplicateMessage(req, timeout=5)
                        if response.status == "TOPIC_NOT_FOUND":
                            logger.error(f"[{self_host}] Aún no se encontró el tópico '{topic_name}' en {grpc_address} tras creación")
                            continue
                    logger.info(f"[{self_host}] Mensaje replicado a {grpc_address}")
                    break
                except Exception as inner_e:
                    logger.error(f"[{self_host}] Error en llamada RPC: {str(inner_e)}")
                    if attempt == max_retries - 1:
                        try:
                            create_req = messaging_pb2.TopicRequest(name=topic_name, owner=sender)
                            stub.CreateTopic(create_req, timeout=5)
                            response = stub.ReplicateMessage(req, timeout=5)
                            logger.info(f"[{self_host}] Mensaje replicado a {grpc_address} tras crear tópico")
                            break
                        except Exception as final_e:
                            logger.error(f"[{self_host}] Error final al replicar mensaje: {str(final_e)}")
                finally:
                    channel.close()
            except Exception as e:
                logger.error(f"[{self_host}] Error replicando mensaje a {grpc_address} (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)