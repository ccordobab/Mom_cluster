# api/routers/messages.py - Versión corregida

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import logging

# AÑADIR estas nuevas importaciones
from mom_server.services.partitioning import get_partition_for_topic, get_partition_for_queue, is_node_responsible, get_responsible_nodes
from mom_server.config import PARTITIONING_ENABLED, CLUSTER_NODES
import requests
import logging

# Mantener las importaciones originales
from api.routers.auth import verify_token
from mom_server.services.state import (
    get_topics,
    add_topic_message,
    get_queues,
    add_queue_message,
    consume_queue_message
)
from mom_server.services.messaging import replicate_message_to_cluster, replicate_message_to_specific_nodes

logger = logging.getLogger(__name__)
router = APIRouter()

class Message(BaseModel):
    sender: str
    content: str

@router.post("/topic/{topic_name}")
def send_message_endpoint(topic_name: str, message: Message, token: str, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_topic(topic_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo mensaje para tópico '{topic_name}' al nodo primario: {primary_node}")
                response = requests.post(
                    f"http://{primary_node}/messages/messages/topic/{topic_name}",
                    json={"sender": message.sender, "content": message.content},
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Procesando mensaje localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para el tópico '{topic_name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    topics = get_topics()
    if topic_name not in topics:
        logger.warning(f"Tópico '{topic_name}' no encontrado")
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    try:
        logger.info(f"Agregando mensaje al tópico '{topic_name}' localmente")
        add_topic_message(topic_name, user, message.content)
    except Exception as e:
        logger.error(f"Error al agregar mensaje al tópico '{topic_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al agregar mensaje al tópico: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(topic_name)
        logger.info(f"Replicando mensaje para '{topic_name}' a nodos responsables: {responsible_nodes}")
        replicate_message_to_specific_nodes(topic_name, user, message.content, responsible_nodes)
    else:
        # Comportamiento original: replicar a todos los nodos
        logger.info(f"Replicando mensaje para '{topic_name}' a todo el clúster")
        replicate_message_to_cluster(topic_name, user, message.content)
    
    return {"message": "Mensaje enviado y replicado"}

# MODIFICAR las funciones para colas de manera similar
@router.post("/queue/{queue_name}")
def send_queue_message_endpoint(queue_name: str, message: Message, token: str, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_queue(queue_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo mensaje para cola '{queue_name}' al nodo primario: {primary_node}")
                response = requests.post(
                    f"http://{primary_node}/messages/messages/queue/{queue_name}",
                    json={"sender": message.sender, "content": message.content},
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Procesando mensaje localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para la cola '{queue_name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    queues = get_queues()
    if queue_name not in queues:
        logger.warning(f"Cola '{queue_name}' no encontrada")
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    try:
        logger.info(f"Agregando mensaje a la cola '{queue_name}' localmente")
        add_queue_message(queue_name, user, message.content)
    except Exception as e:
        logger.error(f"Error al agregar mensaje a la cola '{queue_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al agregar mensaje a la cola: {str(e)}")
    return {"message": "Mensaje enviado a la cola"}

@router.get("/topic/{topic_name}")
def get_messages_endpoint(topic_name: str, redirected: bool = False):
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_topic(topic_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo obtención de mensajes de tópico '{topic_name}' al nodo primario: {primary_node}")
                response = requests.get(
                    f"http://{primary_node}/messages/messages/topic/{topic_name}",
                    params={"redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Obteniendo mensajes localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para el tópico '{topic_name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    topics = get_topics()
    if topic_name not in topics:
        logger.warning(f"Tópico '{topic_name}' no encontrado")
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    return {"messages": topics[topic_name]["messages"]}

@router.get("/queue/{queue_name}")
def get_queue_message_endpoint(queue_name: str, token: str, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_queue(queue_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo obtención de mensaje de cola '{queue_name}' al nodo primario: {primary_node}")
                response = requests.get(
                    f"http://{primary_node}/messages/messages/queue/{queue_name}",
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Obteniendo mensaje localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para la cola '{queue_name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    queues = get_queues()
    if queue_name not in queues:
        logger.warning(f"Cola '{queue_name}' no encontrada")
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    
    msg = consume_queue_message(queue_name)
    logger.info(f"Mensaje consumido de cola '{queue_name}': {msg}")
    if not msg:
        return {"message": None}
    
    return {"message": msg}