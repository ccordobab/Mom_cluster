# api/routers/messages.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

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
def send_message_endpoint(topic_name: str, message: Message, token: str):
    user = verify_token(token)
    
    # NUEVO CÓDIGO: Verificar particionamiento
    if PARTITIONING_ENABLED:
        partition_info = get_partition_for_topic(topic_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario
                response = requests.post(
                    f"http://{primary_node}/messages/messages/topic/{topic_name}",
                    json={"sender": message.sender, "content": message.content},
                    params={"token": token},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al contactar nodo primario: {str(e)}")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    topics = get_topics()
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    try:
        add_topic_message(topic_name, user, message.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al agregar mensaje al tópico: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(topic_name)
        replicate_message_to_specific_nodes(topic_name, user, message.content, responsible_nodes)
    else:
        # Comportamiento original: replicar a todos los nodos
        replicate_message_to_cluster(topic_name, user, message.content)
    
    return {"message": "Mensaje enviado y replicado"}

# MODIFICAR las funciones para colas de manera similar
@router.post("/queue/{queue_name}")
def send_queue_message_endpoint(queue_name: str, message: Message, token: str):
    user = verify_token(token)
    
    # NUEVO CÓDIGO: Verificar particionamiento
    if PARTITIONING_ENABLED:
        partition_info = get_partition_for_queue(queue_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario
                response = requests.post(
                    f"http://{primary_node}/messages/messages/queue/{queue_name}",
                    json={"sender": message.sender, "content": message.content},
                    params={"token": token},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al contactar nodo primario: {str(e)}")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    queues = get_queues()
    if queue_name not in queues:
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    try:
        add_queue_message(queue_name, user, message.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al agregar mensaje a la cola: {str(e)}")
    return {"message": "Mensaje enviado a la cola"}

@router.get("/topic/{topic_name}")
def get_messages_endpoint(topic_name: str):
    # NUEVO CÓDIGO: Verificar particionamiento
    if PARTITIONING_ENABLED:
        partition_info = get_partition_for_topic(topic_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario
                response = requests.get(
                    f"http://{primary_node}/messages/messages/topic/{topic_name}",
                    timeout=5
                )
                return response.json()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al contactar nodo primario: {str(e)}")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    topics = get_topics()
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    return {"messages": topics[topic_name]["messages"]}

@router.get("/queue/{queue_name}")
def get_queue_message_endpoint(queue_name: str, token: str):
    user = verify_token(token)
    
    # NUEVO CÓDIGO: Verificar particionamiento
    if PARTITIONING_ENABLED:
        partition_info = get_partition_for_queue(queue_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario
                response = requests.get(
                    f"http://{primary_node}/messages/messages/queue/{queue_name}",
                    params={"token": token},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al contactar nodo primario: {str(e)}")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    queues = get_queues()
    if queue_name not in queues:
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    
    msg = consume_queue_message(queue_name)
    if not msg:
        return {"message": None}
    
    return {"message": msg}