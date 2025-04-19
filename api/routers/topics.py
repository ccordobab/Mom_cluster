# api/routers/topics.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Importar las funciones originales
from api.routers.auth import verify_token  
from mom_server.services.state import (
    get_topics,
    create_topic,
    delete_topic
)
from mom_server.services.messaging import (
    replicate_topic_to_cluster,
    replicate_topic_deletion_to_cluster,
    replicate_topic_to_specific_nodes,
    replicate_topic_deletion_to_specific_nodes
)

# NUEVAS IMPORTACIONES para particionamiento
from mom_server.services.partitioning import get_partition_for_topic, is_node_responsible, get_responsible_nodes
import requests
from mom_server.config import PARTITIONING_ENABLED, CLUSTER_NODES
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

class TopicQueue(BaseModel):
    name: str
    owner: str

@router.post("/")
def create_topic_endpoint(topic: TopicQueue, token: str):
    user = verify_token(token)
    
    # NUEVO CÓDIGO: Verificar particionamiento
    if PARTITIONING_ENABLED:
        partition_info = get_partition_for_topic(topic.name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario
                response = requests.post(
                    f"http://{primary_node}/messages/topics",
                    json={"name": topic.name, "owner": user},
                    params={"token": token},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al contactar nodo primario: {str(e)}")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    topics = get_topics()
    if topic.name in topics:
        raise HTTPException(status_code=400, detail="Tópico ya existe")
    try:
        create_topic(topic.name, user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear el tópico: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(topic.name)
        replicate_topic_to_specific_nodes(topic.name, user, responsible_nodes)
    else:
        # Comportamiento original: replicar a todos los nodos
        replicate_topic_to_cluster(topic.name, user)
    
    return {"message": f"Tópico {topic.name} creado"}

@router.delete("/{topic_name}")
def delete_topic_endpoint(topic_name: str, token: str):
    user = verify_token(token)
    
    # NUEVO CÓDIGO: Verificar particionamiento
    if PARTITIONING_ENABLED:
        partition_info = get_partition_for_topic(topic_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario
                response = requests.delete(
                    f"http://{primary_node}/messages/topics/{topic_name}",
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
    if topics[topic_name]["owner"] != user:
        raise HTTPException(status_code=403, detail="No autorizado para eliminar este tópico")
    try:
        delete_topic(topic_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar el tópico: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(topic_name)
        replicate_topic_deletion_to_specific_nodes(topic_name, user, responsible_nodes)
    else:
        # Comportamiento original: replicar a todos los nodos
        replicate_topic_deletion_to_cluster(topic_name, user)
    
    return {"message": f"Tópico {topic_name} eliminado"}

@router.get("/")
def list_topics_endpoint():
    # Obtener tópicos locales
    local_topics = get_topics()
    
    # NUEVO CÓDIGO: Si el particionamiento está habilitado, consultar otros nodos
    if PARTITIONING_ENABLED:
        all_topics = list(local_topics.keys())
        
        # Consultar otros nodos para tópicos adicionales
        for node in CLUSTER_NODES:
            try:
                response = requests.get(f"http://{node}/messages/topics", timeout=3)
                if response.status_code == 200:
                    remote_topics = response.json().get("topics", [])
                    # Añadir tópicos únicos a la lista
                    for topic in remote_topics:
                        if topic not in all_topics:
                            all_topics.append(topic)
            except Exception as e:
                logger.warning(f"Error al consultar tópicos en nodo {node}: {str(e)}")
        
        return {"topics": all_topics}
    
    # CÓDIGO ORIGINAL: Sin particionamiento
    return {"topics": list(local_topics.keys())}