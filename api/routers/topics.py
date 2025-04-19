# api/routers/topics.py - Versión corregida

from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel
import logging

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

# Importaciones para particionamiento
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
def create_topic_endpoint(topic: TopicQueue, token: str, request: Request, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_topic(topic.name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo solicitud de creación de tópico '{topic.name}' al nodo primario: {primary_node}")
                response = requests.post(
                    f"http://{primary_node}/messages/topics",
                    json={"name": topic.name, "owner": user},
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Procesando localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para el tópico '{topic.name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o la solicitud ya fue redirigida
    topics = get_topics()
    if topic.name in topics:
        logger.warning(f"Tópico '{topic.name}' ya existe")
        raise HTTPException(status_code=400, detail="Tópico ya existe")
    try:
        logger.info(f"Creando tópico '{topic.name}' localmente")
        create_topic(topic.name, user)
    except Exception as e:
        logger.error(f"Error al crear el tópico '{topic.name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al crear el tópico: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(topic.name)
        replicate_topic_to_specific_nodes(topic.name, user, responsible_nodes)
        logger.info(f"Replicando tópico '{topic.name}' a nodos responsables: {responsible_nodes}")
    else:
        # Comportamiento original: replicar a todos los nodos
        logger.info(f"Replicando tópico '{topic.name}' a todo el clúster")
        replicate_topic_to_cluster(topic.name, user)
    
    return {"message": f"Tópico {topic.name} creado"}

@router.delete("/{topic_name}")
def delete_topic_endpoint(topic_name: str, token: str, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_topic(topic_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo solicitud de eliminación de tópico '{topic_name}' al nodo primario: {primary_node}")
                response = requests.delete(
                    f"http://{primary_node}/messages/topics/{topic_name}",
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Procesando localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para el tópico '{topic_name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o la solicitud ya fue redirigida
    topics = get_topics()
    if topic_name not in topics:
        logger.warning(f"Tópico '{topic_name}' no encontrado")
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    if topics[topic_name]["owner"] != user:
        logger.warning(f"Usuario '{user}' no autorizado para eliminar tópico '{topic_name}'")
        raise HTTPException(status_code=403, detail="No autorizado para eliminar este tópico")
    try:
        logger.info(f"Eliminando tópico '{topic_name}' localmente")
        delete_topic(topic_name)
    except Exception as e:
        logger.error(f"Error al eliminar el tópico '{topic_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al eliminar el tópico: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(topic_name)
        replicate_topic_deletion_to_specific_nodes(topic_name, user, responsible_nodes)
        logger.info(f"Replicando eliminación de tópico '{topic_name}' a nodos responsables: {responsible_nodes}")
    else:
        # Comportamiento original: replicar a todos los nodos
        logger.info(f"Replicando eliminación de tópico '{topic_name}' a todo el clúster")
        replicate_topic_deletion_to_cluster(topic_name, user)
    
    return {"message": f"Tópico {topic_name} eliminado"}

@router.get("/")
def list_topics_endpoint(redirected: bool = False):
    # Obtener tópicos locales
    local_topics = get_topics()
    
    # En modo particionamiento, consultar otros nodos solo si no es una solicitud redirigida
    if PARTITIONING_ENABLED and not redirected:
        all_topics = list(local_topics.keys())
        
        # Consultar otros nodos para tópicos adicionales
        for node in CLUSTER_NODES:
            try:
                logger.info(f"Consultando tópicos en nodo: {node}")
                response = requests.get(f"http://{node}/messages/topics", 
                                        params={"redirected": True},
                                        timeout=3)
                if response.status_code == 200:
                    remote_topics = response.json().get("topics", [])
                    logger.info(f"Tópicos en nodo {node}: {remote_topics}")
                    # Añadir tópicos únicos a la lista
                    for topic in remote_topics:
                        if topic not in all_topics:
                            all_topics.append(topic)
            except Exception as e:
                logger.warning(f"Error al consultar tópicos en nodo {node}: {str(e)}")
        
        logger.info(f"Lista completa de tópicos: {all_topics}")
        return {"topics": all_topics}
    
    # CÓDIGO ORIGINAL: Sin particionamiento o solicitud ya redirigida
    logger.info(f"Retornando solo tópicos locales: {list(local_topics.keys())}")
    return {"topics": list(local_topics.keys())}