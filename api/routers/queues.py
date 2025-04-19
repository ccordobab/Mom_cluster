# api/routers/queues.py - Versión corregida

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import logging

# Mantener importaciones originales
from api.routers.auth import verify_token
from mom_server.services.state import (
    get_queues,
    create_queue,
    delete_queue
)
from mom_server.services.messaging import (
    replicate_queue_to_cluster,
    replicate_queue_deletion_to_cluster,
    replicate_queue_to_specific_nodes,
    replicate_queue_deletion_to_specific_nodes
)

# NUEVAS IMPORTACIONES para particionamiento
from mom_server.services.partitioning import get_partition_for_queue, is_node_responsible, get_responsible_nodes
from mom_server.config import PARTITIONING_ENABLED, CLUSTER_NODES
import requests
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

class QueueData(BaseModel):
    name: str
    owner: str

@router.post("/")
def create_queue_endpoint(queue: QueueData, token: str, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_queue(queue.name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo solicitud de creación de cola '{queue.name}' al nodo primario: {primary_node}")
                response = requests.post(
                    f"http://{primary_node}/messages/queues",
                    json={"name": queue.name, "owner": user},
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Procesando localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para la cola '{queue.name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    queues = get_queues()
    if queue.name in queues:
        logger.warning(f"Cola '{queue.name}' ya existe")
        raise HTTPException(status_code=400, detail="Cola ya existe")
    try:
        logger.info(f"Creando cola '{queue.name}' localmente")
        create_queue(queue.name, user)
    except Exception as e:
        logger.error(f"Error al crear la cola '{queue.name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al crear la cola: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(queue.name, "queue")
        replicate_queue_to_specific_nodes(queue.name, user, responsible_nodes)
        logger.info(f"Replicando cola '{queue.name}' a nodos responsables: {responsible_nodes}")
    else:
        # Comportamiento original: replicar a todos los nodos
        logger.info(f"Replicando cola '{queue.name}' a todo el clúster")
        replicate_queue_to_cluster(queue.name, user)
    
    return {"message": f"Cola {queue.name} creada"}

@router.delete("/{queue_name}")
def delete_queue_endpoint(queue_name: str, token: str, redirected: bool = False):
    user = verify_token(token)
    
    # CÓDIGO CORREGIDO: Verificar particionamiento con parámetro de redirección
    if PARTITIONING_ENABLED and not redirected:
        partition_info = get_partition_for_queue(queue_name)
        
        if not partition_info["is_primary"] and not partition_info["is_secondary"]:
            # Este nodo no es responsable - reenviar al nodo primario
            primary_node = partition_info["primary"]
            try:
                # Reenviar la solicitud al nodo primario con flag de redirección
                logger.info(f"Redirigiendo solicitud de eliminación de cola '{queue_name}' al nodo primario: {primary_node}")
                response = requests.delete(
                    f"http://{primary_node}/messages/queues/{queue_name}",
                    params={"token": token, "redirected": True},
                    timeout=5
                )
                return response.json()
            except Exception as e:
                logger.error(f"Error al contactar nodo primario {primary_node}: {str(e)}")
                # En caso de error, procesamos localmente en vez de fallar
                logger.warning(f"Procesando localmente debido al error de comunicación")
        else:
            logger.info(f"Nodo actual es responsable para la cola '{queue_name}' (primario: {partition_info['is_primary']}, secundario: {partition_info['is_secondary']})")
    
    # CÓDIGO ORIGINAL: Este nodo es responsable o no hay particionamiento
    queues = get_queues()
    if queue_name not in queues:
        logger.warning(f"Cola '{queue_name}' no encontrada")
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    if queues[queue_name]["owner"] != user:
        logger.warning(f"Usuario '{user}' no autorizado para eliminar cola '{queue_name}'")
        raise HTTPException(status_code=403, detail="No autorizado para eliminar esta cola")
    try:
        logger.info(f"Eliminando cola '{queue_name}' localmente")
        delete_queue(queue_name)
    except Exception as e:
        logger.error(f"Error al eliminar la cola '{queue_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al eliminar la cola: {str(e)}")
    
    # MODIFICACIÓN: Si hay particionamiento, replicar solo a nodos responsables
    if PARTITIONING_ENABLED:
        responsible_nodes = get_responsible_nodes(queue_name, "queue")
        replicate_queue_deletion_to_specific_nodes(queue_name, user, responsible_nodes)
        logger.info(f"Replicando eliminación de cola '{queue_name}' a nodos responsables: {responsible_nodes}")
    else:
        # Comportamiento original: replicar a todos los nodos
        logger.info(f"Replicando eliminación de cola '{queue_name}' a todo el clúster")
        replicate_queue_deletion_to_cluster(queue_name, user)
    
    return {"message": f"Cola {queue_name} eliminada"}

@router.get("/")
def list_queues_endpoint(redirected: bool = False):
    # Obtener colas locales
    local_queues = get_queues()
    
    # CÓDIGO CORREGIDO: Si el particionamiento está habilitado, consultar otros nodos
    # solo si no es una solicitud redirigida
    if PARTITIONING_ENABLED and not redirected:
        all_queues = list(local_queues.keys())
        
        # Consultar otros nodos para colas adicionales
        for node in CLUSTER_NODES:
            try:
                logger.info(f"Consultando colas en nodo: {node}")
                response = requests.get(
                    f"http://{node}/messages/queues", 
                    params={"redirected": True},
                    timeout=3
                )
                if response.status_code == 200:
                    remote_queues = response.json().get("queues", [])
                    logger.info(f"Colas en nodo {node}: {remote_queues}")
                    # Añadir colas únicas a la lista
                    for queue in remote_queues:
                        if queue not in all_queues:
                            all_queues.append(queue)
            except Exception as e:
                logger.warning(f"Error al consultar colas en nodo {node}: {str(e)}")
        
        logger.info(f"Lista completa de colas: {all_queues}")
        return {"queues": all_queues}
    
    # CÓDIGO ORIGINAL: Sin particionamiento o solicitud ya redirigida
    logger.info(f"Retornando solo colas locales: {list(local_queues.keys())}")
    return {"queues": list(local_queues.keys())}