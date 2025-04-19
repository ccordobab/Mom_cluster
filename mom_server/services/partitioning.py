# mom_server/services/partitioning.py - Versión corregida
import hashlib
import logging
from mom_server.config import CLUSTER_NODES, SELF_HOST, PARTITION_REPLICATION_FACTOR

logger = logging.getLogger(__name__)

def get_partition_for_topic(topic_name, redirected=False):
    """Determina qué nodo es responsable de un tópico específico"""
    # Usamos un hash para distribuir los tópicos uniformemente
    hash_value = int(hashlib.md5(topic_name.encode()).hexdigest(), 16)
    node_count = len(CLUSTER_NODES) + 1  # Incluir el nodo local
    
    # Lista completa de nodos en el anillo
    nodes = [SELF_HOST] + CLUSTER_NODES
    
    # Ordenamos la lista para asegurar coherencia entre nodos
    sorted_nodes = sorted(nodes)
    
    # El índice primario se determina por el hash
    primary_index = hash_value % node_count
    primary_node = sorted_nodes[primary_index] if primary_index < len(sorted_nodes) else sorted_nodes[0]
    
    # Crear una lista de nodos secundarios (tantos como indique el factor de replicación)
    secondary_nodes = []
    for i in range(1, min(PARTITION_REPLICATION_FACTOR, node_count)):
        sec_index = (primary_index + i) % node_count
        secondary_node = sorted_nodes[sec_index] if sec_index < len(sorted_nodes) else sorted_nodes[0]
        secondary_nodes.append(secondary_node)
    
    # Verificar si este nodo es responsable
    is_primary = primary_node == SELF_HOST
    is_secondary = SELF_HOST in secondary_nodes
    
    # Si estamos procesando una solicitud ya redirigida, consideremos a este nodo responsable
    # para evitar ciclos de redirección
    if redirected:
        is_primary = True  # Forzar que el nodo actual se considere responsable
    
    result = {
        "primary": primary_node,
        "secondary_nodes": secondary_nodes,
        "is_primary": is_primary,
        "is_secondary": is_secondary,
        "all_responsible_nodes": [primary_node] + secondary_nodes
    }
    
    logger.debug(f"Particionamiento para tópico '{topic_name}': {result}")
    return result

def get_partition_for_queue(queue_name, redirected=False):
    """Determina qué nodo es responsable de una cola específica"""
    # Similar a tópicos pero con un offset para distribuir diferente
    hash_value = int(hashlib.md5(queue_name.encode()).hexdigest(), 16) + 1
    node_count = len(CLUSTER_NODES) + 1
    
    # Lista completa de nodos en el anillo
    nodes = [SELF_HOST] + CLUSTER_NODES
    
    # Ordenamos la lista para asegurar coherencia entre nodos
    sorted_nodes = sorted(nodes)
    
    # El índice primario se determina por el hash
    primary_index = hash_value % node_count
    primary_node = sorted_nodes[primary_index] if primary_index < len(sorted_nodes) else sorted_nodes[0]
    
    # Crear una lista de nodos secundarios
    secondary_nodes = []
    for i in range(1, min(PARTITION_REPLICATION_FACTOR, node_count)):
        sec_index = (primary_index + i) % node_count
        secondary_node = sorted_nodes[sec_index] if sec_index < len(sorted_nodes) else sorted_nodes[0]
        secondary_nodes.append(secondary_node)
    
    # Verificar si este nodo es responsable
    is_primary = primary_node == SELF_HOST
    is_secondary = SELF_HOST in secondary_nodes
    
    # Si estamos procesando una solicitud ya redirigida, consideremos a este nodo responsable
    if redirected:
        is_primary = True  # Forzar que el nodo actual se considere responsable
    
    result = {
        "primary": primary_node,
        "secondary_nodes": secondary_nodes,
        "is_primary": is_primary,
        "is_secondary": is_secondary,
        "all_responsible_nodes": [primary_node] + secondary_nodes
    }
    
    logger.debug(f"Particionamiento para cola '{queue_name}': {result}")
    return result

def is_node_responsible(entity_name, entity_type="topic", redirected=False):
    """Verifica si el nodo actual es responsable del tópico o cola"""
    if entity_type == "topic":
        partition = get_partition_for_topic(entity_name, redirected)
    else:  # queue
        partition = get_partition_for_queue(entity_name, redirected)
    
    return partition["is_primary"] or partition["is_secondary"]

def get_responsible_nodes(entity_name, entity_type="topic"):
    """Obtiene los nodos responsables para un tópico o cola"""
    if entity_type == "topic":
        partition = get_partition_for_topic(entity_name)
    else:  # queue
        partition = get_partition_for_queue(entity_name)
    
    return partition["all_responsible_nodes"]