import hashlib
from mom_server.config import CLUSTER_NODES, SELF_HOST

def get_partition_for_topic(topic_name):
    """Determina qué nodo es responsable de un tópico específico"""
    # Usamos un hash para distribuir los tópicos uniformemente
    hash_value = int(hashlib.md5(topic_name.encode()).hexdigest(), 16)
    node_count = len(CLUSTER_NODES) + 1  # Incluir el nodo local
    primary_index = hash_value % node_count
    
    # Asignar un nodo secundario (siguiente en el anillo)
    secondary_index = (primary_index + 1) % node_count
    
    # Convertir índices a direcciones de nodos
    nodes = [SELF_HOST] + CLUSTER_NODES
    primary_node = nodes[primary_index] if primary_index < len(nodes) else nodes[0]
    secondary_node = nodes[secondary_index] if secondary_index < len(nodes) else nodes[0]
    
    return {
        "primary": primary_node,
        "secondary": secondary_node,
        "is_primary": primary_node == SELF_HOST,
        "is_secondary": secondary_node == SELF_HOST
    }

def get_partition_for_queue(queue_name):
    """Determina qué nodo es responsable de una cola específica"""
    # Similar a tópicos pero con un offset para distribuir diferente
    hash_value = int(hashlib.md5(queue_name.encode()).hexdigest(), 16) + 1
    node_count = len(CLUSTER_NODES) + 1
    primary_index = hash_value % node_count
    
    # Asignar un nodo secundario
    secondary_index = (primary_index + 1) % node_count
    
    nodes = [SELF_HOST] + CLUSTER_NODES
    primary_node = nodes[primary_index]
    secondary_node = nodes[secondary_index]
    
    return {
        "primary": primary_node,
        "secondary": secondary_node,
        "is_primary": primary_node == SELF_HOST,
        "is_secondary": secondary_node == SELF_HOST
    }

def is_node_responsible(entity_name, entity_type="topic"):
    """Verifica si el nodo actual es responsable del tópico o cola"""
    if entity_type == "topic":
        partition = get_partition_for_topic(entity_name)
    else:  # queue
        partition = get_partition_for_queue(entity_name)
    
    return partition["is_primary"] or partition["is_secondary"]

def get_responsible_nodes(entity_name, entity_type="topic"):
    """Obtiene los nodos responsables para un tópico o cola"""
    if entity_type == "topic":
        partition = get_partition_for_topic(entity_name)
    else:  # queue
        partition = get_partition_for_queue(entity_name)
    
    return [partition["primary"], partition["secondary"]]