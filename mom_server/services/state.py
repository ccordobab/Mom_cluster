# mom_server/services/state.py

"""
Este módulo proporciona funciones de estado para mantener compatibilidad con el código existente.
Actúa como una capa de adaptación que reenvía las llamadas a los repositorios correspondientes.
"""

import logging
# Importar funciones desde los repositorios
from mom_server.db.topic_repository import (
    get_topics, get_topic_messages, create_topic, delete_topic, add_topic_message
)
from mom_server.db.queue_repository import (
    get_queues, get_queue_messages, create_queue, delete_queue, add_queue_message, 
    consume_queue_message
)
from mom_server.db.user_repository import (
    get_user, create_user, verify_password, get_all_users
)

logger = logging.getLogger(__name__)

def update_state(entity_type=None):
    """
    Función de compatibilidad que no realiza ninguna acción real.
    Mantenida para evitar romper código existente.
    
    Args:
        entity_type (str, optional): Tipo de entidad a actualizar (no usado)
    
    Returns:
        bool: True siempre
    """
    logger.info(f"Actualizando estado en DB: {entity_type or 'todos'}")
    return True

# No necesitamos reimplementar las funciones ya que las importamos directamente