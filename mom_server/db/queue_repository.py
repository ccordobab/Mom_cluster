# mom_server/db/queue_repository.py

import logging
from mom_server.database import get_connection

logger = logging.getLogger(__name__)

def get_queues():
    """
    Obtiene todas las colas desde la base de datos.
    
    Returns:
        dict: Diccionario con todas las colas y sus mensajes
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM queues")
    rows = cursor.fetchall()
    queues = {}
    for row in rows:
        queues[row["name"]] = {"owner": row["owner"], "messages": get_queue_messages(row["name"])}
    conn.close()
    return queues

def get_queue_messages(queue_name):
    """
    Obtiene todos los mensajes de una cola específica.
    
    Args:
        queue_name (str): Nombre de la cola
        
    Returns:
        list: Lista de mensajes de la cola
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sender, content, timestamp FROM queue_messages WHERE queue_name = ?", (queue_name,))
    messages = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return messages

def create_queue(queue_name, owner):
    """
    Crea una nueva cola en la base de datos.
    
    Args:
        queue_name (str): Nombre de la cola a crear
        owner (str): Propietario de la cola
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO queues (name, owner) VALUES (?, ?)", (queue_name, owner))
        conn.commit()
    except Exception as e:
        conn.close()
        raise e
    conn.close()

def delete_queue(queue_name):
    """
    Elimina una cola y sus mensajes asociados.
    
    Args:
        queue_name (str): Nombre de la cola a eliminar
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM queues WHERE name = ?", (queue_name,))
    cursor.execute("DELETE FROM queue_messages WHERE queue_name = ?", (queue_name,))
    conn.commit()
    conn.close()

def add_queue_message(queue_name, sender, content):
    """
    Añade un mensaje a una cola existente.
    
    Args:
        queue_name (str): Nombre de la cola
        sender (str): Remitente del mensaje
        content (str): Contenido del mensaje
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO queue_messages (queue_name, sender, content) VALUES (?, ?, ?)",
                   (queue_name, sender, content))
    conn.commit()
    conn.close()

def consume_queue_message(queue_name):
    """
    Consume (lee y elimina) el primer mensaje de una cola de forma atómica.
    
    Args:
        queue_name (str): Nombre de la cola
        
    Returns:
        dict: Mensaje consumido o None si la cola está vacía
    """
    conn = get_connection()
    conn.isolation_level = None  # Habilita el modo de autocommit
    cursor = conn.cursor()
    
    try:
        # Iniciar transacción explícitamente
        cursor.execute("BEGIN IMMEDIATE")
        
        # Obtenemos el primer mensaje (el más antiguo)
        cursor.execute("""
            SELECT id, sender, content, timestamp 
            FROM queue_messages 
            WHERE queue_name = ? 
            ORDER BY timestamp ASC
            LIMIT 1
        """, (queue_name,))
        
        msg = cursor.fetchone()
        if not msg:
            cursor.execute("ROLLBACK")
            conn.close()
            return None
        
        # Guardamos el ID del mensaje a eliminar
        msg_id = msg['id']
        
        # Formateamos el mensaje para devolverlo
        message = {
            "sender": msg['sender'],
            "content": msg['content'],
            "timestamp": msg['timestamp']
        }
        
        # Eliminamos el mensaje recuperado
        cursor.execute("DELETE FROM queue_messages WHERE id = ?", (msg_id,))
        
        # Confirmamos la transacción explícitamente
        cursor.execute("COMMIT")
        
        # Registramos la operación para depuración
        logger.info(f"Mensaje con ID {msg_id} consumido exitosamente de la cola {queue_name}")
        
        conn.close()
        return message
    except Exception as e:
        # En caso de error, hacemos rollback
        try:
            cursor.execute("ROLLBACK")
        except:
            pass
        logger.error(f"Error al consumir mensaje de cola {queue_name}: {str(e)}")
        conn.close()
        return None