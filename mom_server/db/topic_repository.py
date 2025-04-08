# mom_server/db/topic_repository.py

import logging
from mom_server.database import get_connection

logger = logging.getLogger(__name__)

def get_topics():
    """
    Obtiene todos los tópicos desde la base de datos.
    
    Returns:
        dict: Diccionario con todos los tópicos y sus mensajes
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM topics")
    rows = cursor.fetchall()
    topics = {}
    for row in rows:
        topics[row["name"]] = {"owner": row["owner"], "messages": get_topic_messages(row["name"])}
    conn.close()
    return topics

def get_topic_messages(topic_name):
    """
    Obtiene todos los mensajes de un tópico específico.
    
    Args:
        topic_name (str): Nombre del tópico
        
    Returns:
        list: Lista de mensajes del tópico
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sender, content, timestamp FROM topic_messages WHERE topic_name = ?", (topic_name,))
    messages = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return messages

def create_topic(topic_name, owner):
    """
    Crea un nuevo tópico en la base de datos.
    
    Args:
        topic_name (str): Nombre del tópico a crear
        owner (str): Propietario del tópico
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO topics (name, owner) VALUES (?, ?)", (topic_name, owner))
        conn.commit()
    except Exception as e:
        conn.close()
        raise e
    conn.close()

def delete_topic(topic_name):
    """
    Elimina un tópico y sus mensajes asociados.
    
    Args:
        topic_name (str): Nombre del tópico a eliminar
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM topics WHERE name = ?", (topic_name,))
    # También borramos los mensajes asociados
    cursor.execute("DELETE FROM topic_messages WHERE topic_name = ?", (topic_name,))
    conn.commit()
    conn.close()

def add_topic_message(topic_name, sender, content):
    """
    Añade un mensaje a un tópico existente.
    
    Args:
        topic_name (str): Nombre del tópico
        sender (str): Remitente del mensaje
        content (str): Contenido del mensaje
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO topic_messages (topic_name, sender, content) VALUES (?, ?, ?)",
                   (topic_name, sender, content))
    conn.commit()
    conn.close()