# mom_server/user_repository.py

import logging
from mom_server.database import get_connection

logger = logging.getLogger(__name__)

def get_user(username):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT username, password FROM users WHERE username = ?", (username,))
    user = cursor.fetchone()
    conn.close()
    
    if user:
        return dict(user)
    return None

def create_user(username, password):
    conn = get_connection()
    cursor = conn.cursor()
    
    # Almacenando la contraseña sin hashear (NO RECOMENDADO)
    try:
        cursor.execute("INSERT INTO users (username, password) VALUES (?, ?)", 
                      (username, password))
        conn.commit()
    except Exception as e:
        conn.close()
        logger.error(f"Error al crear usuario {username}: {str(e)}")
        raise e
    
    conn.close()
    return {"username": username}

def verify_password(stored_password, provided_password):
    # Comparación directa sin hashing
    return stored_password == provided_password

def get_all_users():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM users")
    users = [row['username'] for row in cursor.fetchall()]
    conn.close()
    return users