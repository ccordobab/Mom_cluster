# mom_server/database.py
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'mom_state.db')

def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    
        
    # Crear tabla para usuarios
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Crear tabla para tópicos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS topics (
        name TEXT PRIMARY KEY,
        owner TEXT NOT NULL
    )
    """)
    
    # Crear tabla para mensajes de tópicos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS topic_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic_name TEXT NOT NULL,
        sender TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (topic_name) REFERENCES topics(name)
    )
    """)
    
    # Crear tabla para colas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS queues (
        name TEXT PRIMARY KEY,
        owner TEXT NOT NULL
    )
    """)
    
    # Crear tabla para mensajes de colas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS queue_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue_name TEXT NOT NULL,
        sender TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (queue_name) REFERENCES queues(name)
    )
    """)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    print("Base de datos inicializada en", DB_PATH)
