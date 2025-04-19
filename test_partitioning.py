#!/usr/bin/env python
# test_partitioning.py

import requests
import json
import time
import sys
import random
import os
import logging
import threading
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MOM_Partitioning_Test")

# URLs de los nodos para pruebas
BASE_URLS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002"
]

# Colores para terminal
COLORS = {
    "GREEN": "\033[92m",
    "RED": "\033[91m",
    "YELLOW": "\033[93m",
    "BLUE": "\033[94m",
    "MAGENTA": "\033[95m",
    "CYAN": "\033[96m",
    "BOLD": "\033[1m",
    "UNDERLINE": "\033[4m",
    "END": "\033[0m"
}

# Estadísticas de pruebas
TEST_STATS = {
    "passed": 0,
    "failed": 0,
    "total": 0
}

def print_header(message):
    """Imprime un encabezado formateado con color"""
    print("\n" + "=" * 70)
    print(f"{COLORS['BOLD']}{COLORS['CYAN']} {message} {COLORS['END']}".center(80))
    print("=" * 70)

def print_step(message):
    """Imprime un paso de prueba formateado"""
    print(f"\n{COLORS['BLUE']}➡️ {message}{COLORS['END']}")

def print_result(success, message):
    """Imprime un resultado de prueba formateado con color"""
    TEST_STATS["total"] += 1
    
    if success:
        TEST_STATS["passed"] += 1
        print(f"{COLORS['GREEN']}✅ {message}{COLORS['END']}")
    else:
        TEST_STATS["failed"] += 1
        print(f"{COLORS['RED']}❌ {message}{COLORS['END']}")
    
    return success

def wait_for_replication(delay=2, message="Esperando a que se complete la replicación..."):
    """Espera un tiempo específico para la replicación"""
    print(f"{COLORS['YELLOW']}{message} ({delay}s){COLORS['END']}")
    time.sleep(delay)

def register_user(base_url, username, password):
    print_step(f"Registrando usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/register"
    data = {"username": username, "password": password}
    try:
        response = requests.post(url, json=data, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Registro de usuario '{username}' {'exitoso' if success else 'fallido'}")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return False

def login_user(base_url, username, password):
    print_step(f"Iniciando sesión con usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/login"
    data = {"username": username, "password": password}
    try:
        response = requests.post(url, json=data, timeout=5)
        print(f"Estado: {response.status_code}")
        
        if response.status_code == 200:
            token = response.json().get("token")
            print(f"Token obtenido: {token[:15]}...")
            print_result(True, f"Login exitoso para '{username}'")
            return token
        else:
            print(f"Error: {response.json()}")
            print_result(False, f"Login fallido para '{username}'")
            return None
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return None

def create_topic(base_url, token, topic_name, owner):
    print_step(f"Creando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics"
    data = {"name": topic_name, "owner": owner}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Creación de tópico '{topic_name}' {'exitosa' if success else 'fallida'}")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return False

def list_topics(base_url):
    print_step(f"Listando tópicos en {base_url}")
    url = f"{base_url}/messages/topics"
    try:
        response = requests.get(url, timeout=5)
        print(f"Estado: {response.status_code}")
        topics = response.json().get("topics", [])
        print(f"Tópicos: {topics}")
        print_result(response.status_code == 200, f"Listado de tópicos exitoso")
        return topics
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return []

def delete_topic(base_url, token, topic_name):
    print_step(f"Eliminando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics/{topic_name}"
    try:
        response = requests.delete(url, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Eliminación de tópico '{topic_name}' {'exitosa' if success else 'fallida'}")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return False

def test_partitioning():
    print_header("PRUEBA DE PARTICIONAMIENTO")
    
    # Generar ID de prueba único
    test_id = random.randint(1000, 9999)
    print(f"ID de prueba: {test_id}")
    
    # Crear usuario de prueba
    username = f"part_user_{test_id}"
    password = "pass123"
    
    # Registrar usuario y obtener token
    register_user(BASE_URLS[0], username, password)
    token = login_user(BASE_URLS[0], username, password)
    
    if not token:
        print_result(False, "No se pudo obtener token, cancelando pruebas")
        return
    
    # Crear varios tópicos para verificar distribución
    print_header("ANÁLISIS DE DISTRIBUCIÓN DE TÓPICOS")
    topics_created = {}
    node_distribution = {BASE_URLS[0]: 0, BASE_URLS[1]: 0, BASE_URLS[2]: 0}
    
    # Crear 15 tópicos aleatorios
    for i in range(15):
        topic_name = f"part_topic_{test_id}_{i}"
        create_topic(BASE_URLS[0], token, topic_name, username)
        topics_created[topic_name] = True
    
    # Esperar por replicación
    wait_for_replication(3, "Esperando replicación para analizar particionamiento...")
    
    # Verificar tópicos existentes en cada nodo
    for url in BASE_URLS:
        topics = list_topics(url)
        
        # Contar tópicos locales
        for topic in topics_created.keys():
            if topic in topics:
                node_distribution[url] += 1
    
    # Mostrar distribución
    print_header("RESULTADOS DE DISTRIBUCIÓN")
    for url, count in node_distribution.items():
        balance = (count / len(topics_created)) * 100
        print_result(
            balance > 0, 
            f"Nodo {url}: {count}/{len(topics_created)} tópicos ({balance:.1f}%)"
        )
    
    # Comprobar que cada tópico está al menos en un nodo
    all_topics_found = set()
    for url in BASE_URLS:
        topics = list_topics(url)
        for topic in topics:
            if topic in topics_created.keys():
                all_topics_found.add(topic)
    
    # Verificar que todos los tópicos creados están disponibles en algún nodo
    print_result(
        len(all_topics_found) == len(topics_created),
        f"Disponibilidad de tópicos: {len(all_topics_found)}/{len(topics_created)} tópicos detectados en el clúster"
    )
    
    # Limpieza: eliminar tópicos de prueba
    print_header("LIMPIEZA")
    for topic_name in topics_created.keys():
        try:
            delete_topic(BASE_URLS[0], token, topic_name)
        except:
            pass  # Ignorar errores durante la limpieza
    
    # Mostrar resumen
    print_header("RESUMEN DE PRUEBAS")
    total = TEST_STATS["total"]
    passed = TEST_STATS["passed"]
    failed = TEST_STATS["failed"]
    
    success_rate = (passed / total) * 100 if total > 0 else 0
    
    print(f"\n{COLORS['BOLD']}Resultados de particionamiento:{COLORS['END']}")
    print(f"  Total de pruebas: {total}")
    print(f"  Pruebas exitosas: {COLORS['GREEN']}{passed}{COLORS['END']} ({success_rate:.1f}%)")
    print(f"  Pruebas fallidas: {COLORS['RED']}{failed}{COLORS['END']} ({100-success_rate:.1f}%)")
    
    if failed == 0:
        print(f"\n{COLORS['GREEN']}{COLORS['BOLD']}✅ PARTICIONAMIENTO FUNCIONANDO CORRECTAMENTE{COLORS['END']}")
    else:
        print(f"\n{COLORS['RED']}{COLORS['BOLD']}❌ PROBLEMAS EN EL PARTICIONAMIENTO{COLORS['END']}")

if __name__ == "__main__":
    test_partitioning()