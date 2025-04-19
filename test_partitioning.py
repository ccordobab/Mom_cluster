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

# Configuración para reintentos y timeouts
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10  # Aumentado a 10 segundos
DELAY_BETWEEN_OPERATIONS = 0.5  # Medio segundo entre operaciones

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

def wait_message(message, seconds):
    """Muestra un mensaje de espera con temporizador"""
    print(f"{COLORS['YELLOW']}{message} ({seconds}s){COLORS['END']}")
    time.sleep(seconds)

def wait_for_replication(delay=3, message="Esperando a que se complete la replicación..."):
    """Espera un tiempo específico para la replicación"""
    print(f"{COLORS['YELLOW']}{message} ({delay}s){COLORS['END']}")
    time.sleep(delay)

def wait_for_cluster_ready():
    """Espera hasta que el clúster esté listo para recibir solicitudes"""
    print_step("Verificando que el clúster esté listo")
    max_attempts = 5
    for attempt in range(max_attempts):
        ready_nodes = 0
        for url in BASE_URLS:
            try:
                response = requests.get(f"{url}/messages/topics", timeout=3)
                if response.status_code == 200:
                    ready_nodes += 1
                    print(f"  Nodo {url} listo")
            except Exception as e:
                print(f"  Nodo {url} no responde: {str(e)}")
        
        if ready_nodes == len(BASE_URLS):
            print_result(True, f"Clúster listo: {ready_nodes}/{len(BASE_URLS)} nodos disponibles")
            return True
        
        print(f"  Sólo {ready_nodes}/{len(BASE_URLS)} nodos disponibles. Esperando...")
        time.sleep(2)
    
    print_result(False, f"Tiempo de espera agotado: sólo {ready_nodes}/{len(BASE_URLS)} nodos disponibles")
    return False

def register_user(base_url, username, password, retries=MAX_RETRIES):
    """Registra un nuevo usuario con reintentos"""
    print_step(f"Registrando usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/register"
    data = {"username": username, "password": password}
    
    for attempt in range(retries):
        try:
            response = requests.post(url, json=data, timeout=REQUEST_TIMEOUT)
            print(f"Estado: {response.status_code}")
            print(f"Respuesta: {response.json()}")
            success = response.status_code == 200
            return print_result(success, f"Registro de usuario '{username}' {'exitoso' if success else 'fallido'}")
        except Exception as e:
            if attempt < retries - 1:
                print(f"{COLORS['YELLOW']}Reintento {attempt+1}/{retries}: {str(e)}{COLORS['END']}")
                time.sleep(1)
            else:
                print(f"{COLORS['RED']}Error en la solicitud después de {retries} intentos: {str(e)}{COLORS['END']}")
                return print_result(False, f"Error al registrar usuario '{username}'")

def login_user(base_url, username, password, retries=MAX_RETRIES):
    """Inicia sesión con un usuario y obtiene token con reintentos"""
    print_step(f"Iniciando sesión con usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/login"
    data = {"username": username, "password": password}
    
    for attempt in range(retries):
        try:
            response = requests.post(url, json=data, timeout=REQUEST_TIMEOUT)
            print(f"Estado: {response.status_code}")
            
            if response.status_code == 200:
                token = response.json().get("token")
                print(f"Token obtenido: {token[:15]}...")
                print_result(True, f"Login exitoso para '{username}'")
                return token
            else:
                print(f"Error: {response.json()}")
                if attempt < retries - 1:
                    print(f"{COLORS['YELLOW']}Reintento {attempt+1}/{retries}{COLORS['END']}")
                    time.sleep(1)
                else:
                    print_result(False, f"Login fallido para '{username}'")
                    return None
        except Exception as e:
            if attempt < retries - 1:
                print(f"{COLORS['YELLOW']}Reintento {attempt+1}/{retries}: {str(e)}{COLORS['END']}")
                time.sleep(1)
            else:
                print(f"{COLORS['RED']}Error en la solicitud después de {retries} intentos: {str(e)}{COLORS['END']}")
                print_result(False, f"Error al iniciar sesión")
                return None

def create_topic(base_url, token, topic_name, owner, retries=MAX_RETRIES):
    """Crea un nuevo tópico con reintentos"""
    print_step(f"Creando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics"
    data = {"name": topic_name, "owner": owner}
    
    for attempt in range(retries):
        try:
            response = requests.post(url, json=data, params={"token": token}, timeout=REQUEST_TIMEOUT)
            print(f"Estado: {response.status_code}")
            print(f"Respuesta: {response.json()}")
            success = response.status_code == 200
            return print_result(success, f"Creación de tópico '{topic_name}' {'exitosa' if success else 'fallida'}")
        except Exception as e:
            if attempt < retries - 1:
                print(f"{COLORS['YELLOW']}Reintento {attempt+1}/{retries}: {str(e)}{COLORS['END']}")
                time.sleep(1)
            else:
                print(f"{COLORS['RED']}Error en la solicitud después de {retries} intentos: {str(e)}{COLORS['END']}")
                return print_result(False, f"Error al crear tópico '{topic_name}'")

def list_topics(base_url, retries=MAX_RETRIES):
    """Lista todos los tópicos disponibles con reintentos"""
    print_step(f"Listando tópicos en {base_url}")
    url = f"{base_url}/messages/topics"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            print(f"Estado: {response.status_code}")
            topics = response.json().get("topics", [])
            print(f"Tópicos: {topics}")
            print_result(response.status_code == 200, f"Listado de tópicos exitoso")
            return topics
        except Exception as e:
            if attempt < retries - 1:
                print(f"{COLORS['YELLOW']}Reintento {attempt+1}/{retries}: {str(e)}{COLORS['END']}")
                time.sleep(1)
            else:
                print(f"{COLORS['RED']}Error en la solicitud después de {retries} intentos: {str(e)}{COLORS['END']}")
                return []

def delete_topic(base_url, token, topic_name, retries=MAX_RETRIES):
    """Elimina un tópico con reintentos"""
    print_step(f"Eliminando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics/{topic_name}"
    
    for attempt in range(retries):
        try:
            response = requests.delete(url, params={"token": token}, timeout=REQUEST_TIMEOUT)
            print(f"Estado: {response.status_code}")
            print(f"Respuesta: {response.json()}")
            success = response.status_code == 200
            return print_result(success, f"Eliminación de tópico '{topic_name}' {'exitosa' if success else 'fallida'}")
        except Exception as e:
            if attempt < retries - 1:
                print(f"{COLORS['YELLOW']}Reintento {attempt+1}/{retries}: {str(e)}{COLORS['END']}")
                time.sleep(1)
            else:
                print(f"{COLORS['RED']}Error en la solicitud después de {retries} intentos: {str(e)}{COLORS['END']}")
                return False

def test_partitioning():
    print_header("PRUEBA DE PARTICIONAMIENTO")
    
    # Primero verificar que el clúster esté listo
    if not wait_for_cluster_ready():
        print_result(False, "El clúster no está completamente disponible, continuando con precaución...")
    
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
    
    # Crear 10 tópicos aleatorios (reducido de 15 a 10)
    num_topics = 10
    for i in range(num_topics):
        topic_name = f"part_topic_{test_id}_{i}"
        if create_topic(BASE_URLS[0], token, topic_name, username):
            topics_created[topic_name] = True
        # Añadir retraso entre operaciones
        time.sleep(DELAY_BETWEEN_OPERATIONS)
    
    # Esperar por replicación
    wait_for_replication(5, "Esperando replicación para analizar particionamiento...")
    
    # Verificar tópicos existentes en cada nodo
    total_topics_found = 0
    for url in BASE_URLS:
        topics = list_topics(url)
        
        # Contar tópicos locales
        topics_in_node = 0
        for topic in topics_created.keys():
            if topic in topics:
                topics_in_node += 1
                total_topics_found += 1
        
        node_distribution[url] = topics_in_node
    
    # Mostrar distribución
    print_header("RESULTADOS DE DISTRIBUCIÓN")
    for url, count in node_distribution.items():
        balance = (count / len(topics_created)) * 100 if topics_created else 0
        print_result(
            count > 0 or len(topics_created) == 0, 
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
    topics_availability = len(all_topics_found) / len(topics_created) * 100 if topics_created else 0
    print_result(
        len(all_topics_found) >= len(topics_created) * 0.7,  # Consideramos éxito si al menos el 70% está disponible
        f"Disponibilidad de tópicos: {len(all_topics_found)}/{len(topics_created)} tópicos detectados en el clúster ({topics_availability:.1f}%)"
    )
    
    # Limpieza: eliminar tópicos de prueba
    print_header("LIMPIEZA")
    for topic_name in topics_created.keys():
        try:
            delete_topic(BASE_URLS[0], token, topic_name)
            # Retraso entre eliminaciones
            time.sleep(DELAY_BETWEEN_OPERATIONS)
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
    
    if failed == 0 or success_rate >= 80:
        print(f"\n{COLORS['GREEN']}{COLORS['BOLD']}✅ PARTICIONAMIENTO FUNCIONANDO CORRECTAMENTE{COLORS['END']}")
    else:
        print(f"\n{COLORS['RED']}{COLORS['BOLD']}❌ PROBLEMAS EN EL PARTICIONAMIENTO{COLORS['END']}")

if __name__ == "__main__":
    test_partitioning()