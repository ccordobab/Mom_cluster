#!/usr/bin/env python
# test_mom_persistency.py - Prueba de persistencia de datos en el sistema MOM

import requests
import json
import time
import sys
import random
import os
import logging
import subprocess
import signal
import threading
import argparse
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("mom_persistency_tests.log")
    ]
)
logger = logging.getLogger("MOM_Persistency_Tests")

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

# Lista para mantener referencia a los procesos
cluster_processes = []

def color_text(text, color):
    """Añade color al texto para terminal"""
    return f"{COLORS.get(color, '')}{text}{COLORS['END']}"

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
    if success:
        print(f"{COLORS['GREEN']}✅ {message}{COLORS['END']}")
    else:
        print(f"{COLORS['RED']}❌ {message}{COLORS['END']}")
    return success

def wait_message(message, seconds):
    """Muestra un mensaje de espera con temporizador"""
    print(f"{COLORS['YELLOW']}{message} ({seconds}s){COLORS['END']}")
    time.sleep(seconds)

def register_user(base_url, username, password):
    """Registra un nuevo usuario"""
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
    """Inicia sesión con un usuario y obtiene token"""
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
    """Crea un nuevo tópico"""
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
    """Lista todos los tópicos disponibles"""
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

def send_message(base_url, token, topic_name, sender, content):
    """Envía un mensaje a un tópico"""
    print_step(f"Enviando mensaje al tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/messages/topic/{topic_name}"
    data = {"sender": sender, "content": content}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Envío de mensaje a '{topic_name}' {'exitoso' if success else 'fallido'}")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return False

def get_messages(base_url, topic_name):
    """Obtiene mensajes de un tópico"""
    print_step(f"Obteniendo mensajes del tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/messages/topic/{topic_name}"
    try:
        response = requests.get(url, timeout=5)
        print(f"Estado: {response.status_code}")
        messages = response.json().get("messages", [])
        print(f"Mensajes: {messages}")
        print_result(response.status_code == 200, f"Obtención de mensajes de '{topic_name}' exitosa")
        return messages
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return []

def create_queue(base_url, token, queue_name, owner):
    """Crea una nueva cola"""
    print_step(f"Creando cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/queues"
    data = {"name": queue_name, "owner": owner}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Creación de cola '{queue_name}' {'exitosa' if success else 'fallida'}")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return False

def list_queues(base_url):
    """Lista todas las colas disponibles"""
    print_step(f"Listando colas en {base_url}")
    url = f"{base_url}/messages/queues"
    try:
        response = requests.get(url, timeout=5)
        print(f"Estado: {response.status_code}")
        queues = response.json().get("queues", [])
        print(f"Colas: {queues}")
        print_result(response.status_code == 200, f"Listado de colas exitoso")
        return queues
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return []

def send_queue_message(base_url, token, queue_name, sender, content):
    """Envía un mensaje a una cola"""
    print_step(f"Enviando mensaje a la cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/messages/queue/{queue_name}"
    data = {"sender": sender, "content": content}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Envío de mensaje a cola '{queue_name}' {'exitoso' if success else 'fallido'}")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return False

def get_queue_message(base_url, token, queue_name):
    """Obtiene y consume un mensaje de una cola"""
    print_step(f"Consumiendo mensaje de la cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/messages/queue/{queue_name}"
    try:
        response = requests.get(url, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        print_result(response.status_code == 200, f"Consumo de mensaje de cola '{queue_name}' exitoso")
        return response.json().get("message")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return None

def verify_topic_exists(base_url, topic_name):
    """Verifica si un tópico existe"""
    topics = list_topics(base_url)
    exists = topic_name in topics
    result = print_result(exists, f"Verificación de existencia del tópico '{topic_name}': {exists}")
    return result

def verify_queue_exists(base_url, queue_name):
    """Verifica si una cola existe"""
    queues = list_queues(base_url)
    exists = queue_name in queues
    result = print_result(exists, f"Verificación de existencia de la cola '{queue_name}': {exists}")
    return result

def verify_message_content(messages, expected_content):
    """Verifica si un mensaje con el contenido esperado existe en la lista de mensajes"""
    for msg in messages:
        if msg.get('content') == expected_content:
            return print_result(True, f"Mensaje con contenido '{expected_content}' encontrado")
    return print_result(False, f"Mensaje con contenido '{expected_content}' NO encontrado")

def start_cluster():
    """Inicia el clúster MOM usando subprocess"""
    print_step("Iniciando clúster MOM...")
    # Asegurarse de que no hay procesos existentes
    stop_cluster()
    
    # Iniciar el clúster
    cmd = [sys.executable, "launch_cluster.py"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cluster_processes.append(process)
    
    # Esperar a que el clúster esté listo
    wait_message("Esperando a que el clúster se inicie...", 10)
    
    # Verificar que el clúster está respondiendo
    for url in BASE_URLS:
        try:
            response = requests.get(f"{url}/messages/topics", timeout=2)
            print_result(response.status_code == 200, f"Nodo {url} respondiendo correctamente")
        except Exception as e:
            print_result(False, f"Error al conectar con nodo {url}: {str(e)}")
    
    return True

def stop_cluster():
    """Detiene el clúster MOM"""
    print_step("Deteniendo clúster MOM...")
    for process in cluster_processes:
        if process.poll() is None:  # Si el proceso sigue en ejecución
            process.terminate()
            try:
                process.wait(timeout=5)
                print_result(True, f"Proceso terminado correctamente")
            except subprocess.TimeoutExpired:
                print_result(False, f"Tiempo de espera agotado al terminar proceso")
                process.kill()
    
    # Limpiar la lista de procesos
    cluster_processes.clear()
    
    # Esperar a que los puertos se liberen
    wait_message("Esperando a que los puertos se liberen...", 5)
    return True

def run_persistence_test():
    """Ejecuta prueba de persistencia"""
    print_header("PRUEBA DE PERSISTENCIA DE DATOS MOM")
    
    # Generar ID de prueba único
    test_id = random.randint(1000, 9999)
    print(f"ID de prueba: {test_id}")
    
    # Nombres para los recursos
    username = f"pers_user_{test_id}"
    password = "pass123"
    topic_name = f"pers_topic_{test_id}"
    queue_name = f"pers_queue_{test_id}"
    message_content = f"Mensaje persistente {test_id}"
    
    # FASE 1: Crear recursos
    print_header("FASE 1: CREACIÓN DE RECURSOS")
    
    # Iniciar el clúster
    start_cluster()
    
    # Crear usuario y obtener token
    register_user(BASE_URLS[0], username, password)
    token = login_user(BASE_URLS[0], username, password)
    
    if not token:
        print_result(False, "No se pudo obtener token, deteniendo prueba")
        stop_cluster()
        return
    
    # Crear tópico y cola
    create_topic(BASE_URLS[0], token, topic_name, username)
    create_queue(BASE_URLS[0], token, queue_name, username)
    
    # Esperar por replicación
    wait_message("Esperando por replicación...", 3)
    
    # Verificar que se crearon
    topics_created = verify_topic_exists(BASE_URLS[0], topic_name)
    queues_created = verify_queue_exists(BASE_URLS[0], queue_name)
    
    if not topics_created or not queues_created:
        print_result(False, "Recursos no creados correctamente, deteniendo prueba")
        stop_cluster()
        return
    
    # Enviar mensaje al tópico
    send_message(BASE_URLS[0], token, topic_name, username, message_content)
    
    # Esperar por replicación
    wait_message("Esperando por replicación...", 3)
    
    # Verificar mensaje en tópico
    messages = get_messages(BASE_URLS[0], topic_name)
    message_sent = verify_message_content(messages, message_content)
    
    if not message_sent:
        print_result(False, "Mensaje no enviado correctamente, deteniendo prueba")
        stop_cluster()
        return
    
    # Enviar mensaje a la cola
    queue_message = f"Mensaje de cola {test_id}"
    send_queue_message(BASE_URLS[0], token, queue_name, username, queue_message)
    
    # FASE 2: Reiniciar el clúster y verificar persistencia
    print_header("FASE 2: REINICIO DEL CLÚSTER")
    
    # Detener el clúster
    stop_cluster()
    print_result(True, "Clúster detenido correctamente")
    
    # Iniciar el clúster de nuevo
    start_cluster()
    print_result(True, "Clúster reiniciado correctamente")
    
    # Volver a iniciar sesión
    new_token = login_user(BASE_URLS[0], username, password)
    
    if not new_token:
        print_result(False, "No se pudo obtener token después del reinicio, deteniendo prueba")
        stop_cluster()
        return
    
    # FASE 3: Verificar que los datos persisten
    print_header("FASE 3: VERIFICACIÓN DE PERSISTENCIA")
    
    # Verificar que el tópico sigue existiendo
    topic_persisted = verify_topic_exists(BASE_URLS[0], topic_name)
    
    # Verificar que la cola sigue existiendo
    queue_persisted = verify_queue_exists(BASE_URLS[0], queue_name)
    
    # Verificar que el mensaje del tópico persiste
    if topic_persisted:
        messages_after_restart = get_messages(BASE_URLS[0], topic_name)
        message_persisted = verify_message_content(messages_after_restart, message_content)
    else:
        message_persisted = False
        print_result(False, "No se pudo verificar persistencia de mensajes porque el tópico no existe")
    
    # Verificar que el mensaje de la cola persiste
    if queue_persisted:
        queue_message_after_restart = get_queue_message(BASE_URLS[0], new_token, queue_name)
        if queue_message_after_restart and queue_message_after_restart.get('content') == queue_message:
            queue_message_persisted = print_result(True, f"Mensaje de cola '{queue_message}' persistido correctamente")
        else:
            queue_message_persisted = print_result(False, f"Mensaje de cola no persistido correctamente")
    else:
        queue_message_persisted = False
        print_result(False, "No se pudo verificar persistencia de mensajes de cola porque la cola no existe")
    
    # Detener el clúster al finalizar
    stop_cluster()
    
    # Resumen final
    print_header("RESUMEN DE PRUEBA DE PERSISTENCIA")
    print(f"{COLORS['BOLD']}Resultados:{COLORS['END']}")
    print(f"  Usuario persistido: {COLORS['GREEN' if new_token else 'RED']}{'✓' if new_token else '✗'}{COLORS['END']}")
    print(f"  Tópico persistido: {COLORS['GREEN' if topic_persisted else 'RED']}{'✓' if topic_persisted else '✗'}{COLORS['END']}")
    print(f"  Cola persistida: {COLORS['GREEN' if queue_persisted else 'RED']}{'✓' if queue_persisted else '✗'}{COLORS['END']}")
    print(f"  Mensaje de tópico persistido: {COLORS['GREEN' if message_persisted else 'RED']}{'✓' if message_persisted else '✗'}{COLORS['END']}")
    print(f"  Mensaje de cola persistido: {COLORS['GREEN' if queue_message_persisted else 'RED']}{'✓' if queue_message_persisted else '✗'}{COLORS['END']}")
    
    # Conclusión
    all_tests_passed = new_token and topic_persisted and queue_persisted and message_persisted and queue_message_persisted
    
    if all_tests_passed:
        print(f"\n{COLORS['GREEN']}{COLORS['BOLD']}✅ PRUEBA DE PERSISTENCIA EXITOSA - Todos los datos persistieron correctamente{COLORS['END']}")
    else:
        print(f"\n{COLORS['RED']}{COLORS['BOLD']}❌ PRUEBA DE PERSISTENCIA FALLIDA - Algunos datos no persistieron{COLORS['END']}")
    
    return all_tests_passed

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pruebas de persistencia para el MOM Cluster')
    parser.add_argument('--skip-cluster-management', action='store_true', 
                        help='No iniciar/detener el clúster (útil si ya está en ejecución)')
    
    args = parser.parse_args()
    
    if args.skip_cluster_management:
        print_header("EJECUTANDO PRUEBAS SIN GESTIÓN DEL CLÚSTER")
        print("Asegúrate de que el clúster ya está en ejecución")
        
        def start_cluster():
            print_step("Omitiendo inicio del clúster (--skip-cluster-management)")
            return True
            
        def stop_cluster():
            print_step("Omitiendo detención del clúster (--skip-cluster-management)")
            return True
    
    try:
        run_persistence_test()
    except KeyboardInterrupt:
        print("\nPrueba interrumpida por el usuario")
        stop_cluster()
    except Exception as e:
        print(f"{COLORS['RED']}Error durante la prueba: {str(e)}{COLORS['END']}")
        stop_cluster()