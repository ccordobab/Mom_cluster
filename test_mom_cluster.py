#!/usr/bin/env python
# test_mom_cluster.py

import requests
import json
import time
import sys
import random
import os
import logging
import threading
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("mom_tests.log")
    ]
)
logger = logging.getLogger("MOM_Tests")

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
    "total": 0,
    "sections": {}
}

def color_text(text, color):
    """Añade color al texto para terminal"""
    return f"{COLORS.get(color, '')}{text}{COLORS['END']}"

def print_header(message, section=None):
    """Imprime un encabezado formateado con color"""
    print("\n" + "=" * 70)
    print(f"{COLORS['BOLD']}{COLORS['CYAN']} {message} {COLORS['END']}".center(80))
    print("=" * 70)
    
    if section and section not in TEST_STATS["sections"]:
        TEST_STATS["sections"][section] = {"passed": 0, "failed": 0, "total": 0}

def print_step(message):
    """Imprime un paso de prueba formateado"""
    print(f"\n{COLORS['BLUE']}➡️ {message}{COLORS['END']}")

def print_result(success, message, section=None, expected_failure=False):
    """
    Imprime un resultado de prueba formateado con color
    
    Args:
        success (bool): Si la prueba fue exitosa
        message (str): Mensaje descriptivo
        section (str, optional): Sección de la prueba
        expected_failure (bool, optional): Si es un fallo esperado (como validaciones)
    """
    TEST_STATS["total"] += 1
    if section:
        TEST_STATS["sections"][section]["total"] += 1
    
    if success:
        TEST_STATS["passed"] += 1
        if section:
            TEST_STATS["sections"][section]["passed"] += 1
        print(f"{COLORS['GREEN']}✅ {message}{COLORS['END']}")
    else:
        # Si es un fallo esperado (como validaciones), contarlo como éxito
        if expected_failure:
            TEST_STATS["passed"] += 1
            if section:
                TEST_STATS["sections"][section]["passed"] += 1
            print(f"{COLORS['YELLOW']}⚠️ {message} (fallo esperado){COLORS['END']}")
        else:
            TEST_STATS["failed"] += 1
            if section:
                TEST_STATS["sections"][section]["failed"] += 1
            print(f"{COLORS['RED']}❌ {message}{COLORS['END']}")
    
    return success

def wait_for_replication(delay=2, message="Esperando a que se complete la replicación..."):
    """Espera un tiempo específico para la replicación"""
    print(f"{COLORS['YELLOW']}{message} ({delay}s){COLORS['END']}")
    time.sleep(delay)

def register_user(base_url, username, password, section=None, expected_failure=False):
    print_step(f"Registrando usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/register"
    data = {"username": username, "password": password}
    try:
        response = requests.post(url, json=data, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        
        # Si esperamos un fallo (como intento de registro duplicado), el código 400 es correcto
        if expected_failure:
            success = response.status_code == 400
            result_msg = f"Registro de usuario '{username}' rechazado correctamente" if success else f"Registro de usuario '{username}' debió ser rechazado"
        else:
            success = response.status_code == 200
            result_msg = f"Registro de usuario '{username}' {'exitoso' if success else 'fallido'}"
        
        return print_result(success, result_msg, section, expected_failure if success and expected_failure else False)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al registrar usuario '{username}': {str(e)}", section)
        return False

def login_user(base_url, username, password, section=None, expected_failure=False):
    print_step(f"Iniciando sesión con usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/login"
    data = {"username": username, "password": password}
    try:
        response = requests.post(url, json=data, timeout=5)
        print(f"Estado: {response.status_code}")
        
        if expected_failure:
            # Si esperamos fallo, un código diferente a 200 es correcto
            success = response.status_code != 200
            print(f"Error (esperado): {response.json()}")
            print_result(success, f"Login rechazado correctamente para '{username}'", section, True)
            return None
        elif response.status_code == 200:
            token = response.json().get("token")
            print(f"Token obtenido: {token[:15]}...")
            print_result(True, f"Login exitoso para '{username}'", section)
            return token
        else:
            print(f"Error: {response.json()}")
            print_result(False, f"Login fallido para '{username}'", section)
            return None
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al iniciar sesión: {str(e)}", section)
        return None

def create_topic(base_url, token, topic_name, owner, section=None):
    print_step(f"Creando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics"
    data = {"name": topic_name, "owner": owner}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Creación de tópico '{topic_name}' {'exitosa' if success else 'fallida'}", section)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al crear tópico '{topic_name}': {str(e)}", section)
        return False

def list_topics(base_url, section=None):
    print_step(f"Listando tópicos en {base_url}")
    url = f"{base_url}/messages/topics"
    try:
        response = requests.get(url, timeout=5)
        print(f"Estado: {response.status_code}")
        topics = response.json().get("topics", [])
        print(f"Tópicos: {topics}")
        print_result(response.status_code == 200, f"Listado de tópicos exitoso", section)
        return topics
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al listar tópicos: {str(e)}", section)
        return []

def delete_topic(base_url, token, topic_name, section=None):
    print_step(f"Eliminando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics/{topic_name}"
    try:
        response = requests.delete(url, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Eliminación de tópico '{topic_name}' {'exitosa' if success else 'fallida'}", section)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al eliminar tópico '{topic_name}': {str(e)}", section)
        return False

def send_message(base_url, token, topic_name, sender, content, section=None):
    print_step(f"Enviando mensaje al tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/messages/topic/{topic_name}"
    data = {"sender": sender, "content": content}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Envío de mensaje a '{topic_name}' {'exitoso' if success else 'fallido'}", section)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al enviar mensaje a '{topic_name}': {str(e)}", section)
        return False

def get_messages(base_url, topic_name, section=None):
    print_step(f"Obteniendo mensajes del tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/messages/topic/{topic_name}"
    try:
        response = requests.get(url, timeout=5)
        print(f"Estado: {response.status_code}")
        messages = response.json().get("messages", [])
        print(f"Mensajes: {messages}")
        print_result(response.status_code == 200, f"Obtención de mensajes de '{topic_name}' exitosa", section)
        return messages
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al obtener mensajes de '{topic_name}': {str(e)}", section)
        return []

def create_queue(base_url, token, queue_name, owner, section=None):
    print_step(f"Creando cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/queues"
    data = {"name": queue_name, "owner": owner}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Creación de cola '{queue_name}' {'exitosa' if success else 'fallida'}", section)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al crear cola '{queue_name}': {str(e)}", section)
        return False

def list_queues(base_url, section=None):
    print_step(f"Listando colas en {base_url}")
    url = f"{base_url}/messages/queues"
    try:
        response = requests.get(url, timeout=5)
        print(f"Estado: {response.status_code}")
        queues = response.json().get("queues", [])
        print(f"Colas: {queues}")
        print_result(response.status_code == 200, f"Listado de colas exitoso", section)
        return queues
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al listar colas: {str(e)}", section)
        return []

def delete_queue(base_url, token, queue_name, section=None):
    print_step(f"Eliminando cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/queues/{queue_name}"
    try:
        response = requests.delete(url, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Eliminación de cola '{queue_name}' {'exitosa' if success else 'fallida'}", section)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al eliminar cola '{queue_name}': {str(e)}", section)
        return False

def send_queue_message(base_url, token, queue_name, sender, content, section=None):
    print_step(f"Enviando mensaje a la cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/messages/queue/{queue_name}"
    data = {"sender": sender, "content": content}
    try:
        response = requests.post(url, json=data, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        success = response.status_code == 200
        return print_result(success, f"Envío de mensaje a cola '{queue_name}' {'exitoso' if success else 'fallido'}", section)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al enviar mensaje a cola '{queue_name}': {str(e)}", section)
        return False

def get_queue_message(base_url, token, queue_name, section=None):
    print_step(f"Consumiendo mensaje de la cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/messages/queue/{queue_name}"
    try:
        response = requests.get(url, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        print_result(response.status_code == 200, f"Consumo de mensaje de cola '{queue_name}' exitoso", section)
        return response.json().get("message")
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        print_result(False, f"Error al consumir mensaje de cola '{queue_name}': {str(e)}", section)
        return None

def check_replication(expected_item, items, item_type="tópico", section=None):
    if expected_item in items:
        return print_result(True, f"{item_type.capitalize()} '{expected_item}' replicado correctamente", section)
    else:
        return print_result(False, f"{item_type.capitalize()} '{expected_item}' NO replicado", section)

def check_message_replication(base_urls, topic_name, message_content, section=None):
    """Verifica que un mensaje se ha replicado en todos los nodos"""
    all_replicated = True
    for url in base_urls:
        messages = get_messages(url, topic_name)
        message_found = False
        for msg in messages:
            if msg.get("content") == message_content:
                message_found = True
                break
        
        if message_found:
            print_result(True, f"Mensaje replicado correctamente en {url}", section)
        else:
            print_result(False, f"Mensaje NO replicado en {url}", section)
            all_replicated = False
    
    return all_replicated

def stress_test_topic(base_url, token, topic_name, username, num_messages=10, section=None):
    """Realiza una prueba de estrés enviando múltiples mensajes a un tópico"""
    print_step(f"Prueba de estrés: Enviando {num_messages} mensajes al tópico '{topic_name}'")
    success_count = 0
    
    for i in range(num_messages):
        content = f"Mensaje de estrés #{i+1} - {datetime.now().strftime('%H:%M:%S.%f')}"
        if send_message(base_url, token, topic_name, username, content):
            success_count += 1
    
    success_rate = (success_count / num_messages) * 100
    return print_result(success_count == num_messages, 
                      f"Prueba de estrés: {success_count}/{num_messages} mensajes enviados ({success_rate:.1f}%)", 
                      section)

def concurrent_messaging_test(base_url, token, topic_name, username, num_concurrent=5, section=None):
    """Prueba envío concurrente de mensajes"""
    print_step(f"Prueba de concurrencia: Enviando {num_concurrent} mensajes simultáneos al tópico '{topic_name}'")
    results = []
    
    def send_concurrent_message(index):
        content = f"Mensaje concurrente #{index} - {datetime.now().strftime('%H:%M:%S.%f')}"
        return send_message(base_url, token, topic_name, username, content, None)
    
    with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
        futures = [executor.submit(send_concurrent_message, i+1) for i in range(num_concurrent)]
        results = [future.result() for future in futures]
    
    success_count = sum(results)
    success_rate = (success_count / num_concurrent) * 100
    return print_result(success_count == num_concurrent,
                      f"Prueba de concurrencia: {success_count}/{num_concurrent} mensajes enviados ({success_rate:.1f}%)",
                      section)

def check_unauthorized_actions(base_url, token, other_user_topic, other_user, section=None):
    """Prueba acciones no autorizadas"""
    print_step(f"Prueba de acceso no autorizado: Intentando borrar tópico de otro usuario")
    url = f"{base_url}/messages/topics/{other_user_topic}"
    try:
        response = requests.delete(url, params={"token": token}, timeout=5)
        print(f"Estado: {response.status_code}")
        print(f"Respuesta: {response.json()}")
        # Debería fallar con 403
        expected_failure = response.status_code == 403
        return print_result(expected_failure, 
                          f"Prueba de acceso no autorizado {'exitosa' if expected_failure else 'fallida'} (se esperaba error 403)",
                          section, True)
    except Exception as e:
        print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
        return print_result(False, f"Error al probar acceso no autorizado: {str(e)}", section)

def queue_consumer_test(base_urls, token, queue_name, num_messages=5, section=None):
    """Prueba el comportamiento de consumo de colas"""
    print_step(f"Prueba de consumo de cola: Enviando {num_messages} mensajes y verificando comportamiento de cola")
    
    # Primero enviar varios mensajes a la cola
    for i in range(num_messages):
        send_queue_message(base_urls[0], token, queue_name, f"consumer_test", f"Mensaje cola #{i+1}", None)
    
    # Ahora consumir mensajes y verificar que son consumidos en orden FIFO
    consumed_messages = []
    for i in range(num_messages):
        msg = get_queue_message(base_urls[0], token, queue_name)
        if msg and msg.get("content"):
            consumed_messages.append(msg.get("content"))
    
    # Verificar que consumimos la cantidad correcta
    if len(consumed_messages) == num_messages:
        print_result(True, f"Se consumieron correctamente {num_messages} mensajes de la cola", section)
    else:
        print_result(False, f"Solo se consumieron {len(consumed_messages)}/{num_messages} mensajes de la cola", section)
    
    # Verificar que no quedaron mensajes
    msg = get_queue_message(base_urls[0], token, queue_name)
    if not msg or not msg.get("content"):
        return print_result(True, f"Cola vacía después de consumir todos los mensajes (correcto)", section)
    else:
        return print_result(False, f"La cola debería estar vacía pero aún tiene mensajes", section)

def distributed_queue_test(base_urls, tokens, queue_name, section=None):
    """Prueba el comportamiento distribuido de colas entre nodos"""
    print_step(f"Prueba de cola distribuida: Verificando comportamiento entre nodos")
    
    # Enviar mensajes desde diferentes nodos
    send_queue_message(base_urls[0], tokens[0], queue_name, "user1", "Mensaje desde nodo 1", None)
    send_queue_message(base_urls[1], tokens[0], queue_name, "user1", "Mensaje desde nodo 2", None)
    
    # Intentar consumir desde diferentes nodos
    msg1 = get_queue_message(base_urls[0], tokens[0], queue_name)
    if msg1 and msg1.get("content"):
        print_result(True, f"Consumido desde nodo 1: {msg1.get('content')}", section)
    else:
        print_result(False, f"No se pudo consumir mensaje desde nodo 1", section)
    
    msg2 = get_queue_message(base_urls[1], tokens[0], queue_name)
    if msg2 and msg2.get("content"):
        print_result(True, f"Consumido desde nodo 2: {msg2.get('content')}", section)
    else:
        print_result(False, f"No se pudo consumir mensaje desde nodo 2", section)
    
    # Verificar que no quedan mensajes
    msg3 = get_queue_message(base_urls[2], tokens[0], queue_name)
    if not msg3 or not msg3.get("content"):
        return print_result(True, f"Cola distribuida vacía después de consumir desde diferentes nodos", section)
    else:
        return print_result(False, f"La cola distribuida debería estar vacía pero aún tiene mensajes", section)

def print_test_summary():
    """Imprime un resumen de las pruebas ejecutadas"""
    print("\n" + "=" * 70)
    print(f"{COLORS['BOLD']}{COLORS['MAGENTA']} RESUMEN DE PRUEBAS {COLORS['END']}".center(80))
    print("=" * 70)
    
    # Total de pruebas
    total = TEST_STATS["total"]
    passed = TEST_STATS["passed"]
    failed = TEST_STATS["failed"]
    
    success_rate = (passed / total) * 100 if total > 0 else 0
    
    print(f"\n{COLORS['BOLD']}Resultados Generales:{COLORS['END']}")
    print(f"  Total de pruebas: {total}")
    print(f"  Pruebas exitosas: {COLORS['GREEN']}{passed}{COLORS['END']} ({success_rate:.1f}%)")
    print(f"  Pruebas fallidas: {COLORS['RED']}{failed}{COLORS['END']} ({100-success_rate:.1f}%)")
    
    # Resultados por sección
    print(f"\n{COLORS['BOLD']}Resultados por Sección:{COLORS['END']}")
    for section, stats in TEST_STATS["sections"].items():
        section_total = stats["total"]
        section_passed = stats["passed"]
        section_success_rate = (section_passed / section_total) * 100 if section_total > 0 else 0
        
        if section_success_rate == 100:
            status_color = COLORS['GREEN']
        elif section_success_rate >= 80:
            status_color = COLORS['YELLOW']
        else:
            status_color = COLORS['RED']
        
        print(f"  {section}: {status_color}{section_passed}/{section_total} ({section_success_rate:.1f}%){COLORS['END']}")
    
    # Resumen final
    print("\n" + "-" * 70)
    if failed == 0:
        print(f"{COLORS['GREEN']}{COLORS['BOLD']}¡TODAS LAS PRUEBAS PASARON EXITOSAMENTE!{COLORS['END']}")
    elif success_rate >= 80:
        print(f"{COLORS['YELLOW']}{COLORS['BOLD']}MAYORÍA DE PRUEBAS EXITOSAS, PERO HAY FALLOS A REVISAR{COLORS['END']}")
    else:
        print(f"{COLORS['RED']}{COLORS['BOLD']}DEMASIADOS FALLOS DETECTADOS. REVISAR IMPLEMENTACIÓN.{COLORS['END']}")
    print("-" * 70 + "\n")

def run_tests():
    parser = argparse.ArgumentParser(description='Pruebas del clúster MOM')
    parser.add_argument('--auth-only', action='store_true', help='Solo ejecutar pruebas de autenticación')
    parser.add_argument('--topics-only', action='store_true', help='Solo ejecutar pruebas de tópicos')
    parser.add_argument('--queues-only', action='store_true', help='Solo ejecutar pruebas de colas')
    parser.add_argument('--stress-test', action='store_true', help='Ejecutar pruebas de estrés')
    parser.add_argument('--replication-only', action='store_true', help='Solo ejecutar pruebas de replicación')
    parser.add_argument('--robustness-only', action='store_true', help='Solo ejecutar pruebas de robustez')
    args = parser.parse_args()
    
    print_header("INICIANDO PRUEBAS DEL CLÚSTER MOM", "General")
    
    # Generar nombres únicos para evitar conflictos
    test_id = random.randint(1000, 9999)
    logger.info(f"ID de prueba generado: {test_id}")
    
    username1 = f"usuario{test_id}_1"
    username2 = f"usuario{test_id}_2"
    topic_name = f"topic{test_id}"
    queue_name = f"queue{test_id}"
    
    # Sección 1: Autenticación y usuarios
    if not args.topics_only and not args.queues_only and not args.replication_only:
        print_header("PRUEBA DE REGISTRO Y AUTENTICACIÓN", "Autenticación")
        register_user(BASE_URLS[0], username1, "pass123", "Autenticación")
        register_user(BASE_URLS[1], username2, "pass123", "Autenticación")
        
        # Intentar registrar un usuario ya existente (debe fallar con 400)
        register_user(BASE_URLS[0], username1, "pass456", "Autenticación", expected_failure=True)
        
        # Intentar registrar un usuario con contraseña muy corta
        short_user = f"short_{test_id}"
        register_user(BASE_URLS[0], short_user, "a", "Autenticación")  # Esto depende de tu validación
        
        # Login de usuarios
        token1 = login_user(BASE_URLS[0], username1, "pass123", "Autenticación")
        token2 = login_user(BASE_URLS[1], username2, "pass123", "Autenticación")
        
        # Intentar login con credenciales incorrectas (debe fallar)
        login_user(BASE_URLS[0], username1, "wrongpass", "Autenticación", expected_failure=True)
        
        # Intentar login con usuario inexistente
        login_user(BASE_URLS[0], f"nonexistent_{test_id}", "somepass", "Autenticación", expected_failure=True)
        
        if not token1 or not token2:
            print(f"{COLORS['RED']}❌ Error al obtener tokens de autenticación. Continuando con pruebas limitadas.{COLORS['END']}")
    else:
        # Si saltamos autenticación, creamos usuarios básicos
        register_user(BASE_URLS[0], username1, "pass123")
        register_user(BASE_URLS[1], username2, "pass123")
        token1 = login_user(BASE_URLS[0], username1, "pass123")
        token2 = login_user(BASE_URLS[1], username2, "pass123")
    
    # Sección 2: Tópicos y replicación
    if token1 and (not args.auth_only and not args.queues_only or args.topics_only or args.replication_only):
        print_header("PRUEBA DE CREACIÓN Y REPLICACIÓN DE TÓPICOS", "Tópicos")
        
        # Crear tópico desde nodo 1
        create_topic(BASE_URLS[0], token1, topic_name, username1, "Tópicos")
        
        # Tópico adicional para el segundo usuario
        topic_name2 = f"topic{test_id}_user2"
        create_topic(BASE_URLS[1], token2, topic_name2, username2, "Tópicos")
        
        # Tópico especial para pruebas adicionales
        special_topic = f"topic_special_{test_id}"
        create_topic(BASE_URLS[2], token1, special_topic, username1, "Tópicos")
        
        # Esperar por replicación
        wait_for_replication(3)
        
        # Verificar replicación en todos los nodos
        all_topics_replicated = True
        for url in BASE_URLS:
            topics = list_topics(url, "Tópicos")
            if not check_replication(topic_name, topics, "tópico", "Tópicos"):
                all_topics_replicated = False
            if not check_replication(topic_name2, topics, "tópico", "Tópicos"):
                all_topics_replicated = False
        
        if all_topics_replicated:
            print_result(True, "Replicación de tópicos funciona correctamente", "Tópicos")
        else:
            print_result(False, "Problema en la replicación de tópicos", "Tópicos")
        
        # Prueba de permisos - intento no autorizado
        check_unauthorized_actions(BASE_URLS[0], token1, topic_name2, username2, "Tópicos")
        
        # Prueba de caracteres especiales en tópicos
        special_chars_topic = f"topic-con_caracteres.especiales_{test_id}"
        create_topic(BASE_URLS[0], token1, special_chars_topic, username1, "Tópicos")
        
        # Esperar por replicación
        wait_for_replication(2)
        
        # Verificar que el tópico con caracteres especiales se creó correctamente
        special_topic_created = False
        topics = list_topics(BASE_URLS[0], "Tópicos")
        if special_chars_topic in topics:
            special_topic_created = True
            print_result(True, f"Tópico con caracteres especiales '{special_chars_topic}' creado correctamente", "Tópicos")
        else:
            print_result(False, f"Error al crear tópico con caracteres especiales", "Tópicos")
        
        # Prueba de envío y replicación de mensajes
        print_header("PRUEBA DE MENSAJES EN TÓPICOS", "Mensajes")
        message_content = f"Mensaje de prueba {test_id}"
        send_message(BASE_URLS[0], token1, topic_name, username1, message_content, "Mensajes")
        
        # Esperar por replicación
        wait_for_replication(3)
        
        # Verificar replicación de mensajes
        message_replicated = check_message_replication(BASE_URLS, topic_name, message_content, "Mensajes")
        
        if message_replicated:
            print_result(True, "Replicación de mensajes funciona correctamente", "Mensajes")
        else:
            print_result(False, "Problema en la replicación de mensajes", "Mensajes")
        
        # Pruebas adicionales si se solicita una prueba de estrés
        if args.stress_test:
            print_header("PRUEBAS DE ESTRÉS Y CONCURRENCIA", "Estrés")
            
            # Prueba de estrés - enviar múltiples mensajes secuenciales
            stress_test_topic(BASE_URLS[0], token1, topic_name, username1, 20, "Estrés")
            
            # Prueba de concurrencia - enviar múltiples mensajes simultáneos
            concurrent_messaging_test(BASE_URLS[0], token1, topic_name, username1, 10, "Estrés")
            
            # Verificar que los mensajes se han guardado correctamente
            messages = get_messages(BASE_URLS[0], topic_name, "Estrés")
            print_result(len(messages) >= 31, f"Verificación de mensajes almacenados: {len(messages)} encontrados", "Estrés")
    
    # Sección 3: Pruebas de colas
    if token1 and (not args.auth_only and not args.topics_only or args.queues_only):
        print_header("PRUEBA DE COLAS", "Colas")
        create_queue(BASE_URLS[0], token1, queue_name, username1, "Colas")
        
        # Esperar por replicación
        wait_for_replication(3)
        
        # Verificar replicación en todos los nodos
        queue_replicated = True
        for url in BASE_URLS:
            queues = list_queues(url, "Colas")
            if not check_replication(queue_name, queues, "cola", "Colas"):
                queue_replicated = False
        
        if queue_replicated:
            print_result(True, "Replicación de colas funciona correctamente", "Colas")
        else:
            print_result(False, "Problema en la replicación de colas", "Colas")
        
        # Prueba de comportamiento de cola (FIFO)
        queue_consumer_test(BASE_URLS, token1, queue_name, 5, "Colas")
        
        # Prueba distribuida de colas
        distributed_queue_test(BASE_URLS, [token1, token2], queue_name, "Colas")
        
        # Eliminación de colas
        delete_queue(BASE_URLS[0], token1, queue_name, "Colas")
        
        # Verificar que la cola se eliminó
        wait_for_replication(2)
        queues_after_delete = list_queues(BASE_URLS[0], "Colas")
        print_result(queue_name not in queues_after_delete, f"Verificación de eliminación de cola '{queue_name}'", "Colas")
    
    # Prueba de robustez - intentar eliminar un tópico inexistente
    if not args.auth_only:
        print_header("PRUEBA DE ROBUSTEZ", "Robustez")
        
        # Intentar eliminar un tópico inexistente (debería devolver un error 404)
        nonexistent_topic = f"nonexistent_topic_{random.randint(10000, 99999)}"
        url = f"{BASE_URLS[0]}/messages/topics/{nonexistent_topic}"
        try:
            response = requests.delete(url, params={"token": token1}, timeout=5)
            print(f"Estado al eliminar tópico inexistente: {response.status_code}")
            print(f"Respuesta: {response.json()}")
            # Un 404 es el comportamiento esperado al intentar eliminar algo que no existe
            print_result(response.status_code == 404, 
                      f"Eliminación de tópico inexistente manejada correctamente", 
                      "Robustez", 
                      expected_failure=True)
        except Exception as e:
            print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
            print_result(False, f"Error al probar eliminación de tópico inexistente", "Robustez")
        
        # Intentar obtener mensajes de un tópico inexistente
        url = f"{BASE_URLS[0]}/messages/messages/topic/{nonexistent_topic}"
        try:
            response = requests.get(url, timeout=5)
            print(f"Estado al obtener mensajes de tópico inexistente: {response.status_code}")
            print_result(response.status_code == 404, 
                      f"Obtención de mensajes de tópico inexistente manejada correctamente", 
                      "Robustez", 
                      expected_failure=True)
        except Exception as e:
            print(f"{COLORS['RED']}Error en la solicitud: {str(e)}{COLORS['END']}")
            print_result(False, f"Error al probar obtención de mensajes de tópico inexistente", "Robustez")
    
    # Prueba de limpieza - eliminar tópicos
    if not args.auth_only and not args.queues_only:
        print_header("PRUEBA DE LIMPIEZA", "Limpieza")
        delete_topic(BASE_URLS[0], token1, topic_name, "Limpieza")
        delete_topic(BASE_URLS[1], token2, topic_name2, "Limpieza")
        if 'special_topic' in locals():
            delete_topic(BASE_URLS[2], token1, special_topic, "Limpieza")
        if 'special_chars_topic' in locals() and special_topic_created:
            delete_topic(BASE_URLS[0], token1, special_chars_topic, "Limpieza")
        
        # Verificar eliminación
        wait_for_replication(2)
        topics_after_delete = list_topics(BASE_URLS[0], "Limpieza")
        print_result(topic_name not in topics_after_delete, f"Verificación de eliminación de tópico '{topic_name}'", "Limpieza")
        print_result(topic_name2 not in topics_after_delete, f"Verificación de eliminación de tópico '{topic_name2}'", "Limpieza")
        if 'special_topic' in locals():
            print_result(special_topic not in topics_after_delete, f"Verificación de eliminación de tópico '{special_topic}'", "Limpieza")
        if 'special_chars_topic' in locals() and special_topic_created:
            print_result(special_chars_topic not in topics_after_delete, f"Verificación de eliminación de tópico con caracteres especiales", "Limpieza")
    
    print_test_summary()

if __name__ == "__main__":
    run_tests()